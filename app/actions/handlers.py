import httpx
import logging
import datetime
import pydantic

import app.actions.client as client

from app.actions.configurations import (
    AuthenticateConfig,
    PullObservationsConfig,
    PullStationConditionsConfig,
    FetchDailySummaryConfig,
    get_auth_config
)
from gundi_core.schemas.v2 import LogLevel
from app.services.action_scheduler import trigger_action, crontab_schedule
from app.services.activity_logger import activity_logger, log_action_activity
from app.services.gundi import send_observations_to_gundi, send_events_to_gundi
from app.services.state import IntegrationStateManager
from app.services.utils import generate_batches

logger = logging.getLogger(__name__)
state_manager = IntegrationStateManager()


VW_BASE_URL = "https://www.vitalweather.co.za/api/v1"


def transform_daily_summary(summary, units, station):
    def match_units(conditions, units):
        for key, value in conditions.items():
            if key in units and key != "ts":
                conditions[key] = f"{value} {units[key]}"
        return conditions

    readings = match_units(summary.dict(), units.dict())

    yield dict(
        title=f"Station {summary.station_id} Summary ({summary.Date})",
        event_type="weather_station_summary",
        recorded_at=datetime.datetime.strptime(summary.Date, "%Y-%m-%d").replace(tzinfo=datetime.timezone.utc),
        location={
            "lat": station.latitude,
            "lon": station.longitude
        },
        event_details={**readings}
    )


def transform(station, observations):
    def match_units(conditions, units):
        for record in conditions:
            for key, value in record.items():
                if key in units and key != "ts":
                    record[key] = f"{value} {units[key]}"
        return conditions

    readings = match_units([h.dict() for h in observations.conditions], observations.units.dict())

    for reading in readings:
        yield {
            "source_name": station.Station_Name,
            "source": station.Station_ID,
            "type": "stationary-object",
            "subtype": "weather_station",
            "recorded_at": reading.pop("ts"),
            "location": {
                "lat": station.latitude,
                "lon": station.longitude
            },
            "additional": {
                "station_height": station.height,
                **reading
            }
        }


async def action_auth(integration, action_config: AuthenticateConfig):
    logger.info(f"Executing 'auth' action with integration ID {integration.id} and action_config {action_config}...")

    base_url = integration.base_url or VW_BASE_URL
    key = action_config.key.get_secret_value()

    try:
        response = await client.get_stations(integration, base_url, key)
        if not response:
            logger.error(f"Failed to authenticate with integration {integration.id} using {action_config}")
            return {"valid_credentials": False, "message": "Bad credentials"}
        try:
            client.StationsResponse.parse_obj(response)
            return {"valid_credentials": True}
        except pydantic.ValidationError:
            logger.error(f"Failed to authenticate with integration {integration.id} using {action_config}")
            return {"valid_credentials": False, "message": "Bad credentials"}
    except (client.VWUnauthorizedException, client.VWNotFoundException, client.VWException) as e:
        return {"valid_credentials": False, "status_code": e.status_code, "message": e.message}
    except httpx.HTTPStatusError as e:
        return {"error": True, "status_code": e.response.status_code}


@activity_logger()
async def action_pull_observations(integration, action_config: PullObservationsConfig):
    logger.info(f"Executing 'pull_observations' action with integration ID {integration.id} and action_config {action_config}...")

    base_url = integration.base_url or VW_BASE_URL
    auth_config = get_auth_config(integration)
    key = auth_config.key.get_secret_value()

    try:
        response = await client.get_stations(integration, base_url, key)
        if response:
            try:
                stations_response = client.StationsResponse.parse_obj(response)
            except pydantic.ValidationError as e:
                msg = f"Response: {response['stations']}. Exception: {e}"
                logger.error(msg)
                await log_action_activity(
                    integration_id=integration.id,
                    action_id="pull_observations",
                    level=LogLevel.ERROR,
                    title=f"Get stations error",
                    data={"message": msg}
                )
                raise

            logger.info(f"Found {len(stations_response.stations)} stations for integration {integration.id}")
            stations_triggered = 0
            for station in stations_response.stations:
                logger.info(f"Triggering 'action_pull_station_conditions' action for station {station.Station_ID} to extract observations...")

                parsed_config = PullStationConditionsConfig(
                    station=station
                )
                await trigger_action(integration.id, "pull_station_conditions", config=parsed_config)
                stations_triggered += 1
            return {"stations_triggered": stations_triggered}
        else:
            logger.warning(f"No stations found for integration {integration.id}")
            return {"stations_triggered": 0}
    except (client.VWUnauthorizedException, client.VWNotFoundException, client.VWException) as e:
        message = f"Failed to authenticate with integration {integration.id} using {auth_config}. Exception: {e}"
        logger.exception(message)
        raise e
    except httpx.HTTPStatusError as e:
        message = f"'pull_observations' action error with integration {integration.id} using {auth_config}. Exception: {e}"
        logger.exception(message)
        raise e


@activity_logger()
async def action_pull_station_conditions(integration, action_config: PullStationConditionsConfig):
    logger.info(f"Executing action 'pull_station_conditions' for integration ID {integration.id} and action_config {action_config}...")

    base_url = integration.base_url or VW_BASE_URL
    auth_config = get_auth_config(integration)
    observations_extracted = 0

    key = auth_config.key.get_secret_value()

    try:
        conditions_response = await client.get_station_conditions(integration, base_url, action_config.station.Station_ID, key)
        if conditions_response:
            # filter conditions with fault_status, log a warning if any and remove them from conditions_response
            for condition in conditions_response["conditions"]:
                if condition.get("fault_status", 0) != 0:
                    faults = f'Fault status: {condition.get("fault_status")}', f'Message: {condition.get("message")}'
                    msg = f"Station {action_config.station.Station_ID} has a fault status. Response: {condition}"
                    logger.warning(msg)
                    await log_action_activity(
                        integration_id=integration.id,
                        action_id="pull_station_conditions",
                        level=LogLevel.WARNING,
                        title=f"Station {action_config.station.Station_ID} reported an error.",
                        data={"faults": faults}
                    )
                    conditions_response["conditions"].remove(condition)
            try:
                conditions_response = client.ConditionsResponse.parse_obj(conditions_response)
            except pydantic.ValidationError as e:
                msg = f"Response: {conditions_response['conditions']}. Exception: {e}"
                logger.error(msg)
                await log_action_activity(
                    integration_id=integration.id,
                    action_id="pull_station_conditions",
                    level=LogLevel.WARNING,
                    title=f"Get station conditions error for station '{action_config.station.Station_ID}'",
                    data={"message": msg}
                )
                return None

            logger.info(f"Extracted {len(conditions_response.conditions)} observations for station {action_config.station.Station_ID}.")
            transformed_data = transform(action_config.station, conditions_response)

            for i, batch in enumerate(generate_batches(list(transformed_data), 200)):
                logger.info(f'Sending observations batch #{i}: {len(batch)} observations. station: {action_config.station.Station_ID}')
                response = await send_observations_to_gundi(observations=batch, integration_id=integration.id)
                observations_extracted += len(response)

            return {"observations_extracted": observations_extracted}
        else:
            logger.warning(f"No observations found for station {action_config.station.Station_ID}")
            return {"observations_extracted": 0}
    except client.VWUnauthorizedException as e:
        message = f"Failed to authenticate with integration {integration.id} using {action_config}. Exception: {e}"
        logger.exception(message)
        raise e
    except client.VWNotFoundException as e:
        message = f"Not found response with integration {integration.id} using {action_config}. Exception: {e}"
        logger.exception(message)
        raise e


@activity_logger()
@crontab_schedule("0 1 * * *")
async def action_fetch_daily_summary(integration, action_config: FetchDailySummaryConfig):
    logger.info(f"Executing 'fetch_daily_summary' action with integration ID {integration.id} and action_config {action_config}...")

    base_url = integration.base_url or VW_BASE_URL
    auth_config = get_auth_config(integration)
    key = auth_config.key.get_secret_value()

    summaries_fetched = 0

    try:
        stations = await client.get_stations(integration, base_url, key)
        if stations:
            logger.info(f"Found {len(stations.stations)} stations for integration {integration.id}")
            for station in stations.stations:
                daily_summary = await client.get_daily_summary(integration, base_url, station.Station_ID, key)
                if daily_summary:
                    try:
                        daily_summary = client.DailySummaryResponse.parse_obj(daily_summary)
                    except pydantic.ValidationError as e:
                        msg = f"Response: {daily_summary['dailysummary']}. Exception: {e}"
                        logger.error(msg)
                        await log_action_activity(
                            integration_id=integration.id,
                            action_id="fetch_daily_summary",
                            level=LogLevel.WARNING,
                            title=f"Get station daily summary error for station '{station.Station_ID}'",
                            data={"message": msg}
                        )
                        raise

                    for station_summary in daily_summary.dailysummary:
                        logger.info(f"Sending daily summary for station {station_summary.station_id} Date: {station_summary.Date}.")
                        transformed_data = transform_daily_summary(station_summary, daily_summary.units, station)

                        for i, batch in enumerate(generate_batches(list(transformed_data), 200)):
                            logger.info(f'Sending events batch #{i}: {len(batch)} events. station: {station_summary.station_id}')
                            response = await send_events_to_gundi(events=batch, integration_id=integration.id)
                            summaries_fetched += len(response)

        return {"summaries_fetched": summaries_fetched}
    except (client.VWUnauthorizedException, client.VWNotFoundException, client.VWException) as e:
        message = f"Failed to authenticate with integration {integration.id} using {auth_config}. Exception: {e}"
        logger.exception(message)
        raise e
    except httpx.HTTPStatusError as e:
        message = f"'fetch_daily_summary' action error with integration {integration.id} using {auth_config}. Exception: {e}"
        logger.exception(message)
        raise e
