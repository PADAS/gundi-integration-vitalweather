import httpx
import logging

import app.actions.client as client

from app.actions.configurations import AuthenticateConfig, PullObservationsConfig, PullStationConditionsConfig, get_auth_config
from app.services.action_scheduler import trigger_action
from app.services.activity_logger import activity_logger
from app.services.gundi import send_observations_to_gundi
from app.services.state import IntegrationStateManager
from app.services.utils import generate_batches

logger = logging.getLogger(__name__)
state_manager = IntegrationStateManager()


VW_BASE_URL = "https://www.vitalweather.co.za/api/v1"


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

    try:
        response = await client.get_stations(integration, base_url, action_config)
        if not response:
            logger.error(f"Failed to authenticate with integration {integration.id} using {action_config}")
            return {"valid_credentials": False, "message": "Bad credentials"}
        return {"valid_credentials": True}
    except (client.VWUnauthorizedException, client.VWNotFoundException, client.VWException) as e:
        return {"valid_credentials": False, "status_code": e.status_code, "message": e.message}
    except httpx.HTTPStatusError as e:
        return {"error": True, "status_code": e.response.status_code}


@activity_logger()
async def action_pull_observations(integration, action_config: PullObservationsConfig):
    logger.info(f"Executing 'pull_observations' action with integration ID {integration.id} and action_config {action_config}...")

    base_url = integration.base_url or VW_BASE_URL
    auth_config = get_auth_config(integration)

    try:
        response = await client.get_stations(integration, base_url, auth_config)
        if response:
            logger.info(f"Found {len(response.stations)} stations for integration {integration.id}")
            stations_triggered = 0
            for station in response.stations:
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

    try:
        conditions_response = await client.get_station_conditions(integration, base_url, action_config, auth_config)
        if conditions_response:
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
