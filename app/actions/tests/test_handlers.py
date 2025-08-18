import pytest

from app import settings
from unittest.mock import AsyncMock, MagicMock
from app.actions.handlers import (
    LogLevel,
    action_auth,
    action_pull_observations,
    action_pull_station_conditions,
    action_fetch_daily_summary
)
from app.actions.configurations import (
    AuthenticateConfig,
    PullObservationsConfig,
    PullStationConditionsConfig,
    FetchDailySummaryConfig
)
from app.actions.client import VWException, Station, StationsResponse, DailySummaryResponse


@pytest.mark.asyncio
async def test_action_auth_success(mocker):
    mock_integration = mocker.Mock()
    mock_action_config = AuthenticateConfig(key="testkey")

    mock_response = {
        "stations": [
            {
                "Station_ID": 123,
                "Station_Name": "Test Station",
                "latitude": -33.9,
                "longitude": 18.4,
                "height": 10
            }
        ],
        "generated_at": 1755016414,
        "code": 200,
        "message": "ok"
    }

    mocker.patch('app.actions.client.get_stations', new=AsyncMock(return_value=mock_response))

    result = await action_auth(mock_integration, mock_action_config)
    assert result == {"valid_credentials": True}

@pytest.mark.asyncio
async def test_action_auth_error(mocker):
    mock_integration = mocker.Mock()
    mock_action_config = AuthenticateConfig(key="testkey")

    mocker.patch('app.actions.client.get_stations', new=AsyncMock(side_effect=VWException(
        error=Exception("Incorrect KEY"),
        message="Incorrect KEY",
        status_code=400
    )))

    result = await action_auth(mock_integration, mock_action_config)
    assert result == {"valid_credentials": False, "status_code": 400, "message": "Incorrect KEY"}

@pytest.mark.asyncio
async def test_action_pull_observations_triggers_pull_station_conditions(mocker, integration_v2, mock_publish_event):
    settings.TRIGGER_ACTIONS_ALWAYS_SYNC = False
    settings.INTEGRATION_COMMANDS_TOPIC = "vitalweather-actions-topic"

    mocker.patch("app.services.state.IntegrationStateManager.get_state", return_value=None)
    mocker.patch("app.services.activity_logger.publish_event", mock_publish_event)
    mocker.patch("app.services.action_runner.publish_event", mock_publish_event)

    mock_trigger_action = mocker.patch("app.actions.handlers.trigger_action", return_value=None)

    mocker.patch("app.services.action_scheduler.publish_event", mock_publish_event)
    mocker.patch("app.services.action_runner.execute_action", return_value=None)

    mocker.patch('app.actions.client.get_stations', new=AsyncMock(return_value=StationsResponse.parse_obj(
        {
            "stations": [
                {
                    "Station_ID":123,
                    "Station_Name":"Test Station",
                    "latitude":-15.92883055,
                    "longitude":34.606880555,
                    "height":166.6
                }
            ],
            "generated_at": 1739811533,
            "code": 200,
            "message": "success"
        }
    )))

    integration = integration_v2

    # Modify auth config
    integration.configurations[2].data = {"key": "testkey"}

    action_config = PullObservationsConfig(default_lookback_days=15)

    result = await action_pull_observations(integration, action_config)
    assert result == {"stations_triggered": 1}

    mock_trigger_action.assert_called_once()

@pytest.mark.asyncio
async def test_action_pull_observations_error(mocker, integration_v2, mock_publish_event):
    mocker.patch('app.actions.client.get_stations', new=AsyncMock(side_effect=VWException(
        error=Exception("Incorrect KEY"),
        message="Incorrect KEY",
        status_code=400
    )))
    mocker.patch("app.services.state.IntegrationStateManager.get_state", return_value=None)
    mocker.patch("app.services.activity_logger.publish_event", mock_publish_event)
    mocker.patch("app.services.action_runner.publish_event", mock_publish_event)
    mocker.patch("app.services.action_scheduler.publish_event", mock_publish_event)

    integration = integration_v2

    # Modify auth config
    integration.configurations[2].data = {"key": "testkey"}

    action_config = PullObservationsConfig(default_lookback_days=15)

    with pytest.raises(VWException):
        await action_pull_observations(integration, action_config)

@pytest.mark.asyncio
async def test_action_pull_station_conditions_success(mocker, integration_v2, mock_publish_event):
    action_config = PullStationConditionsConfig(
        station=Station(
            Station_ID=123,
            Station_Name="Test Station",
            latitude=-15.92883055,
            longitude=34.606880555,
            height=166.6
        )
    )

    integration = integration_v2

    # Modify auth config
    integration.configurations[2].data = {"key": "testkey"}

    mock_conditions_response = {
        "code": 200,
        "conditions": [
            {
                "station_id": 123,
                "station_Name": "Test Station",
                "pressure": 995.3,
                "temperature": 25.6,
                "humidity": 88,
                "wind_average": 0.0,
                "max_wind": 3.2,
                "wind_direction": 180,
                "total_rain": 0.0,
                "FDI": 0,
                "solar_radiation": 0.0,
                "fault_status": 0,
                "message": "ok",
                "ts": 1234567890
            }
        ],
        "unites": {
            "local_time_last_update": "UTC",
            "ts": "s",
            "temperature": "°C",
            "humidity": "%",
            "pressure": "mb",
            "wind_average": "kph",
            "wind_direction": "deg",
            "total_rain": "mm",
            "solar_radiation": "W/M2",
            "FDI": "index"
        },
        "generated_at": "2025-05-12T00:00:00Z",
        "message": "ok"
    }

    mocker.patch("app.services.state.IntegrationStateManager.get_state", return_value=None)
    mocker.patch("app.services.activity_logger.publish_event", mock_publish_event)
    mocker.patch("app.services.action_runner.publish_event", mock_publish_event)
    mocker.patch("app.services.action_scheduler.publish_event", mock_publish_event)
    mocker.patch('app.actions.client.get_station_conditions', new=AsyncMock(return_value=mock_conditions_response))
    mocker.patch('app.services.state.IntegrationStateManager.set_state', new=AsyncMock())
    mocker.patch('app.services.utils.generate_batches', return_value=[[{}]])
    mocker.patch('app.actions.handlers.send_observations_to_gundi', new=AsyncMock(return_value=[{}]))
    mocker.patch('app.actions.handlers.transform', return_value=[{
        'additional': {
            'humidity': '88 %',
            'pressure': '995.3 mb',
            'solar_radiation': '0.0  W/M2',
            'station_height': 366.7,
            'temperature': '25.6 °C',
            'total_rain': '0.0 mm',
            'uv': 25.5,
            'wind_average': '0.0 Kp/h',
            'wind_direction': '180 360 points',
            'wind_max': 3.2,
            'wind_min': 0
        },
        'location': {'lat': -15.72883055, 'lon': 34.506880555},
        'recorded_at': '2025-03-06 14:35:02+00:00',
        'source': 123,
        'source_name': 'Test S',
        'subtype': 'weather_station',
        'type': 'stationary-object'
    }])

    result = await action_pull_station_conditions(integration, action_config)
    assert result == {"observations_extracted": 1}


@pytest.mark.asyncio
async def test_action_pull_station_conditions_fault_status_sends_warning_activity_log(mocker, integration_v2, mock_publish_event):
    action_config = PullStationConditionsConfig(
        station=Station(
            Station_ID=123,
            Station_Name="Test Station",
            latitude=-15.92883055,
            longitude=34.606880555,
            height=166.6
        )
    )

    integration = integration_v2

    # Modify auth config
    integration.configurations[2].data = {"key": "testkey"}

    mocker.patch("app.services.state.IntegrationStateManager.get_state", return_value=None)
    mocker.patch("app.services.activity_logger.publish_event", mock_publish_event)
    mocker.patch("app.services.action_runner.publish_event", mock_publish_event)
    mocker.patch("app.services.action_scheduler.publish_event", mock_publish_event)
    mocker.patch('app.services.state.IntegrationStateManager.set_state', new=AsyncMock())

    bad_response = {
        "code": 200,
        "conditions": [{'station_id': 123, 'fault_status': 11, 'message': 'no data'}],  # Real bad response from vitalweather
        "unites": {
            "local_time_last_update": "UTC",
            "ts": "s",
            "temperature": "°C",
            "humidity": "%",
            "pressure": "mb",
            "wind_average": "kph",
            "wind_direction": "deg",
            "total_rain": "mm",
            "solar_radiation": "W/M2",
            "FDI": "index"
        },
        "generated_at": "2025-05-12T00:00:00Z",
        "message": "ok"
    }

    mock_httpx_response = MagicMock()
    mock_httpx_response.is_error = False
    mock_httpx_response.json.return_value = bad_response
    mock_httpx_response.text = str(bad_response)
    mock_httpx_response.raise_for_status = MagicMock()

    mocker.patch("httpx.AsyncClient.get", return_value=mock_httpx_response)

    mock_log = mocker.patch("app.actions.handlers.log_action_activity", new_callable=AsyncMock)

    result = await action_pull_station_conditions(integration, action_config)

    assert result == {"observations_extracted": 0}

    # Check that the log was created
    assert mock_log.await_count == 1
    assert mock_log.call_args[1]["integration_id"] == integration.id
    assert mock_log.call_args[1]["action_id"] == "pull_station_conditions"
    assert mock_log.call_args[1]["level"] == LogLevel.WARNING
    assert mock_log.call_args[1]["title"] == f"Station 123 reported an error."


@pytest.mark.asyncio
async def test_action_pull_station_conditions_error(mocker, integration_v2, mock_publish_event):
    action_config = PullStationConditionsConfig(
        station=Station(
            Station_ID=123,
            Station_Name="Test Station",
            latitude=-15.92883055,
            longitude=34.606880555,
            height=166.6
        )
    )
    mocker.patch("app.services.state.IntegrationStateManager.get_state", return_value=None)
    mocker.patch("app.services.activity_logger.publish_event", mock_publish_event)
    mocker.patch("app.services.action_runner.publish_event", mock_publish_event)
    mocker.patch("app.services.action_scheduler.publish_event", mock_publish_event)

    integration = integration_v2

    # Modify auth config
    integration.configurations[2].data = {"key": "testkey"}

    mocker.patch('app.actions.client.get_station_conditions', new=AsyncMock(side_effect=VWException(
        error=Exception("Incorrect KEY"),
        message="Incorrect KEY",
        status_code=400
    )))

    with pytest.raises(VWException):
        await action_pull_station_conditions(integration, action_config)


@pytest.mark.asyncio
async def test_action_fetch_daily_summary_success(mocker, integration_v2, mock_publish_event):
    mocker.patch("app.services.activity_logger.publish_event", mock_publish_event)
    mocker.patch("app.services.action_runner.publish_event", mock_publish_event)
    mocker.patch("app.services.action_scheduler.publish_event", mock_publish_event)
    mocker.patch('app.services.utils.generate_batches', return_value=[[{}]])
    mocker.patch('app.actions.handlers.send_events_to_gundi', new=AsyncMock(return_value=[{}]))

    mocker.patch('app.actions.client.get_stations', new=AsyncMock(return_value=StationsResponse.parse_obj(
        {
            "stations": [
                {
                    "Station_ID": 123,
                    "Station_Name": "Test Station",
                    "latitude": -15.92883055,
                    "longitude": 34.606880555,
                    "height": 166.6
                }
            ],
            "generated_at": 1739811533,
            "code": 200,
            "message": "success"
        }
    )))

    mocker.patch("app.actions.client.get_daily_summary", new=AsyncMock(return_value=DailySummaryResponse.parse_obj(
        {
            "dailysummary": [
                {
                    "station_id": 123,
                    "Date": "2025-03-03",
                    "rain": 58.8,
                    "avg_temp": 21.6,
                    "min_temp": 20.6,
                    "max_temp": 22.8,
                    "avg_RH": 94,
                    "min_RH": 92,
                    "max_RH": 95,
                    "avg_wind": 0.1,
                    "avg_solar": 0,
                    "avg_pressure": 1003.71,
                    "min_pressure": 1002.51,
                    "max_pressure": 1005.28,
                    "avg_winddirection": [
                        225,
                        "SW"
                    ]
                }
            ],
            "unites": {
                "rain": "mm",
                "avg_temp": "°C",
                "max_temp": "°C",
                "min_temp": "°C",
                "avg_RH": "%",
                "max_hum": "%",
                "min_hum": "%",
                "avg_wind": "kph",
                "avg_solar": "Watts/M",
                "avg_pressure": "mb",
                "min_pressure": "mb",
                "max_pressure": "mb",
                "avg_winddirection": [
                    "°",
                    "DIR"
                ]
            },
            "generated_at": 1747242490,
            "code": 200,
            "message": "success"
        }
    )))
    mocker.patch("app.services.utils.generate_batches", return_value=[[{"event": "test_event"}]])
    mocker.patch("app.services.gundi.send_events_to_gundi", return_value=AsyncMock(return_value=[{"status": "success"}]))

    integration = integration_v2

    # Modify auth config
    integration.configurations[2].data = {"key": "testkey"}

    action_config = FetchDailySummaryConfig()

    result = await action_fetch_daily_summary(integration, action_config)
    assert result == {"summaries_fetched": 1}


@pytest.mark.asyncio
async def test_action_fetch_daily_summary_no_stations(mocker, integration_v2, mock_publish_event):
    mocker.patch("app.services.activity_logger.publish_event", mock_publish_event)
    mocker.patch("app.services.action_runner.publish_event", mock_publish_event)
    mocker.patch("app.services.action_scheduler.publish_event", mock_publish_event)

    mocker.patch('app.actions.client.get_stations', new=AsyncMock(return_value=None))

    integration = integration_v2

    # Modify auth config
    integration.configurations[2].data = {"key": "testkey"}

    action_config = FetchDailySummaryConfig()

    result = await action_fetch_daily_summary(integration, action_config)

    assert result == {"summaries_fetched": 0}


@pytest.mark.asyncio
async def test_action_fetch_daily_summary_error(mocker, integration_v2, mock_publish_event):
    mocker.patch("app.services.activity_logger.publish_event", mock_publish_event)
    mocker.patch("app.services.action_runner.publish_event", mock_publish_event)
    mocker.patch("app.services.action_scheduler.publish_event", mock_publish_event)

    mocker.patch('app.actions.client.get_stations', new=AsyncMock(side_effect=VWException(
        error=Exception("Incorrect KEY"),
        message="Incorrect KEY",
        status_code=400
    )))

    integration = integration_v2

    # Modify auth config
    integration.configurations[2].data = {"key": "testkey"}

    action_config = FetchDailySummaryConfig()

    with pytest.raises(Exception, match="'400: Incorrect KEY, Error: Incorrect KEY'"):
        await action_fetch_daily_summary(integration, action_config)
