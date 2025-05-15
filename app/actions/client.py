import logging
import httpx
import pydantic
import stamina

from datetime import datetime, timezone
from typing import List, Union
from app.services.state import IntegrationStateManager


logger = logging.getLogger(__name__)
state_manager = IntegrationStateManager()


class Station(pydantic.BaseModel):
    Station_ID: int
    Station_Name: str
    latitude: float
    longitude: float
    height: float


class StationsResponse(pydantic.BaseModel):
    stations: List[Station]
    generated_at: datetime
    code: int
    message: str

    @pydantic.validator('generated_at', always=True)
    def parse_time_string(cls, v):
        if not v.tzinfo:
            return v.replace(tzinfo=timezone.utc)
        return v


class ConditionsItem(pydantic.BaseModel):
    station_id: int
    station_Name: str
    ts: datetime
    pressure: float
    temperature: float
    humidity: int
    wind_average: float
    max_wind: float
    wind_direction: int
    total_rain: float
    FDI: int
    solar_radiation: float

    @pydantic.validator('ts', always=True)
    def parse_time_string(cls, v):
        if not v.tzinfo:
            return v.replace(tzinfo=timezone.utc)
        return v


class Units(pydantic.BaseModel):
    local_time_last_update: str
    ts: str
    temperature: str
    humidity: str
    pressure: str
    wind_average: str
    wind_direction: str
    total_rain: str
    solar_radiation: str
    FDI: str


class DailySummaryUnits(pydantic.BaseModel):
    rain: str
    avg_temp: str
    max_temp: str
    min_temp: str
    avg_RH: str
    max_hum: str
    min_hum: str
    avg_wind: str
    avg_solar: str
    avg_pressure: str
    min_pressure: str
    max_pressure: str
    avg_winddirection: List[str]


class ConditionsResponse(pydantic.BaseModel):
    conditions: List[ConditionsItem]
    units: Units = pydantic.Field(alias='unites')
    generated_at: datetime
    code: int
    message: str

    @pydantic.validator('generated_at', always=True)
    def parse_time_string(cls, v):
        if not v.tzinfo:
            return v.replace(tzinfo=timezone.utc)
        return v

    class Config:
        allow_population_by_field_name = True


class DailySummaryItem(pydantic.BaseModel):
    station_id: int
    Date: str
    rain: float
    avg_temp: float
    min_temp: float
    max_temp: float
    avg_RH: int
    min_RH: int
    max_RH: int
    avg_wind: float
    avg_solar: int
    avg_pressure: float
    min_pressure: float
    max_pressure: float
    avg_winddirection: List[Union[int, str]]


class DailySummaryResponse(pydantic.BaseModel):
    dailysummary: List[DailySummaryItem]
    units: DailySummaryUnits = pydantic.Field(alias='unites')
    generated_at: datetime
    code: int
    message: str

    @pydantic.validator('generated_at', always=True)
    def parse_time_string(cls, v):
        if not v.tzinfo:
            return v.replace(tzinfo=timezone.utc)
        return v

    class Config:
        allow_population_by_field_name = True


class VWException(Exception):
    def __init__(self, error: Exception, message: str, status_code: int):
        self.status_code = status_code
        self.message = message
        self.error = error
        super().__init__(f"'{self.status_code}: {self.message}, Error: {self.error}'")


class VWNotFoundException(Exception):
    def __init__(self, error: Exception, message: str, status_code=404):
        self.status_code = status_code
        self.message = message
        self.error = error
        super().__init__(f"'{self.status_code}: {self.message}, Error: {self.error}'")


class VWUnauthorizedException(Exception):
    def __init__(self, error: Exception, message: str, status_code=401):
        self.status_code = status_code
        self.message = message
        self.error = error
        super().__init__(f"'{self.status_code}: {self.message}, Error: {self.error}'")


@stamina.retry(on=httpx.HTTPError, wait_initial=4.0, wait_jitter=5.0, wait_max=32.0)
async def get_stations(integration, base_url, auth):
    async with httpx.AsyncClient(timeout=120) as session:
        logger.info(f"-- Getting stations for integration ID: {integration.id} --")

        url = f"{base_url}/stations.php"

        try:
            response = await session.get(url, params={"key": auth.key.get_secret_value()})
            if response.is_error:
                logger.error(f"Error 'get_stations'. Response body: {response.text}")
            response.raise_for_status()
            parsed_response = response.json()
            if parsed_response:
                if parsed_response["code"] != 200:
                    raise VWException(
                        error=Exception(parsed_response["message"]),
                        message=parsed_response["message"],
                        status_code=parsed_response["code"]
                    )
                return StationsResponse.parse_obj(parsed_response)
            else:
                return response.text
        except httpx.HTTPStatusError as e:
            if e.response.status_code == 401:
                raise VWUnauthorizedException(e, "Unauthorized access")
            elif e.response.status_code == 404:
                raise VWNotFoundException(e, "User not found")
            raise e


@stamina.retry(on=httpx.HTTPError, wait_initial=4.0, wait_jitter=5.0, wait_max=32.0)
async def get_station_conditions(integration, base_url, config, auth):
    async with httpx.AsyncClient(timeout=120) as session:
        url = f"{base_url}/conditions.php/{config.station.Station_ID}"
        params = {
            "key": auth.key.get_secret_value(),
        }

        logger.info(f"-- Getting latest conditions for integration ID: {integration.id} Station ID: {config.station.Station_ID} --")

        try:
            response = await session.get(url, params=params)
            if response.is_error:
                logger.error(f"Error 'get_station_conditions'. Response body: {response.text}")
            response.raise_for_status()
            parsed_response = response.json()
            if parsed_response:
                if parsed_response["code"] != 200:
                    raise VWException(
                        error=Exception(parsed_response["message"]),
                        message=parsed_response["message"],
                        status_code=parsed_response["code"]
                    )
                obs = ConditionsResponse.parse_obj(parsed_response)
                return obs
            else:
                return response.text
        except httpx.HTTPStatusError as e:
            if e.response.status_code == 401:
                raise VWUnauthorizedException(e, "Unauthorized access")
            elif e.response.status_code == 404:
                raise VWNotFoundException(e, "User not found")
            raise e


@stamina.retry(on=httpx.HTTPError, wait_initial=4.0, wait_jitter=5.0, wait_max=32.0)
async def get_daily_summary(integration, base_url, station, config):
    async with httpx.AsyncClient(timeout=120) as session:
        url = f"{base_url}/dailysummary.php/{station.Station_ID}"
        params = {
            "key": config.key.get_secret_value(),
        }

        logger.info(f"-- Getting daily summary for integration ID: {integration.id} Station: {station.Station_ID} --")

        try:
            response = await session.get(url, params=params)
            if response.is_error:
                logger.error(f"Error 'get_daily_summary'. Response body: {response.text}")
            response.raise_for_status()
            parsed_response = response.json()
            if parsed_response:
                if parsed_response["code"] != 200:
                    raise VWException(
                        error=Exception(parsed_response["message"]),
                        message=parsed_response["message"],
                        status_code=parsed_response["code"]
                    )
                summary = DailySummaryResponse.parse_obj(parsed_response)
                return summary
            else:
                return response.text
        except httpx.HTTPStatusError as e:
            if e.response.status_code == 401:
                raise VWUnauthorizedException(e, "Unauthorized access")
            elif e.response.status_code == 404:
                raise VWNotFoundException(e, "User not found")
            raise e
