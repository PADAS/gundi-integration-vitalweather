import logging
import httpx
import pydantic
import stamina

from datetime import datetime, timezone
from typing import List
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


class HistoryItem(pydantic.BaseModel):
    ts: datetime
    pressure: float
    temperature: float
    humidity: int
    wind_min: int
    wind_avg: float = pydantic.Field(alias='wind_average')
    wind_max: float
    wind_dir: int = pydantic.Field(alias='wind_direction')
    Rain: float = pydantic.Field(alias='total_rain')
    uv: float
    solar_radiation: float

    @pydantic.validator('ts', always=True)
    def parse_time_string(cls, v):
        if not v.tzinfo:
            return v.replace(tzinfo=timezone.utc)
        return v

    class Config:
        allow_population_by_field_name = True


class Unites(pydantic.BaseModel):
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


class HistoryResponse(pydantic.BaseModel):
    History: List[HistoryItem]
    unites: Unites
    generated_at: datetime
    code: int
    message: str

    @pydantic.validator('generated_at', always=True)
    def parse_time_string(cls, v):
        if not v.tzinfo:
            return v.replace(tzinfo=timezone.utc)
        return v


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
async def get_station_history(integration, base_url, config):
    async with httpx.AsyncClient(timeout=120) as session:
        url = f"{base_url}/history.php"
        params = {
            "ID": config.station.Station_ID,
            "key": config.key.get_secret_value(),
            "from": config.from_timestamp,
            "to": config.to_timestamp,
        }

        logger.info(f"-- Getting historic conditions for integration ID: {integration.id} Station ID: {config.station.Station_ID} --")

        try:
            response = await session.get(url, params=params)
            if response.is_error:
                logger.error(f"Error 'get_station_history'. Response body: {response.text}")
            response.raise_for_status()
            parsed_response = response.json()
            if parsed_response:
                if parsed_response["code"] != 200:
                    raise VWException(
                        error=Exception(parsed_response["message"]),
                        message=parsed_response["message"],
                        status_code=parsed_response["code"]
                    )
                obs = HistoryResponse.parse_obj(parsed_response)
                return obs
            else:
                return response.text
        except httpx.HTTPStatusError as e:
            if e.response.status_code == 401:
                raise VWUnauthorizedException(e, "Unauthorized access")
            elif e.response.status_code == 404:
                raise VWNotFoundException(e, "User not found")
            raise e
