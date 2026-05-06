from __future__ import annotations
from datetime import datetime, timezone
from typing import Optional

from pydantic import BaseModel, field_validator, model_validator


class AqiRow(BaseModel):
    time: datetime
    region_id: str
    station_id: str
    lat: Optional[float] = None
    lon: Optional[float] = None
    parameter: str
    aqi: Optional[int] = None
    concentration: Optional[float] = None
    unit: Optional[str] = None
    category: Optional[str] = None

    def to_db(self) -> dict:
        return self.model_dump()


class WeatherRow(BaseModel):
    time: datetime
    region_id: str
    temperature_2m: Optional[float] = None
    apparent_temperature: Optional[float] = None
    relative_humidity: Optional[float] = None
    precipitation: Optional[float] = None
    wind_speed_10m: Optional[float] = None
    wind_direction_10m: Optional[float] = None
    cloud_cover: Optional[float] = None
    surface_pressure: Optional[float] = None
    is_forecast: bool = False

    def to_db(self) -> dict:
        return self.model_dump()


class GridGenerationRow(BaseModel):
    time: datetime
    region_id: str
    natural_gas_mwh: Optional[float] = None
    coal_mwh: Optional[float] = None
    nuclear_mwh: Optional[float] = None
    wind_mwh: Optional[float] = None
    solar_mwh: Optional[float] = None
    hydro_mwh: Optional[float] = None
    other_mwh: Optional[float] = None
    total_mwh: Optional[float] = None
    nuclear_pct: Optional[float] = None
    fossil_pct: Optional[float] = None
    clean_pct: Optional[float] = None

    @field_validator("time", mode="before")
    @classmethod
    def parse_eia_period(cls, v):
        if isinstance(v, str):
            # EIA returns period as "2024-01-01T00" (no minutes)
            for fmt in ("%Y-%m-%dT%H", "%Y-%m-%dT%H:%M", "%Y-%m-%d"):
                try:
                    return datetime.strptime(v, fmt).replace(tzinfo=timezone.utc)
                except ValueError:
                    continue
        return v

    @model_validator(mode="after")
    def compute_percentages(self) -> GridGenerationRow:
        total = self.total_mwh or 0.0
        if total > 0:
            self.nuclear_pct = round((self.nuclear_mwh or 0) / total * 100, 2)
            self.fossil_pct = round(
                ((self.natural_gas_mwh or 0) + (self.coal_mwh or 0)) / total * 100, 2
            )
            self.clean_pct = round(
                (
                    (self.nuclear_mwh or 0)
                    + (self.wind_mwh or 0)
                    + (self.solar_mwh or 0)
                    + (self.hydro_mwh or 0)
                )
                / total
                * 100,
                2,
            )
        return self

    def to_db(self) -> dict:
        return self.model_dump()


class GridDemandRow(BaseModel):
    time: datetime
    region_id: str
    demand_mw: float
    forecast_mw: Optional[float] = None
    source: str

    @field_validator("time", mode="before")
    @classmethod
    def parse_eia_period(cls, v):
        if isinstance(v, str):
            for fmt in ("%Y-%m-%dT%H", "%Y-%m-%dT%H:%M", "%Y-%m-%d"):
                try:
                    return datetime.strptime(v, fmt).replace(tzinfo=timezone.utc)
                except ValueError:
                    continue
        return v

    def to_db(self) -> dict:
        return self.model_dump()


class NuclearStatusRow(BaseModel):
    time: datetime
    unit_name: str
    power_pct: Optional[float] = None
    status_code: Optional[str] = None
    state_code: Optional[str] = None
    operator: Optional[str] = None
    region_id: Optional[str] = None

    @field_validator("time", mode="before")
    @classmethod
    def parse_nrc_date(cls, v):
        if isinstance(v, str):
            v = v.strip()
            for fmt in ("%m/%d/%Y %I:%M:%S %p", "%m/%d/%Y", "%Y-%m-%d"):
                try:
                    return datetime.strptime(v, fmt).replace(tzinfo=timezone.utc)
                except ValueError:
                    continue
            raise ValueError(f"Cannot parse NRC date: {v!r}")
        return v

    @field_validator("power_pct", mode="before")
    @classmethod
    def coerce_power_pct(cls, v):
        if v is None or v == "":
            return None
        try:
            return float(v)
        except (ValueError, TypeError):
            return None

    def to_db(self) -> dict:
        return self.model_dump()


class SeismicRow(BaseModel):
    time: datetime
    event_id: str
    magnitude: float
    magnitude_type: Optional[str] = None
    depth_km: Optional[float] = None
    lat: float
    lon: float
    place: Optional[str] = None
    alert_level: Optional[str] = None
    tsunami: bool = False
    region_id: Optional[str] = None

    @field_validator("time", mode="before")
    @classmethod
    def parse_usgs_time(cls, v):
        # USGS returns time as Unix milliseconds
        if isinstance(v, (int, float)):
            return datetime.fromtimestamp(v / 1000, tz=timezone.utc)
        return v

    def to_db(self) -> dict:
        return self.model_dump()
