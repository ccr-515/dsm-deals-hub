from datetime import datetime
from typing import Literal, Optional

from pydantic import BaseModel, ConfigDict, Field, field_validator, model_validator

from . import config
from .models import DealType, Status
from .utils import normalize_slug, normalize_weekday_pattern, parse_hhmm


class OwnerCreate(BaseModel):
    name: str
    email: str
    phone: Optional[str] = None


class OwnerOut(BaseModel):
    model_config = ConfigDict(from_attributes=True)

    id: int
    name: str
    email: str
    phone: Optional[str]


class VenueBase(BaseModel):
    name: str
    slug: str
    address: str
    neighborhood: Optional[str] = None
    lat: Optional[float] = None
    lng: Optional[float] = None
    phone: Optional[str] = None
    website: Optional[str] = None
    hours_json: Optional[str] = None
    description: Optional[str] = None


class VenueCreate(VenueBase):
    owner_id: Optional[int] = None

    @field_validator("slug")
    @classmethod
    def normalize_slug_value(cls, value: str) -> str:
        return normalize_slug(value)


class VenueUpdate(BaseModel):
    owner_id: Optional[int] = None
    name: Optional[str] = None
    slug: Optional[str] = None
    address: Optional[str] = None
    neighborhood: Optional[str] = None
    lat: Optional[float] = None
    lng: Optional[float] = None
    phone: Optional[str] = None
    website: Optional[str] = None
    hours_json: Optional[str] = None
    description: Optional[str] = None

    @field_validator("slug")
    @classmethod
    def normalize_slug_value(cls, value: Optional[str]) -> Optional[str]:
        if value is None:
            return None
        return normalize_slug(value)


class VenueOut(VenueBase):
    model_config = ConfigDict(from_attributes=True)

    id: int
    owner_id: Optional[int] = None
    created_at: datetime
    updated_at: datetime


class AdminVenueOut(VenueOut):
    owner_name: Optional[str] = None
    deal_count: int = 0
    live_deal_count: int = 0


class VenueSummaryOut(BaseModel):
    model_config = ConfigDict(from_attributes=True)

    id: int
    name: str
    slug: str
    neighborhood: Optional[str] = None


class DealBase(BaseModel):
    title: str
    short_description: str
    age_21_plus: bool = False
    menu_link: Optional[str] = None
    image_url: Optional[str] = None
    sponsored: bool = False
    source_type: Literal["admin", "business", "scrape"] = "admin"
    source_url: Optional[str] = None
    source_text: Optional[str] = None
    source_posted_at: Optional[datetime] = None
    notes_private: Optional[str] = None


class WeeklyDealCreate(DealBase):
    venue_id: int
    weekday_pattern: str = Field(description="Mon,Tue,... or All")
    start_time: str = Field(description="HH:MM")
    end_time: str = Field(description="HH:MM")

    @field_validator("weekday_pattern")
    @classmethod
    def normalize_weekdays(cls, value: str) -> str:
        return normalize_weekday_pattern(value)

    @field_validator("start_time", "end_time")
    @classmethod
    def validate_times(cls, value: str) -> str:
        parse_hhmm(value)
        return value

    @model_validator(mode="after")
    def validate_range(self):
        if parse_hhmm(self.end_time) <= parse_hhmm(self.start_time):
            raise ValueError("end_time must be after start_time")
        return self


class LastMinuteDealCreate(DealBase):
    venue_id: int
    start_at: datetime
    end_at: datetime

    @model_validator(mode="after")
    def validate_range(self):
        if self.end_at <= self.start_at:
            raise ValueError("end_at must be after start_at")
        hours = (self.end_at - self.start_at).total_seconds() / 3600.0
        if hours > config.LAST_MINUTE_MAX_HOURS:
            raise ValueError(f"Last-minute deals max {config.LAST_MINUTE_MAX_HOURS} hours")
        return self


class DealUpdate(BaseModel):
    venue_id: Optional[int] = None
    title: Optional[str] = None
    short_description: Optional[str] = None
    weekday_pattern: Optional[str] = None
    start_time: Optional[str] = None
    end_time: Optional[str] = None
    start_at: Optional[datetime] = None
    end_at: Optional[datetime] = None
    age_21_plus: Optional[bool] = None
    menu_link: Optional[str] = None
    image_url: Optional[str] = None
    sponsored: Optional[bool] = None
    source_type: Optional[Literal["admin", "business", "scrape"]] = None
    source_url: Optional[str] = None
    source_text: Optional[str] = None
    source_posted_at: Optional[datetime] = None
    notes_private: Optional[str] = None
    status: Optional[Status] = None

    @field_validator("weekday_pattern")
    @classmethod
    def normalize_weekdays(cls, value: Optional[str]) -> Optional[str]:
        if value is None:
            return None
        return normalize_weekday_pattern(value)

    @field_validator("start_time", "end_time")
    @classmethod
    def validate_times(cls, value: Optional[str]) -> Optional[str]:
        if value is None:
            return None
        parse_hhmm(value)
        return value


class DealOut(BaseModel):
    model_config = ConfigDict(from_attributes=True)

    id: int
    venue_id: int
    title: str
    short_description: str
    type: DealType
    weekday_pattern: Optional[str] = None
    start_time: Optional[str] = None
    end_time: Optional[str] = None
    start_at: Optional[datetime] = None
    end_at: Optional[datetime] = None
    age_21_plus: bool
    menu_link: Optional[str] = None
    image_url: Optional[str] = None
    sponsored: bool
    status: Status
    created_at: datetime
    updated_at: datetime


class PublicDealOut(DealOut):
    venue: VenueSummaryOut


class AdminDealOut(PublicDealOut):
    source_type: str
    source_url: Optional[str] = None
    source_text: Optional[str] = None
    source_posted_at: Optional[datetime] = None
    notes_private: Optional[str] = None


class ApproveRequest(BaseModel):
    approve: bool = True
    reason: Optional[str] = None
