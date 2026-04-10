
from sqlalchemy import Column, Integer, String, Boolean, DateTime, Float, Enum, ForeignKey, Text
from sqlalchemy.orm import relationship
from datetime import datetime
import enum

from .database import Base

class DealType(str, enum.Enum):
    weekly = "weekly"
    last_minute = "last_minute"

class Status(str, enum.Enum):
    draft = "draft"
    queued = "queued"
    live = "live"
    expired = "expired"
    archived = "archived"
    rejected = "rejected"

class BusinessOwner(Base):
    __tablename__ = "business_owners"
    id = Column(Integer, primary_key=True, index=True)
    name = Column(String, nullable=False)
    email = Column(String, unique=True, index=True, nullable=False)
    phone = Column(String, nullable=True)
    created_at = Column(DateTime, default=datetime.utcnow)

    venues = relationship("Venue", back_populates="owner")

class Venue(Base):
    __tablename__ = "venues"
    id = Column(Integer, primary_key=True, index=True)
    owner_id = Column(Integer, ForeignKey("business_owners.id"), nullable=True)
    name = Column(String, nullable=False)
    slug = Column(String, unique=True, index=True, nullable=False)
    address = Column(String, nullable=False)
    neighborhood = Column(String, nullable=True)
    lat = Column(Float, nullable=True)
    lng = Column(Float, nullable=True)
    phone = Column(String, nullable=True)
    website = Column(String, nullable=True)
    hours_json = Column(Text, nullable=True)
    description = Column(Text, nullable=True)
    created_at = Column(DateTime, default=datetime.utcnow)
    updated_at = Column(DateTime, default=datetime.utcnow, onupdate=datetime.utcnow)

    owner = relationship("BusinessOwner", back_populates="venues")
    deals = relationship("Deal", back_populates="venue")

class Deal(Base):
    __tablename__ = "deals"
    id = Column(Integer, primary_key=True, index=True)
    venue_id = Column(Integer, ForeignKey("venues.id"), nullable=False)
    title = Column(String, nullable=False)
    short_description = Column(String, nullable=False)
    type = Column(Enum(DealType), nullable=False)
    # weekly pattern like 'Mon,Tue' or 'All'
    weekday_pattern = Column(String, nullable=True)

    # Simple time-of-day fields for weekly; absolute timestamps for instances/last-minute
    start_time = Column(String, nullable=True)  # HH:MM
    end_time = Column(String, nullable=True)    # HH:MM
    start_at = Column(DateTime, nullable=True)
    end_at = Column(DateTime, nullable=True)

    age_21_plus = Column(Boolean, default=False)
    menu_link = Column(String, nullable=True)
    image_url = Column(String, nullable=True)

    sponsored = Column(Boolean, default=False)
    status = Column(Enum(Status), default=Status.draft, nullable=False)

    source_type = Column(String, default="admin")  # admin|business|scrape
    source_url = Column(String, nullable=True)
    source_text = Column(Text, nullable=True)
    source_posted_at = Column(DateTime, nullable=True)
    notes_private = Column(Text, nullable=True)

    freeze_minutes = Column(Integer, default=30)

    created_at = Column(DateTime, default=datetime.utcnow)
    updated_at = Column(DateTime, default=datetime.utcnow, onupdate=datetime.utcnow)

    venue = relationship("Venue", back_populates="deals")

class MetricEvent(Base):
    __tablename__ = "events_metrics"
    id = Column(Integer, primary_key=True, index=True)
    deal_id = Column(Integer, ForeignKey("deals.id"), nullable=False)
    kind = Column(String, nullable=False)  # view|click_menu|click_directions|click_call|save|share
    ts = Column(DateTime, default=datetime.utcnow)
    ip_hash = Column(String, nullable=True)
