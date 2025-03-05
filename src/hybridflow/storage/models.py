"""SQLAlchemy ORM models for metadata storage."""

from datetime import datetime

from sqlalchemy import (
    Column,
    DateTime,
    Float,
    ForeignKey,
    Integer,
    String,
    Text,
    UniqueConstraint,
)
from sqlalchemy.orm import declarative_base

Base = declarative_base()


class ChapterMetadata(Base):
    """Chapter metadata tracking for version control and quality monitoring."""

    __tablename__ = "chapter_metadata"

    id = Column(Integer, primary_key=True, autoincrement=True)
    textbook_id = Column(String(50), nullable=False)
    chapter_number = Column(String(10), nullable=False)
    title = Column(String(500), nullable=False)
    content_hash = Column(String(64), nullable=False)
    version = Column(Integer, default=1)
    ingestion_timestamp = Column(DateTime, default=datetime.utcnow)
    source_file_path = Column(String(500))
    chunk_count = Column(Integer, default=0)
    quality_score = Column(Float, nullable=True)

    __table_args__ = (
        UniqueConstraint("textbook_id", "chapter_number", name="uq_textbook_chapter"),
    )


class IngestionLog(Base):
    """Log of ingestion operations for monitoring and debugging."""

    __tablename__ = "ingestion_log"

    id = Column(Integer, primary_key=True, autoincrement=True)
    chapter_id = Column(Integer, ForeignKey("chapter_metadata.id"))
    timestamp = Column(DateTime, default=datetime.utcnow)
    status = Column(String(20), nullable=False)
    parsing_strategy = Column(String(50))
    error_message = Column(Text)
    chunks_inserted = Column(Integer, default=0)
