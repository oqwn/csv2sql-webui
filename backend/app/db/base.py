from sqlalchemy.orm import declarative_base

Base = declarative_base()

# Import all models here to register them with Base
from app.models.data_source import DataSource, ExtractionJob  # noqa: E402