"""
Data Validation Module (Optional)

This module provides OPTIONAL schema validation for advanced use cases.
The main pipeline works WITHOUT any validation - it auto-detects JSON structure.

Only use this if you need to enforce specific data quality rules.
For most cases, just use the pipeline directly without validation.
"""

from typing import List, Optional, Any, Dict
from pydantic import BaseModel, Field, ConfigDict
import logging

logger = logging.getLogger(__name__)


class FlexibleRecord(BaseModel):
    """
    Generic model that accepts ANY fields from JSON.
    This is used when validation is enabled but no specific schema is defined.
    """
    model_config = ConfigDict(extra='allow', str_strip_whitespace=True)


class DataValidator:
    """
    Optional validator for advanced use cases.
    
    NOTE: The main pipeline does NOT require validation.
    It automatically handles any JSON structure.
    
    Only use this class if you need to:
    - Enforce specific field requirements
    - Validate data types strictly
    - Filter out invalid records
    """

    def __init__(self, schema_model: type[BaseModel] = FlexibleRecord):
        """
        Initialize validator with a Pydantic model.
        
        Args:
            schema_model: Pydantic model for validation (default: FlexibleRecord)
        """
        self.schema_model = schema_model
        self.validated_records: List[BaseModel] = []
        self.failed_records: List[dict] = []

    def validate_records(self, records: List[dict], strict: bool = False) -> tuple[List[dict], List[dict]]:
        """
        Validate records against schema.
        
        Args:
            records: List of dictionaries to validate
            strict: If True, raises error on first failure
        
        Returns:
            Tuple of (valid_records, invalid_records)
        """
        valid_records = []
        invalid_records = []

        logger.info(f"Validating {len(records)} records")

        for idx, record in enumerate(records):
            try:
                validated = self.schema_model(**record)
                valid_records.append(validated.model_dump())
            except Exception as e:
                error_info = {
                    "record_index": idx,
                    "record_data": record,
                    "error": str(e)
                }
                invalid_records.append(error_info)
                logger.warning(f"Record {idx + 1} validation failed: {str(e)}")

                if strict:
                    raise ValueError(f"Validation failed at record {idx + 1}: {str(e)}")

        logger.info(f"Validation: {len(valid_records)} valid, {len(invalid_records)} invalid")
        return valid_records, invalid_records
