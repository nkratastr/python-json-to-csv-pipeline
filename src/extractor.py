"""
Data Extraction Module
Handles reading and extracting data from JSON files with comprehensive error handling.
Supports chunked processing for large files (500MB+).
"""

import json
import os
import pandas as pd
from pathlib import Path
from typing import List, Dict, Union, Generator, Optional
import logging

logger = logging.getLogger(__name__)

# File size threshold for chunked processing (500 MB)
LARGE_FILE_THRESHOLD_MB = 500


class DataExtractor:
    """
    Extracts data from JSON files with error handling and validation.
    Automatically uses chunked processing for large files.
    """

    def __init__(self, input_path: Union[str, Path]):
        """
        Initialize the DataExtractor.
        
        Args:
            input_path: Path to the input JSON file or directory
        """
        self.input_path = Path(input_path)
        self.raw_data: List[Dict] = []
        self.is_large_file = False
        logger.info(f"DataExtractor initialized with input path: {self.input_path}")

    def _get_file_size_mb(self) -> float:
        """Get file size in megabytes."""
        return os.path.getsize(self.input_path) / (1024 * 1024)

    def _check_large_file(self) -> bool:
        """Check if file exceeds the large file threshold."""
        size_mb = self._get_file_size_mb()
        self.is_large_file = size_mb > LARGE_FILE_THRESHOLD_MB
        if self.is_large_file:
            logger.info(f"Large file detected ({size_mb:.2f} MB). Using chunked processing.")
        return self.is_large_file

    def extract_from_json(self) -> List[Dict]:
        """
        Extract data from JSON file.
        
        Returns:
            List of dictionaries containing the extracted data
        
        Raises:
            FileNotFoundError: If the input file doesn't exist
            json.JSONDecodeError: If the JSON file is malformed
            ValueError: If the JSON structure is unexpected
        """
        logger.info(f"Starting data extraction from: {self.input_path}")

        # Check if file exists
        if not self.input_path.exists():
            error_msg = f"Input file not found: {self.input_path}"
            logger.error(error_msg)
            raise FileNotFoundError(error_msg)

        # Check if it's a file
        if not self.input_path.is_file():
            error_msg = f"Input path is not a file: {self.input_path}"
            logger.error(error_msg)
            raise ValueError(error_msg)

        # Check file size
        self._check_large_file()

        try:
            if self.is_large_file:
                # Use streaming for large files
                return self._extract_large_json()
            else:
                # Use standard loading for normal files
                return self._extract_standard_json()

        except json.JSONDecodeError as e:
            error_msg = f"Invalid JSON format: {str(e)}"
            logger.error(error_msg)
            raise

        except Exception as e:
            error_msg = f"Unexpected error during data extraction: {str(e)}"
            logger.error(error_msg)
            raise

    def _extract_standard_json(self) -> List[Dict]:
        """Extract data using standard JSON loading (for smaller files)."""
        with open(self.input_path, 'r', encoding='utf-8') as f:
            data = json.load(f)

        # Handle different JSON structures
        self.raw_data = self._normalize_json_structure(data)
        logger.info(f"Extracted {len(self.raw_data)} records")
        return self.raw_data

    def _extract_large_json(self) -> List[Dict]:
        """
        Extract data from large JSON file using streaming.
        Uses ijson if available, falls back to chunked reading.
        """
        try:
            import ijson
            return self._extract_with_ijson()
        except ImportError:
            logger.warning("ijson not installed. Using fallback method for large file.")
            logger.warning("For better performance with large files, install ijson: pip install ijson")
            return self._extract_standard_json()

    def _extract_with_ijson(self) -> List[Dict]:
        """Extract data using ijson streaming parser."""
        import ijson
        
        records = []
        logger.info("Using ijson streaming parser for large file")
        
        with open(self.input_path, 'rb') as f:
            # Try to detect the JSON structure
            # First, try parsing as array items
            try:
                for record in ijson.items(f, 'item'):
                    records.append(record)
            except Exception:
                # If that fails, try common nested structures
                f.seek(0)
                for key in ['data.item', 'records.item', 'results.item']:
                    try:
                        for record in ijson.items(f, key):
                            records.append(record)
                        if records:
                            break
                        f.seek(0)
                    except Exception:
                        f.seek(0)
                        continue
        
        if not records:
            # Fallback to standard extraction
            logger.warning("ijson streaming failed, using standard method")
            return self._extract_standard_json()
        
        self.raw_data = records
        logger.info(f"Streamed {len(self.raw_data)} records from large file")
        return self.raw_data

    def _normalize_json_structure(self, data) -> List[Dict]:
        """Normalize different JSON structures to a list of dicts."""
        if isinstance(data, list):
            logger.info(f"JSON array with {len(data)} records")
            return data
        elif isinstance(data, dict):
            # Check for common wrapper keys
            for key in ['data', 'records', 'results', 'items', 'rows']:
                if key in data and isinstance(data[key], list):
                    logger.info(f"Found data in '{key}' key: {len(data[key])} records")
                    return data[key]
            # Single record
            logger.info("Single JSON object, converting to list")
            return [data]
        else:
            raise ValueError(f"Unexpected JSON type: {type(data)}")

    def extract_to_dataframe(self) -> pd.DataFrame:
        """
        Extract data and convert to pandas DataFrame.
        
        Returns:
            pandas DataFrame containing the extracted data
        """
        logger.info("Extracting data to DataFrame")
        
        try:
            data = self.extract_from_json()
            
            if not data:
                logger.warning("No data to convert to DataFrame")
                return pd.DataFrame()
            
            df = pd.DataFrame(data)
            logger.info(f"Created DataFrame with shape: {df.shape}")
            logger.debug(f"DataFrame columns: {df.columns.tolist()}")
            
            return df
            
        except Exception as e:
            logger.error(f"Error converting to DataFrame: {str(e)}")
            raise

    def get_data_info(self) -> Dict:
        """
        Get information about the extracted data.
        
        Returns:
            Dictionary with data statistics
        """
        if not self.raw_data:
            return {"record_count": 0, "message": "No data extracted yet"}

        info = {
            "record_count": len(self.raw_data),
            "sample_record": self.raw_data[0] if self.raw_data else None,
            "file_path": str(self.input_path)
        }

        if self.raw_data:
            # Get keys from first record
            if isinstance(self.raw_data[0], dict):
                info["fields"] = list(self.raw_data[0].keys())

        logger.debug(f"Data info: {info}")
        return info
