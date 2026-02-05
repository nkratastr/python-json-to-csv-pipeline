"""
Data Extraction Module
Handles reading and extracting data from JSON files with comprehensive error handling.
"""

import json
import pandas as pd
from pathlib import Path
from typing import List, Dict, Union
import logging

logger = logging.getLogger(__name__)


class DataExtractor:
    """
    Extracts data from JSON files with error handling and validation.
    """

    def __init__(self, input_path: Union[str, Path]):
        """
        Initialize the DataExtractor.
        
        Args:
            input_path: Path to the input JSON file or directory
        """
        self.input_path = Path(input_path)
        self.raw_data: List[Dict] = []
        logger.info(f"DataExtractor initialized with input path: {self.input_path}")

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

        try:
            # Read JSON file
            with open(self.input_path, 'r', encoding='utf-8') as f:
                data = json.load(f)

            # Handle different JSON structures
            if isinstance(data, list):
                self.raw_data = data
                logger.info(f"Extracted {len(self.raw_data)} records from JSON array")
            elif isinstance(data, dict):
                # If it's a dict, check if there's a key containing the array
                if 'data' in data:
                    self.raw_data = data['data']
                elif 'records' in data:
                    self.raw_data = data['records']
                elif 'results' in data:
                    self.raw_data = data['results']
                else:
                    # Treat the single dict as a list with one item
                    self.raw_data = [data]
                logger.info(f"Extracted {len(self.raw_data)} records from JSON object")
            else:
                error_msg = f"Unexpected JSON structure. Expected list or dict, got {type(data)}"
                logger.error(error_msg)
                raise ValueError(error_msg)

            # Validate that we have data
            if not self.raw_data:
                logger.warning("No data found in JSON file")
            
            logger.info(f"Data extraction completed successfully. Total records: {len(self.raw_data)}")
            return self.raw_data

        except json.JSONDecodeError as e:
            error_msg = f"Invalid JSON format: {str(e)}"
            logger.error(error_msg)
            raise json.JSONDecodeError(error_msg, e.doc, e.pos)

        except Exception as e:
            error_msg = f"Unexpected error during data extraction: {str(e)}"
            logger.error(error_msg)
            raise Exception(error_msg)

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
