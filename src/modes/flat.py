"""
Flat Conversion Mode
Nested dicts are flattened, arrays of objects become JSON strings.
"""

import json
import pandas as pd
import numpy as np
from typing import Dict, List, Any
from pathlib import Path
import logging

logger = logging.getLogger(__name__)


class FlatConverter:
    """
    Converts JSON to single flat CSV.
    
    Behavior:
    - Nested dicts: flattened with dot notation (department.manager.email)
    - Arrays of primitives: pipe-separated string (Python|Java|SQL)
    - Arrays of objects: JSON string ([{"id": 1}, {"id": 2}])
    """
    
    def __init__(self):
        self.mode_name = "flat"
    
    def convert(self, data: List[Dict]) -> Dict[str, pd.DataFrame]:
        """
        Convert JSON data to flat DataFrame.
        
        Args:
            data: List of JSON records
            
        Returns:
            Dict with single key 'main' containing the DataFrame
        """
        logger.info(f"Converting {len(data)} records using FLAT mode")
        
        # Pre-process arrays
        processed_data = self._convert_arrays_to_strings(data)
        
        # Flatten nested dicts
        df = pd.json_normalize(processed_data, sep='.')
        
        logger.info(f"Created flat DataFrame: {df.shape[0]} rows, {df.shape[1]} columns")
        return {"main": df}
    
    def _convert_arrays_to_strings(self, data: List[Dict]) -> List[Dict]:
        """Convert arrays to CSV-friendly strings."""
        processed = []
        
        for record in data:
            processed.append(self._process_record(record))
        
        return processed
    
    def _process_record(self, record: Dict) -> Dict:
        """Process a single record, converting arrays."""
        result = {}
        
        for key, value in record.items():
            result[key] = self._process_value(value)
        
        return result
    
    def _process_value(self, value: Any) -> Any:
        """Process a value, handling arrays and nested structures."""
        if value is None:
            return None
        
        # Handle numpy arrays
        if isinstance(value, np.ndarray):
            value = value.tolist()
        
        if isinstance(value, list):
            if not value:
                return ""
            
            first_item = value[0]
            
            # Array of primitives -> pipe-separated
            if isinstance(first_item, (str, int, float, bool)):
                return "|".join(str(v) for v in value)
            
            # Array of objects -> JSON string
            elif isinstance(first_item, dict):
                return json.dumps(value, ensure_ascii=False)
            
            # Nested arrays -> JSON string
            else:
                return json.dumps(value, ensure_ascii=False)
        
        elif isinstance(value, dict):
            # Process nested dict values
            return {k: self._process_value(v) for k, v in value.items()}
        
        return value
