"""
Explode Conversion Mode
Fully explodes nested arrays - one row per deepest nested item.
"""

import json
import pandas as pd
import numpy as np
from typing import Dict, List, Any, Optional
import logging

logger = logging.getLogger(__name__)


class ExplodeConverter:
    """
    Converts JSON to single fully-exploded CSV.
    
    Behavior:
    - Nested dicts: flattened with dot notation
    - Arrays of primitives: pipe-separated string
    - Arrays of objects: EXPLODED - each item becomes a separate row
    
    Result: Parent data is duplicated for each child item.
    """
    
    def __init__(self):
        self.mode_name = "explode"
    
    def convert(self, data: List[Dict]) -> Dict[str, pd.DataFrame]:
        """
        Convert JSON data to fully exploded DataFrame.
        
        Args:
            data: List of JSON records
            
        Returns:
            Dict with single key 'main' containing the exploded DataFrame
        """
        logger.info(f"Converting {len(data)} records using EXPLODE mode")
        
        all_rows = []
        for record in data:
            rows = self._explode_record(record)
            all_rows.extend(rows)
        
        df = pd.DataFrame(all_rows)
        
        logger.info(f"Created exploded DataFrame: {df.shape[0]} rows, {df.shape[1]} columns")
        return {"main": df}
    
    def _explode_record(self, record: Dict, prefix: str = "") -> List[Dict]:
        """
        Recursively explode a record's arrays of objects.
        
        Args:
            record: JSON record to explode
            prefix: Current path prefix for nested fields
            
        Returns:
            List of flattened rows
        """
        # Separate flat fields from arrays
        flat_fields = {}
        arrays_to_explode = []
        
        for key, value in record.items():
            full_key = f"{prefix}{key}" if prefix else key
            
            if isinstance(value, np.ndarray):
                value = value.tolist()
            
            if isinstance(value, dict):
                # Flatten nested dict
                nested_flat = self._flatten_dict(value, full_key + ".")
                flat_fields.update(nested_flat)
                
            elif isinstance(value, list) and value and isinstance(value[0], dict):
                # Array of objects - need to explode
                arrays_to_explode.append((full_key, value))
                
            elif isinstance(value, list):
                # Array of primitives - join with pipe
                flat_fields[full_key] = "|".join(str(v) for v in value) if value else ""
                
            else:
                flat_fields[full_key] = value
        
        # If no arrays to explode, return single row
        if not arrays_to_explode:
            return [flat_fields]
        
        # Explode arrays one by one
        rows = [flat_fields]
        
        for array_name, array_items in arrays_to_explode:
            new_rows = []
            
            for base_row in rows:
                for item in array_items:
                    # Clone base row
                    row = base_row.copy()
                    
                    # Recursively explode the array item
                    exploded_items = self._explode_record(item, f"{array_name}.")
                    
                    for exploded_item in exploded_items:
                        final_row = row.copy()
                        final_row.update(exploded_item)
                        new_rows.append(final_row)
            
            rows = new_rows
        
        return rows
    
    def _flatten_dict(self, d: Dict, prefix: str = "") -> Dict:
        """Flatten a nested dictionary with dot notation."""
        flat = {}
        
        for key, value in d.items():
            full_key = f"{prefix}{key}"
            
            if isinstance(value, np.ndarray):
                value = value.tolist()
            
            if isinstance(value, dict):
                flat.update(self._flatten_dict(value, full_key + "."))
                
            elif isinstance(value, list):
                if value and isinstance(value[0], dict):
                    # Keep as JSON for nested arrays within dicts
                    flat[full_key] = json.dumps(value, ensure_ascii=False)
                elif value:
                    flat[full_key] = "|".join(str(v) for v in value)
                else:
                    flat[full_key] = ""
            else:
                flat[full_key] = value
        
        return flat
