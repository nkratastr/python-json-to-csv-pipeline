"""
Relational Conversion Mode
Creates separate linked CSV files for each array of objects.
"""

import json
import pandas as pd
import numpy as np
from typing import Dict, List, Any, Optional, Tuple
from collections import defaultdict
import logging

logger = logging.getLogger(__name__)


class RelationalConverter:
    """
    Converts JSON to multiple linked CSVs (relational model).
    
    Behavior:
    - Main table: flat fields + nested dicts (flattened)
    - Child tables: one per array of objects, with parent ID reference
    - Grandchild tables: for nested arrays within arrays
    
    Result: Normalized data without duplication.
    """
    
    def __init__(self):
        self.mode_name = "relational"
        self.tables: Dict[str, List[Dict]] = defaultdict(list)
    
    def convert(self, data: List[Dict]) -> Dict[str, pd.DataFrame]:
        """
        Convert JSON data to multiple related DataFrames.
        
        Args:
            data: List of JSON records
            
        Returns:
            Dict mapping table names to DataFrames
        """
        logger.info(f"Converting {len(data)} records using RELATIONAL mode")
        
        self.tables = defaultdict(list)
        
        # Detect main ID field
        sample = data[0] if data else {}
        main_id_field = self._find_id_field(sample)
        main_table_name = self._infer_table_name(sample, main_id_field)
        
        # Process each record
        for record in data:
            self._process_record(
                record=record,
                table_name=main_table_name,
                parent_id_field=None,
                parent_id_value=None
            )
        
        # Convert to DataFrames
        result = {}
        for table_name, rows in self.tables.items():
            if rows:
                df = pd.DataFrame(rows)
                result[table_name] = df
                logger.info(f"Created table '{table_name}': {len(rows)} rows, {len(df.columns)} columns")
        
        return result
    
    def _process_record(
        self,
        record: Dict,
        table_name: str,
        parent_id_field: Optional[str],
        parent_id_value: Any
    ) -> None:
        """
        Process a record and extract related tables.
        
        Args:
            record: JSON record to process
            table_name: Name for this table
            parent_id_field: Parent's ID field name (for foreign key)
            parent_id_value: Parent's ID value
        """
        # Find ID field for this record (look inside wrapper objects too)
        id_field, id_value = self._find_id_recursive(record)
        
        # Build main row with flat fields
        row = {}
        
        # Add parent reference if exists
        if parent_id_field and parent_id_value is not None:
            row[parent_id_field] = parent_id_value
        
        # Process each field - collect arrays for child tables
        self._process_fields(record, row, table_name, id_field, id_value, prefix="")
        
        self.tables[table_name].append(row)
    
    def _process_fields(
        self,
        obj: Dict,
        row: Dict,
        table_name: str,
        parent_id_field: str,
        parent_id_value: Any,
        prefix: str = ""
    ) -> None:
        """
        Recursively process fields, extracting child tables from nested arrays.
        """
        for key, value in obj.items():
            full_key = f"{prefix}{key}" if prefix else key
            
            if isinstance(value, np.ndarray):
                value = value.tolist()
            
            if isinstance(value, dict):
                # Check if this dict contains arrays of objects
                has_nested_arrays = any(
                    isinstance(v, list) and v and isinstance(v[0], dict)
                    for v in value.values()
                )
                
                if has_nested_arrays:
                    # Recurse into dict to find and extract arrays
                    self._process_fields(
                        value, row, table_name, parent_id_field, parent_id_value,
                        prefix=full_key + "."
                    )
                else:
                    # Simple nested dict - flatten with dot notation
                    flat = self._flatten_dict(value, f"{full_key}.")
                    row.update(flat)
                
            elif isinstance(value, list) and value and isinstance(value[0], dict):
                # Array of objects - create child table
                child_table_name = key
                child_id_field = f"{table_name}_{parent_id_field}"
                
                for item in value:
                    self._process_record(
                        record=item,
                        table_name=child_table_name,
                        parent_id_field=child_id_field,
                        parent_id_value=parent_id_value
                    )
                    
            elif isinstance(value, list):
                # Array of primitives - pipe separated
                row[full_key] = "|".join(str(v) for v in value) if value else ""
                
            else:
                row[full_key] = value
    
    def _find_id_recursive(self, record: Dict) -> Tuple[str, Any]:
        """Find ID field, even if nested in wrapper object like 'employee'."""
        # Check top level
        id_field = self._find_id_field(record)
        if id_field in record and not isinstance(record[id_field], dict):
            return id_field, record.get(id_field, str(hash(str(record)))[:8])
        
        # Check inside wrapper objects
        for key, value in record.items():
            if isinstance(value, dict):
                nested_id = self._find_id_field(value)
                if nested_id in value and not isinstance(value[nested_id], dict):
                    return nested_id, value.get(nested_id, str(hash(str(record)))[:8])
        
        # Fallback
        return "id", str(hash(str(record)))[:8]
    
    def _flatten_dict(self, d: Dict, prefix: str = "") -> Dict:
        """Flatten nested dictionary with dot notation."""
        flat = {}
        
        for key, value in d.items():
            full_key = f"{prefix}{key}"
            
            if isinstance(value, np.ndarray):
                value = value.tolist()
            
            if isinstance(value, dict):
                flat.update(self._flatten_dict(value, full_key + "."))
                
            elif isinstance(value, list):
                if value and isinstance(value[0], dict):
                    # Deep nested array - keep as JSON
                    flat[full_key] = json.dumps(value, ensure_ascii=False)
                elif value:
                    flat[full_key] = "|".join(str(v) for v in value)
                else:
                    flat[full_key] = ""
            else:
                flat[full_key] = value
        
        return flat
    
    def _find_id_field(self, record: Dict) -> str:
        """Find the most likely ID field in a record."""
        # Priority order for ID field detection
        id_patterns = [
            'id', '_id', 'ID', 'Id',
            'employee_id', 'employeeId', 'employeeid',
            'user_id', 'userId', 'userid',
            'project_id', 'projectId', 'projectid',
            'task_id', 'taskId', 'taskid',
            'order_id', 'orderId', 'orderid',
            'item_id', 'itemId', 'itemid',
        ]
        
        # Check exact matches first
        for pattern in id_patterns:
            if pattern in record:
                return pattern
        
        # Check fields ending with 'id' or 'Id'
        for key in record.keys():
            lower = key.lower()
            if lower.endswith('id') and len(key) <= 20:
                return key
        
        # Fallback to first field
        return list(record.keys())[0] if record else "id"
    
    def _infer_table_name(self, record: Dict, id_field: str) -> str:
        """Infer main table name from ID field."""
        # Try to extract entity name from ID field
        # e.g., "employee_id" -> "employees", "projectId" -> "projects"
        
        if id_field == "id" or id_field == "_id":
            return "main"
        
        # Remove common suffixes
        name = id_field
        for suffix in ['_id', 'Id', 'ID', '_ID']:
            if name.endswith(suffix):
                name = name[:-len(suffix)]
                break
        
        # Pluralize (simple)
        if name and not name.endswith('s'):
            name = name + 's'
        
        return name if name else "main"
    
    def get_table_relationships(self) -> List[Tuple[str, str, str]]:
        """
        Get relationships between tables.
        
        Returns:
            List of (child_table, parent_table, foreign_key) tuples
        """
        relationships = []
        
        for table_name, rows in self.tables.items():
            if rows:
                sample = rows[0]
                for field in sample.keys():
                    if '_' in field and field.endswith(tuple(['_id', '_Id', '_ID'])):
                        # This looks like a foreign key
                        parts = field.rsplit('_', 1)
                        parent_table = parts[0]
                        if parent_table in self.tables:
                            relationships.append((table_name, parent_table, field))
        
        return relationships
