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
        # Find ID field for this record
        id_field = self._find_id_field(record)
        id_value = record.get(id_field, str(hash(str(record)))[:8])
        
        # Build main row with flat fields
        row = {}
        
        # Add parent reference if exists
        if parent_id_field and parent_id_value is not None:
            row[parent_id_field] = parent_id_value
        
        # Process each field
        for key, value in record.items():
            if isinstance(value, np.ndarray):
                value = value.tolist()
            
            if isinstance(value, dict):
                # Nested dict - flatten with dot notation
                flat = self._flatten_dict(value, f"{key}.")
                row.update(flat)
                
            elif isinstance(value, list) and value and isinstance(value[0], dict):
                # Array of objects - create child table
                child_table_name = key
                child_id_field = f"{table_name}_{id_field}"
                
                for item in value:
                    self._process_record(
                        record=item,
                        table_name=child_table_name,
                        parent_id_field=child_id_field,
                        parent_id_value=id_value
                    )
                    
            elif isinstance(value, list):
                # Array of primitives - pipe separated
                row[key] = "|".join(str(v) for v in value) if value else ""
                
            else:
                row[key] = value
        
        self.tables[table_name].append(row)
    
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
