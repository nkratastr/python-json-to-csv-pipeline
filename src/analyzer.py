"""
JSON Structure Analyzer Module
Analyzes JSON structure to detect nesting levels, array types, and field information.
"""

import json
from typing import Dict, List, Any, Optional, Tuple
from pathlib import Path
from dataclasses import dataclass, field
import logging

logger = logging.getLogger(__name__)


@dataclass
class FieldInfo:
    """Information about a single field in JSON structure."""
    name: str
    field_type: str  # string, number, boolean, object, array, null
    path: str  # Full dot-notation path
    depth: int
    is_array_of_objects: bool = False
    array_item_count: int = 0  # Average items in array
    sample_value: Any = None
    children: List['FieldInfo'] = field(default_factory=list)


@dataclass 
class StructureAnalysis:
    """Complete analysis of JSON structure."""
    file_path: str
    file_size_mb: float
    record_count: int
    max_depth: int
    is_nested: bool
    has_array_of_objects: bool
    fields: List[FieldInfo] = field(default_factory=list)
    nested_arrays: List[FieldInfo] = field(default_factory=list)  # Arrays that contain objects
    flat_field_count: int = 0
    estimated_exploded_rows: int = 0


class JSONAnalyzer:
    """
    Analyzes JSON file structure to help user choose conversion mode.
    
    Detects:
    - Nesting depth
    - Arrays of objects (need special handling)
    - Field types and sample values
    - Estimated output sizes for each mode
    """
    
    def __init__(self, file_path: str):
        self.file_path = Path(file_path)
        self.sample_data: List[Dict] = []
        self.analysis: Optional[StructureAnalysis] = None
        
    def analyze(self, sample_size: int = 5) -> StructureAnalysis:
        """
        Analyze JSON file structure.
        
        Args:
            sample_size: Number of records to analyze for structure detection
            
        Returns:
            StructureAnalysis with complete structure information
        """
        logger.info(f"Analyzing JSON structure: {self.file_path}")
        
        # Get file size
        file_size_mb = self.file_path.stat().st_size / (1024 * 1024)
        
        # Load sample data
        with open(self.file_path, 'r', encoding='utf-8') as f:
            data = json.load(f)
        
        # Normalize to list
        if isinstance(data, dict):
            # Check for common wrapper keys
            for key in ['data', 'records', 'results', 'items', 'rows']:
                if key in data and isinstance(data[key], list):
                    data = data[key]
                    break
            else:
                data = [data]
        
        record_count = len(data)
        self.sample_data = data[:sample_size]
        
        # Analyze structure from first record
        fields, max_depth = self._analyze_record(self.sample_data[0] if self.sample_data else {})
        
        # Find nested arrays (arrays of objects)
        nested_arrays = self._find_nested_arrays(fields)
        
        # Calculate estimated exploded rows
        exploded_rows = self._estimate_exploded_rows(data[:min(100, len(data))], nested_arrays)
        
        # Count flat fields
        flat_count = self._count_flat_fields(fields)
        
        self.analysis = StructureAnalysis(
            file_path=str(self.file_path),
            file_size_mb=round(file_size_mb, 2),
            record_count=record_count,
            max_depth=max_depth,
            is_nested=max_depth > 1,
            has_array_of_objects=len(nested_arrays) > 0,
            fields=fields,
            nested_arrays=nested_arrays,
            flat_field_count=flat_count,
            estimated_exploded_rows=exploded_rows
        )
        
        logger.info(f"Analysis complete: {record_count} records, depth={max_depth}, nested_arrays={len(nested_arrays)}")
        return self.analysis
    
    def _analyze_record(self, record: Dict, path: str = "", depth: int = 0) -> Tuple[List[FieldInfo], int]:
        """Recursively analyze a record's structure."""
        fields = []
        max_depth = depth
        
        for key, value in record.items():
            current_path = f"{path}.{key}" if path else key
            field_type = self._get_type(value)
            
            field_info = FieldInfo(
                name=key,
                field_type=field_type,
                path=current_path,
                depth=depth,
                sample_value=self._get_sample_value(value)
            )
            
            if isinstance(value, dict):
                # Nested object - recurse
                children, child_depth = self._analyze_record(value, current_path, depth + 1)
                field_info.children = children
                max_depth = max(max_depth, child_depth)
                
            elif isinstance(value, list) and value:
                first_item = value[0]
                if isinstance(first_item, dict):
                    # Array of objects - important!
                    field_info.is_array_of_objects = True
                    field_info.array_item_count = len(value)
                    children, child_depth = self._analyze_record(first_item, current_path, depth + 1)
                    field_info.children = children
                    max_depth = max(max_depth, child_depth)
                else:
                    # Array of primitives
                    field_info.array_item_count = len(value)
            
            fields.append(field_info)
        
        return fields, max_depth
    
    def _get_type(self, value: Any) -> str:
        """Get JSON type string for a value."""
        if value is None:
            return "null"
        elif isinstance(value, bool):
            return "boolean"
        elif isinstance(value, int):
            return "integer"
        elif isinstance(value, float):
            return "number"
        elif isinstance(value, str):
            return "string"
        elif isinstance(value, dict):
            return "object"
        elif isinstance(value, list):
            return "array"
        return "unknown"
    
    def _get_sample_value(self, value: Any, max_length: int = 30) -> str:
        """Get a truncated sample value for display."""
        if value is None:
            return "null"
        elif isinstance(value, (dict, list)):
            s = json.dumps(value, ensure_ascii=False)
            if len(s) > max_length:
                return s[:max_length-3] + "..."
            return s
        else:
            s = str(value)
            if len(s) > max_length:
                return s[:max_length-3] + "..."
            return s
    
    def _find_nested_arrays(self, fields: List[FieldInfo]) -> List[FieldInfo]:
        """Find all arrays that contain objects (need special handling)."""
        nested = []
        
        def recurse(field_list: List[FieldInfo]):
            for f in field_list:
                if f.is_array_of_objects:
                    nested.append(f)
                if f.children:
                    recurse(f.children)
        
        recurse(fields)
        return nested
    
    def _count_flat_fields(self, fields: List[FieldInfo]) -> int:
        """Count total flat fields after flattening nested objects."""
        count = 0
        
        def recurse(field_list: List[FieldInfo]):
            nonlocal count
            for f in field_list:
                if f.field_type in ('string', 'integer', 'number', 'boolean', 'null'):
                    count += 1
                elif f.field_type == 'array' and not f.is_array_of_objects:
                    count += 1  # Will become pipe-separated string
                elif f.field_type == 'array' and f.is_array_of_objects:
                    count += 1  # Will become JSON string (flat mode)
                elif f.field_type == 'object':
                    recurse(f.children)
        
        recurse(fields)
        return count
    
    def _estimate_exploded_rows(self, sample_data: List[Dict], nested_arrays: List[FieldInfo]) -> int:
        """Estimate total rows after full explosion."""
        if not nested_arrays:
            return len(sample_data)
        
        total_multiplier = 0
        
        for record in sample_data:
            record_multiplier = 1
            for arr_field in nested_arrays:
                # Navigate to the array in the record
                value = self._get_nested_value(record, arr_field.path)
                if isinstance(value, list):
                    record_multiplier *= max(1, len(value))
            total_multiplier += record_multiplier
        
        # Extrapolate to full dataset
        if sample_data:
            avg_multiplier = total_multiplier / len(sample_data)
            return int(avg_multiplier * self.analysis.record_count if self.analysis else total_multiplier)
        return total_multiplier
    
    def _get_nested_value(self, record: Dict, path: str) -> Any:
        """Get value from nested path like 'projects' or 'projects.tasks'."""
        parts = path.split('.')
        value = record
        
        for part in parts:
            if isinstance(value, dict):
                value = value.get(part)
            elif isinstance(value, list) and value:
                # For arrays, check first item
                value = value[0].get(part) if isinstance(value[0], dict) else None
            else:
                return None
        
        return value
    
    def get_sample_record(self) -> Optional[Dict]:
        """Get first sample record for preview."""
        return self.sample_data[0] if self.sample_data else None
    
    def print_structure_tree(self) -> str:
        """Generate ASCII tree representation of structure."""
        if not self.analysis:
            return "No analysis available. Call analyze() first."
        
        lines = []
        
        def print_field(field: FieldInfo, prefix: str = "", is_last: bool = True):
            connector = "└── " if is_last else "├── "
            type_indicator = ""
            
            if field.is_array_of_objects:
                type_indicator = f" (array of objects, ~{field.array_item_count} items)"
            elif field.field_type == "array":
                type_indicator = f" (array, ~{field.array_item_count} items)"
            elif field.field_type == "object":
                type_indicator = " (object)"
            else:
                type_indicator = f" ({field.field_type})"
            
            lines.append(f"{prefix}{connector}{field.name}{type_indicator}")
            
            if field.children:
                new_prefix = prefix + ("    " if is_last else "│   ")
                for i, child in enumerate(field.children):
                    print_field(child, new_prefix, i == len(field.children) - 1)
        
        for i, field in enumerate(self.analysis.fields):
            print_field(field, "", i == len(self.analysis.fields) - 1)
        
        return "\n".join(lines)
