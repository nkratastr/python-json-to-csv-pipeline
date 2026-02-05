"""
Interactive Preview Module
Shows JSON structure analysis and conversion mode options with examples.
"""

import json
from typing import Dict, List, Any, Optional
from dataclasses import dataclass
import pandas as pd

from src.analyzer import JSONAnalyzer, StructureAnalysis, FieldInfo


@dataclass
class ModePreview:
    """Preview information for a conversion mode."""
    mode_id: int
    name: str
    description: str
    detail_description: str  # More detailed explanation
    output_files: int
    output_file_names: List[str]  # Actual CSV file names
    estimated_rows: int
    sample_table: str  # ASCII table preview
    pros: List[str]
    cons: List[str]


class PreviewGenerator:
    """
    Generates interactive preview for JSON to CSV conversion.
    Shows structure analysis and mode options with examples.
    """
    
    def __init__(self, analyzer: JSONAnalyzer):
        self.analyzer = analyzer
        self.analysis = analyzer.analysis
        self.sample_record = analyzer.get_sample_record()
    
    def generate_header(self) -> str:
        """Generate the analysis header section."""
        lines = [
            "",
            "=" * 65,
            "  JSON STRUCTURE ANALYSIS",
            "=" * 65,
            f"  File: {self.analysis.file_path}",
            f"  Size: {self.analysis.file_size_mb} MB",
            f"  Records: {self.analysis.record_count:,}",
            f"  Nesting Depth: {self.analysis.max_depth}",
            f"  Structure: {'NESTED' if self.analysis.is_nested else 'FLAT'}",
            "",
        ]
        return "\n".join(lines)
    
    def generate_structure_tree(self) -> str:
        """Generate structure tree section."""
        lines = [
            "  DETECTED STRUCTURE",
            "  " + "-" * 40,
            "",
        ]
        
        tree = self.analyzer.print_structure_tree()
        # Indent tree
        for line in tree.split("\n"):
            lines.append(f"  {line}")
        
        lines.append("")
        return "\n".join(lines)
    
    def generate_mode_preview(self, mode: int) -> ModePreview:
        """Generate preview for a specific mode."""
        if mode == 1:
            return self._generate_flat_preview()
        elif mode == 2:
            return self._generate_explode_preview()
        elif mode == 3:
            return self._generate_relational_preview()
        else:
            raise ValueError(f"Unknown mode: {mode}")
    
    def _generate_flat_preview(self) -> ModePreview:
        """Generate preview for FLAT mode."""
        # Create sample flat output
        sample_df = self._create_flat_sample()
        
        return ModePreview(
            mode_id=1,
            name="FLAT",
            description="Single CSV - Arrays as JSON text",
            detail_description=(
                "All data in ONE file. Nested arrays (like projects, tasks) are stored as\n"
                "      JSON text in a single column. Simple objects are flattened with dot notation.\n"
                "      Example: department.name, department.manager.email\n"
                "      Arrays: [{\"projectId\": \"P001\", ...}] (as text)"
            ),
            output_files=1,
            output_file_names=["output.csv"],
            estimated_rows=self.analysis.record_count,
            sample_table=self._df_to_ascii(sample_df, max_cols=4),
            pros=["Single file, easy to manage", "Original data preserved", "1 row = 1 record"],
            cons=["Arrays need JSON parsing to use", "Cannot filter/sort by nested values"]
        )
    
    def _generate_explode_preview(self) -> ModePreview:
        """Generate preview for EXPLODE mode."""
        # Create sample exploded output
        sample_df = self._create_explode_sample()
        
        return ModePreview(
            mode_id=2,
            name="EXPLODE",
            description="Single CSV - One row per nested item",
            detail_description=(
                "All data in ONE file. Each nested item becomes a SEPARATE ROW.\n"
                "      Parent data (name, department) is DUPLICATED for each nested item.\n"
                "      Example: If employee has 3 projects with 2 tasks each = 6 rows\n"
                "      All columns are flat - easy to filter/sort/pivot in Excel"
            ),
            output_files=1,
            output_file_names=["output.csv"],
            estimated_rows=self.analysis.estimated_exploded_rows,
            sample_table=self._df_to_ascii(sample_df, max_cols=5),
            pros=["100% flat - works directly in Excel/SQL", "Can filter by ANY field", "Pivot table ready"],
            cons=["Data duplication (larger file)", f"~{self.analysis.estimated_exploded_rows:,} rows from {self.analysis.record_count:,} records"]
        )
    
    def _generate_relational_preview(self) -> ModePreview:
        """Generate preview for RELATIONAL mode."""
        # Create sample relational output
        tables = self._create_relational_sample()
        
        # Get file names with row counts
        file_names = [f"{name}.csv" for name in tables.keys()]
        
        # Build combined preview
        preview_parts = []
        for table_name, df in tables.items():
            preview_parts.append(f"  {table_name}.csv:")
            preview_parts.append(self._df_to_ascii(df, max_cols=4, indent=4))
            preview_parts.append("")
        
        # Create file list description
        file_info = ", ".join(file_names)
        
        return ModePreview(
            mode_id=3,
            name="RELATIONAL",
            description=f"Multiple CSVs - {len(tables)} linked files",
            detail_description=(
                f"Data split into {len(tables)} SEPARATE CSV files (like database tables).\n"
                f"      Files: {file_info}\n"
                "      Tables are linked by ID columns (main_id, projects_projectId, etc.)\n"
                "      NO duplication - clean normalized data, joinable in SQL/Excel"
            ),
            output_files=len(tables),
            output_file_names=file_names,
            estimated_rows=self.analysis.record_count,  # Main table rows
            sample_table="\n".join(preview_parts),
            pros=["No data duplication", "Clean normalized structure", "Database/SQL ready"],
            cons=[f"{len(tables)} files to manage", "Need VLOOKUP/JOIN for full view"]
        )
    
    def _create_flat_sample(self) -> pd.DataFrame:
        """Create sample DataFrame for flat mode."""
        if not self.sample_record:
            return pd.DataFrame()
        
        # Use json_normalize with array conversion
        record = self._convert_arrays_to_strings(self.sample_record.copy())
        df = pd.json_normalize([record], sep='.')
        return df.head(1)
    
    def _create_explode_sample(self) -> pd.DataFrame:
        """Create sample DataFrame for explode mode."""
        if not self.sample_record:
            return pd.DataFrame()
        
        # Find deepest array path and explode
        rows = self._explode_record(self.sample_record)
        if rows:
            df = pd.DataFrame(rows[:3])  # Show first 3 rows
            return df
        return pd.DataFrame()
    
    def _create_relational_sample(self) -> Dict[str, pd.DataFrame]:
        """Create sample DataFrames for relational mode."""
        if not self.sample_record:
            return {}
        
        tables = self._extract_relational_tables(self.sample_record)
        # Limit to 2 rows per table for preview
        return {name: df.head(2) for name, df in tables.items()}
    
    def _convert_arrays_to_strings(self, record: Dict) -> Dict:
        """Convert arrays to strings for flat mode preview."""
        result = {}
        for key, value in record.items():
            if isinstance(value, dict):
                nested = self._convert_arrays_to_strings(value)
                result[key] = nested
            elif isinstance(value, list):
                if value and isinstance(value[0], dict):
                    result[key] = json.dumps(value, ensure_ascii=False)[:50] + "..."
                else:
                    result[key] = "|".join(str(v) for v in value)
            else:
                result[key] = value
        return result
    
    def _explode_record(self, record: Dict, prefix: str = "") -> List[Dict]:
        """Recursively explode a record's arrays."""
        # Find ID field
        id_field = self._find_id_field(record)
        id_value = record.get(id_field, "")
        
        # Start with flat fields
        base_row = {}
        arrays_to_explode = []
        
        for key, value in record.items():
            if isinstance(value, dict):
                # Flatten nested dict
                for k, v in pd.json_normalize([value], sep='.').iloc[0].items():
                    if not isinstance(v, (list, dict)):
                        base_row[f"{key}.{k}"] = v
            elif isinstance(value, list) and value and isinstance(value[0], dict):
                # Array of objects - need to explode
                arrays_to_explode.append((key, value))
            elif isinstance(value, list):
                # Primitive array
                base_row[key] = "|".join(str(v) for v in value)
            else:
                base_row[key] = value
        
        if not arrays_to_explode:
            return [base_row]
        
        # Explode first array (simplification for preview)
        rows = []
        array_name, array_items = arrays_to_explode[0]
        
        for item in array_items:
            row = base_row.copy()
            # Add array item fields
            if isinstance(item, dict):
                for k, v in item.items():
                    if isinstance(v, list) and v and isinstance(v[0], dict):
                        # Nested array - explode further
                        for nested_item in v:
                            nested_row = row.copy()
                            for nk, nv in nested_item.items():
                                if isinstance(nv, list):
                                    nested_row[f"{k}.{nk}"] = "|".join(str(x) for x in nv)
                                elif not isinstance(nv, dict):
                                    nested_row[f"{k}.{nk}"] = nv
                            rows.append(nested_row)
                    elif isinstance(v, list):
                        row[k] = "|".join(str(x) for x in v)
                    elif isinstance(v, dict):
                        for dk, dv in v.items():
                            if not isinstance(dv, (dict, list)):
                                row[f"{k}.{dk}"] = dv
                    else:
                        row[k] = v
                if not any(isinstance(v, list) and v and isinstance(v[0], dict) for v in item.values()):
                    rows.append(row)
        
        return rows if rows else [base_row]
    
    def _extract_relational_tables(self, record: Dict) -> Dict[str, pd.DataFrame]:
        """Extract relational tables from a record - recursively finds all nested arrays."""
        tables = {}
        
        # Find root ID (could be at top level or inside wrapper like 'employee')
        root_id_field, root_id_value = self._find_root_id(record)
        
        # Main table - will hold flattened non-array fields
        main_row = {}
        
        # Recursively extract tables from nested structures
        self._extract_tables_recursive(
            record, 
            tables, 
            main_row, 
            parent_id_field=root_id_field,
            parent_id_value=root_id_value,
            prefix=""
        )
        
        # Insert main table first
        tables = {"main": pd.DataFrame([main_row]), **tables}
        return tables
    
    def _find_root_id(self, record: Dict) -> tuple:
        """Find the root ID field, even if nested in wrapper object."""
        # Check top level
        id_field = self._find_id_field(record)
        if id_field in record and not isinstance(record[id_field], dict):
            return id_field, record.get(id_field, "unknown")
        
        # Check inside wrapper objects (like 'employee', 'data', etc.)
        for key, value in record.items():
            if isinstance(value, dict):
                nested_id = self._find_id_field(value)
                if nested_id in value and not isinstance(value[nested_id], dict):
                    return nested_id, value.get(nested_id, "unknown")
        
        return "id", "unknown"
    
    def _extract_tables_recursive(self, obj: Dict, tables: Dict, main_row: Dict,
                                   parent_id_field: str, parent_id_value: Any,
                                   prefix: str, current_parent_ids: Dict = None):
        """Recursively extract relational tables from nested structure."""
        if current_parent_ids is None:
            current_parent_ids = {parent_id_field: parent_id_value}
        
        for key, value in obj.items():
            full_key = f"{prefix}.{key}" if prefix else key
            
            if isinstance(value, dict):
                # Recurse into nested object
                self._extract_tables_recursive(
                    value, tables, main_row,
                    parent_id_field, parent_id_value,
                    full_key, current_parent_ids
                )
                
            elif isinstance(value, list) and value and isinstance(value[0], dict):
                # Array of objects -> separate table
                table_name = key  # Use array field name as table name
                child_rows = []
                
                for item in value:
                    child_row = {}
                    # Add parent IDs
                    for pid_field, pid_value in current_parent_ids.items():
                        child_row[f"{table_name}_{pid_field}"] = pid_value
                    
                    # Find this item's ID
                    item_id_field = self._find_id_field(item)
                    item_id_value = item.get(item_id_field, "")
                    
                    # Process item fields
                    for k, v in item.items():
                        if isinstance(v, list) and v and isinstance(v[0], dict):
                            # Nested array -> another table (recurse)
                            nested_parent_ids = current_parent_ids.copy()
                            nested_parent_ids[item_id_field] = item_id_value
                            
                            nested_rows = []
                            for nested_item in v:
                                nested_row = {}
                                for pid_field, pid_value in nested_parent_ids.items():
                                    nested_row[f"{k}_{pid_field}"] = pid_value
                                
                                for nk, nv in nested_item.items():
                                    if isinstance(nv, list):
                                        if nv and isinstance(nv[0], dict):
                                            nested_row[nk] = json.dumps(nv)[:50] + "..."
                                        else:
                                            nested_row[nk] = "|".join(str(x) for x in nv)
                                    elif isinstance(nv, dict):
                                        for dk, dv in nv.items():
                                            if not isinstance(dv, (dict, list)):
                                                nested_row[f"{nk}.{dk}"] = dv
                                    else:
                                        nested_row[nk] = nv
                                nested_rows.append(nested_row)
                            
                            if k not in tables:
                                tables[k] = pd.DataFrame(nested_rows)
                            
                        elif isinstance(v, list):
                            child_row[k] = "|".join(str(x) for x in v)
                        elif isinstance(v, dict):
                            for dk, dv in v.items():
                                if not isinstance(dv, (dict, list)):
                                    child_row[f"{k}.{dk}"] = dv
                        else:
                            child_row[k] = v
                    
                    child_rows.append(child_row)
                
                if table_name not in tables:
                    tables[table_name] = pd.DataFrame(child_rows)
                    
            elif isinstance(value, list):
                # Primitive array
                main_row[full_key] = "|".join(str(v) for v in value)
            else:
                # Scalar value - add to main
                main_row[full_key] = value
    
    def _find_id_field(self, record: Dict) -> str:
        """Find the ID field in a record."""
        for key in record.keys():
            if key.lower() in ['id', 'employee_id', 'employeeid', 'userid', 'user_id', '_id']:
                return key
            if key.lower().endswith('id') and len(key) <= 15:
                return key
        return list(record.keys())[0] if record else "id"
    
    def _df_to_ascii(self, df: pd.DataFrame, max_cols: int = 4, max_width: int = 15, indent: int = 2) -> str:
        """Convert DataFrame to ASCII table representation."""
        if df.empty:
            return " " * indent + "(empty)"
        
        # Limit columns
        cols = list(df.columns)[:max_cols]
        if len(df.columns) > max_cols:
            cols_display = cols + ["..."]
        else:
            cols_display = cols
        
        # Truncate column names and values
        def truncate(val, width=max_width):
            s = str(val) if val is not None else ""
            return s[:width-2] + ".." if len(s) > width else s
        
        # Build header
        header = " | ".join(truncate(c) for c in cols_display)
        separator = "-+-".join("-" * min(len(truncate(c)), max_width) for c in cols_display)
        
        lines = [
            " " * indent + header,
            " " * indent + separator
        ]
        
        # Build rows
        for _, row in df.head(3).iterrows():
            values = [truncate(row[c]) if c in row else "..." for c in cols]
            if len(df.columns) > max_cols:
                values.append("...")
            lines.append(" " * indent + " | ".join(values))
        
        if len(df) > 3:
            lines.append(" " * indent + f"... ({len(df)} rows total)")
        
        return "\n".join(lines)
    
    def generate_mode_options(self) -> str:
        """Generate the full mode selection display."""
        if not self.analysis.has_array_of_objects:
            return self._generate_flat_only_message()
        
        # Get nested array names for context
        array_names = [arr.name for arr in self.analysis.nested_arrays]
        array_info = ", ".join(array_names[:3])
        if len(array_names) > 3:
            array_info += f" (+{len(array_names) - 3} more)"
        
        lines = [
            "",
            "  " + "#" * 60,
            "  NESTED ARRAYS DETECTED",
            "  " + "#" * 60,
            f"  Arrays found: {array_info}",
            "",
            "  How would you like to handle nested data?",
            "",
        ]
        
        for mode_id in [1, 2, 3]:
            preview = self.generate_mode_preview(mode_id)
            lines.extend(self._format_mode_option(preview))
            lines.append("")
        
        lines.extend([
            "  " + "=" * 60,
            "",
            "  Enter your choice [1/2/3] or 'q' to quit: ",
        ])
        
        return "\n".join(lines)
    
    def _generate_flat_only_message(self) -> str:
        """Message when JSON is already flat."""
        return """
  STRUCTURE: FLAT (no nested arrays detected)
  
  Your JSON will be converted directly to CSV.
  Press ENTER to continue or 'q' to quit: 
"""
    
    def _format_mode_option(self, preview: ModePreview) -> List[str]:
        """Format a single mode option for display."""
        lines = [
            "  " + "=" * 60,
            f"  [{preview.mode_id}] {preview.name} - {preview.description}",
            "  " + "-" * 60,
            f"      {preview.detail_description}",
            "",
        ]
        
        # Show output files for relational mode
        if preview.output_files > 1:
            lines.append(f"      OUTPUT FILES ({preview.output_files}):")
            for fname in preview.output_file_names:
                lines.append(f"        - {fname}")
            lines.append("")
        else:
            lines.append(f"      OUTPUT: {preview.output_file_names[0]} (~{preview.estimated_rows:,} rows)")
            lines.append("")
        
        lines.append("      SAMPLE:")
        lines.append(preview.sample_table)
        lines.append("")
        lines.append(f"      [+] {' | '.join(preview.pros)}")
        lines.append(f"      [-] {' | '.join(preview.cons)}")
        
        return lines
    
    def display_full_preview(self) -> str:
        """Generate complete interactive preview display."""
        sections = [
            self.generate_header(),
            self.generate_structure_tree(),
            self.generate_mode_options()
        ]
        return "\n".join(sections)
