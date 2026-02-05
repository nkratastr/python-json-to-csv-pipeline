"""
Main Pipeline Module
Converts ANY JSON file to CSV with interactive mode selection.
Supports multiple conversion modes: flat, explode, relational.
"""

import sys
import time
import json
from pathlib import Path
from typing import Optional, Dict, List
import logging

# Add project root to path
project_root = Path(__file__).parent.parent
sys.path.insert(0, str(project_root))

from src.extractor import DataExtractor
from src.transformer import DataTransformer
from src.loader import DataLoader
from src.logger_config import get_logger_from_config, load_config
from src.analyzer import JSONAnalyzer
from src.preview import PreviewGenerator
from src.modes import FlatConverter, ExplodeConverter, RelationalConverter

# Try to import tqdm for progress bar
try:
    from tqdm import tqdm
    TQDM_AVAILABLE = True
except ImportError:
    TQDM_AVAILABLE = False


def format_duration(seconds: float) -> str:
    """Format duration in human-readable format."""
    if seconds < 60:
        return f"{seconds:.2f}s"
    elif seconds < 3600:
        minutes = int(seconds // 60)
        secs = seconds % 60
        return f"{minutes}m {secs:.1f}s"
    else:
        hours = int(seconds // 3600)
        minutes = int((seconds % 3600) // 60)
        return f"{hours}h {minutes}m"


class JSONToCSVPipeline:
    """
    Main pipeline class - converts ANY JSON file to CSV.
    
    Features:
    - Interactive mode selection for nested JSON
    - Three conversion modes: flat, explode, relational
    - Auto-detects JSON structure
    - Progress bar and timing
    
    Modes:
        1. FLAT: Arrays become JSON strings (simple, no data loss)
        2. EXPLODE: Full denormalization (one row per nested item)
        3. RELATIONAL: Separate linked CSVs (normalized, no duplication)
    
    Usage:
        pipeline = JSONToCSVPipeline("data/input/any_file.json")
        output_file = pipeline.run_interactive()  # With mode selection
        output_file = pipeline.run(mode=2)  # Direct mode
    """

    def __init__(
        self,
        input_file: str,
        output_dir: Optional[str] = None,
        config_path: str = "config/config.yaml"
    ):
        """
        Initialize the pipeline.
        
        Args:
            input_file: Path to input JSON file (any structure)
            output_dir: Directory for output CSV (optional)
            config_path: Path to configuration file (optional)
        """
        # Load configuration
        try:
            self.config = load_config(config_path)
        except Exception:
            self.config = self._get_default_config()

        # Setup logger
        try:
            self.logger = get_logger_from_config(config_path)
        except Exception:
            logging.basicConfig(level=logging.INFO)
            self.logger = logging.getLogger(__name__)

        # Initialize components
        self.input_file = input_file
        self.output_dir = output_dir or self.config.get('paths', {}).get('output_dir', 'data/output')

        self.extractor = DataExtractor(input_file)
        self.transformer = DataTransformer()
        self.loader = DataLoader(self.output_dir)
        
        # Timing stats
        self.timings: Dict[str, float] = {}
        
        # Mode converters
        self.converters = {
            1: FlatConverter(),
            2: ExplodeConverter(),
            3: RelationalConverter()
        }

    def _get_default_config(self) -> Dict:
        """Default configuration if config file is not available."""
        return {
            'paths': {'output_dir': 'data/output', 'log_dir': 'logs'},
            'output': {'csv_encoding': 'utf-8', 'csv_index': False, 'timestamp_suffix': True}
        }

    def _print_step(self, step: int, total: int, description: str, status: str = "running"):
        """Print step status with visual indicator."""
        icons = {"running": "⏳", "done": "✓", "error": "✗"}
        icon = icons.get(status, "•")
        print(f"\n[{step}/{total}] {icon} {description}")

    def run_interactive(self, output_filename: Optional[str] = None) -> List[Path]:
        """
        Run pipeline with interactive mode selection.
        
        Shows JSON structure analysis and lets user choose conversion mode.
        
        Args:
            output_filename: Custom output filename (optional)
            
        Returns:
            List of paths to created CSV files
        """
        # Step 1: Analyze JSON structure
        print("\n" + "=" * 65)
        print("  Analyzing JSON structure...")
        print("=" * 65)
        
        analyzer = JSONAnalyzer(self.input_file)
        analysis = analyzer.analyze()
        
        # Step 2: Generate and display preview
        preview = PreviewGenerator(analyzer)
        print(preview.display_full_preview())
        
        # Step 3: Get user mode selection
        if not analysis.has_array_of_objects:
            # No nested arrays - use flat mode
            input("\n  Press ENTER to continue with FLAT mode...")
            mode = 1
        else:
            # Let user choose
            while True:
                choice = input().strip().lower()
                if choice == 'q':
                    print("\n  Cancelled by user.")
                    return []
                if choice in ['1', '2', '3']:
                    mode = int(choice)
                    break
                print("  Invalid choice. Enter 1, 2, 3, or 'q': ", end="")
        
        # Step 4: Run conversion with selected mode
        return self.run_with_mode(mode, output_filename)
    
    def run_with_mode(self, mode: int, output_filename: Optional[str] = None) -> List[Path]:
        """
        Run pipeline with specified conversion mode.
        
        Args:
            mode: Conversion mode (1=flat, 2=explode, 3=relational)
            output_filename: Custom output filename (optional)
            
        Returns:
            List of paths to created CSV files
        """
        total_start = time.time()
        mode_names = {1: "FLAT", 2: "EXPLODE", 3: "RELATIONAL"}
        
        print("\n" + "=" * 65)
        print(f"  Converting with {mode_names.get(mode, 'UNKNOWN')} mode")
        print("=" * 65)
        
        try:
            self.logger.info(f"Pipeline started with mode {mode}")
            
            # ===== STEP 1: EXTRACT RAW DATA =====
            self._print_step(1, 4, "Extracting raw JSON data...")
            step_start = time.time()
            
            data = self.extractor.extract_from_json()
            
            step_duration = time.time() - step_start
            self.timings['extract'] = step_duration
            
            print(f"    Records: {len(data):,}")
            print(f"    Duration: {format_duration(step_duration)}")
            
            # ===== STEP 2: CONVERT =====
            self._print_step(2, 4, f"Converting data ({mode_names[mode]} mode)...")
            step_start = time.time()
            
            converter = self.converters[mode]
            tables = converter.convert(data)
            
            step_duration = time.time() - step_start
            self.timings['convert'] = step_duration
            
            print(f"    Tables: {len(tables)}")
            for name, df in tables.items():
                print(f"      - {name}: {len(df):,} rows, {len(df.columns)} columns")
            print(f"    Duration: {format_duration(step_duration)}")
            
            # ===== STEP 3: TRANSFORM (dedupe) =====
            self._print_step(3, 4, "Transforming data...")
            step_start = time.time()
            
            transformed_tables = {}
            for name, df in tables.items():
                transformed = self.transformer.transform_dataframe(df, drop_duplicates=True)
                transformed_tables[name] = transformed
            
            step_duration = time.time() - step_start
            self.timings['transform'] = step_duration
            print(f"    Duration: {format_duration(step_duration)}")
            
            # ===== STEP 4: WRITE CSVs =====
            self._print_step(4, 4, "Writing CSV files...")
            step_start = time.time()
            
            output_config = self.config.get('output', {})
            output_paths = []
            
            for name, df in transformed_tables.items():
                # Generate filename
                if len(transformed_tables) == 1:
                    fname = output_filename
                else:
                    base = output_filename.replace('.csv', '') if output_filename else name
                    fname = f"{base}_{name}.csv" if output_filename else f"{name}.csv"
                
                output_path = self.loader.load_to_csv(
                    df,
                    filename=fname,
                    add_timestamp=output_config.get('timestamp_suffix', True) if not output_filename else False,
                    encoding=output_config.get('csv_encoding', 'utf-8'),
                    index=output_config.get('csv_index', False)
                )
                output_paths.append(output_path)
                print(f"    Created: {output_path.name} ({len(df):,} rows)")
            
            step_duration = time.time() - step_start
            self.timings['load'] = step_duration
            print(f"    Duration: {format_duration(step_duration)}")
            
            # ===== SUMMARY =====
            total_duration = time.time() - total_start
            self._print_multi_summary(tables, transformed_tables, output_paths, total_duration)
            
            self.logger.info(f"Pipeline completed in {format_duration(total_duration)}")
            return output_paths
            
        except Exception as e:
            self.logger.error(f"Pipeline failed: {str(e)}")
            print(f"\n[ERROR] Pipeline failed: {str(e)}")
            raise
    
    def _print_multi_summary(self, original_tables, final_tables, output_paths, total_duration):
        """Print summary for multi-table output."""
        print("\n" + "=" * 65)
        print("  COMPLETED SUCCESSFULLY")
        print("=" * 65)
        
        total_input = sum(len(df) for df in original_tables.values())
        total_output = sum(len(df) for df in final_tables.values())
        
        print(f"""
  Input Records:  {total_input:,}
  Output Records: {total_output:,}
  Tables Created: {len(output_paths)}
  
  Output Files:""")
        for path in output_paths:
            size_kb = path.stat().st_size / 1024
            print(f"    - {path.name} ({size_kb:.2f} KB)")
        
        print(f"""
  Timing:
    - Extract:   {format_duration(self.timings.get('extract', 0))}
    - Convert:   {format_duration(self.timings.get('convert', 0))}
    - Transform: {format_duration(self.timings.get('transform', 0))}
    - Load:      {format_duration(self.timings.get('load', 0))}
    - TOTAL:     {format_duration(total_duration)}
""")
        print("=" * 65)

    def run(self, output_filename: Optional[str] = None, mode: int = 1) -> Path:
        """
        Run the pipeline - converts JSON to CSV automatically.
        Shows progress and timing for each step.
        
        Args:
            output_filename: Custom output filename (optional)
        
        Returns:
            Path to the created CSV file
        """
        total_start = time.time()
        
        print("\n" + "=" * 60)
        print("  JSON to CSV Pipeline")
        print("=" * 60)
        print(f"  Input:  {self.input_file}")
        print(f"  Output: {self.output_dir}")
        print("=" * 60)
        
        try:
            self.logger.info("Pipeline execution started")
            
            # ===== STEP 1: EXTRACT =====
            self._print_step(1, 3, "Extracting data from JSON...")
            step_start = time.time()
            
            df = self.extractor.extract_to_dataframe()
            
            step_duration = time.time() - step_start
            self.timings['extract'] = step_duration
            
            if df.empty:
                raise ValueError("No data found in JSON file")
            
            print(f"    Records: {len(df):,} | Columns: {len(df.columns)}")
            print(f"    Duration: {format_duration(step_duration)}")
            self.logger.info(f"Extracted: {len(df)} records in {format_duration(step_duration)}")
            
            # ===== STEP 2: TRANSFORM =====
            self._print_step(2, 3, "Transforming data...")
            step_start = time.time()
            
            transformed_df = self.transformer.transform_dataframe(df, drop_duplicates=True)
            
            step_duration = time.time() - step_start
            self.timings['transform'] = step_duration
            
            if transformed_df.empty:
                raise ValueError("No data after transformation")
            
            removed = len(df) - len(transformed_df)
            print(f"    Records: {len(transformed_df):,} | Removed: {removed:,}")
            print(f"    Duration: {format_duration(step_duration)}")
            self.logger.info(f"Transformed: {len(transformed_df)} records in {format_duration(step_duration)}")
            
            # ===== STEP 3: LOAD =====
            self._print_step(3, 3, "Writing to CSV...")
            step_start = time.time()
            
            output_config = self.config.get('output', {})
            output_path = self.loader.load_to_csv(
                transformed_df,
                filename=output_filename,
                add_timestamp=output_config.get('timestamp_suffix', True),
                encoding=output_config.get('csv_encoding', 'utf-8'),
                index=output_config.get('csv_index', False)
            )
            
            step_duration = time.time() - step_start
            self.timings['load'] = step_duration
            
            file_size = output_path.stat().st_size / 1024
            print(f"    File: {output_path.name}")
            print(f"    Size: {file_size:.2f} KB")
            print(f"    Duration: {format_duration(step_duration)}")
            self.logger.info(f"Loaded: {output_path} in {format_duration(step_duration)}")
            
            # ===== SUMMARY =====
            total_duration = time.time() - total_start
            self.timings['total'] = total_duration
            
            self._print_summary(df, transformed_df, output_path, total_duration)
            
            self.logger.info(f"Pipeline completed in {format_duration(total_duration)}")
            
            return output_path
            
        except Exception as e:
            self.logger.error(f"Pipeline failed: {str(e)}")
            print(f"\n[ERROR] Pipeline failed: {str(e)}")
            raise

    def _print_summary(self, original_df, final_df, output_path, total_duration: float = 0):
        """Print pipeline execution summary with timing."""
        print("\n" + "=" * 60)
        print("  COMPLETED SUCCESSFULLY")
        print("=" * 60)
        print(f"""
  Input Records:    {len(original_df):,}
  Output Records:   {len(final_df):,}
  Records Filtered: {len(original_df) - len(final_df):,}
  Columns:          {len(final_df.columns)}
  
  Output File: {output_path}
  File Size:   {output_path.stat().st_size / 1024:.2f} KB
  
  Timing:
    - Extract:   {format_duration(self.timings.get('extract', 0))}
    - Transform: {format_duration(self.timings.get('transform', 0))}
    - Load:      {format_duration(self.timings.get('load', 0))}
    - TOTAL:     {format_duration(total_duration)}
""")
        print("=" * 60)


def main():
    """
    Command line entry point.
    
    Usage:
        python -m src.pipeline data/input/file.json                # Interactive mode
        python -m src.pipeline data/input/file.json --mode 1       # Flat mode
        python -m src.pipeline data/input/file.json --mode 2       # Explode mode
        python -m src.pipeline data/input/file.json --mode 3       # Relational mode
        python -m src.pipeline data/input/file.json -o data/output
        python -m src.pipeline data/input/file.json -f custom_name.csv
    """
    import argparse
    
    parser = argparse.ArgumentParser(
        description='Convert any JSON file to CSV with smart nested data handling',
        epilog='''
Modes:
  1 = FLAT       - Arrays stored as JSON strings (simple, no data loss)
  2 = EXPLODE    - One row per nested item (denormalized, may duplicate)
  3 = RELATIONAL - Separate linked CSVs (normalized, database ready)

Example: 
  python -m src.pipeline data/input/employees.json --mode 3
        '''
    )
    parser.add_argument(
        'input_file',
        type=str,
        help='Path to input JSON file (any structure)'
    )
    parser.add_argument(
        '-m', '--mode',
        type=int,
        choices=[1, 2, 3],
        help='Conversion mode: 1=flat, 2=explode, 3=relational (default: interactive)',
        default=None
    )
    parser.add_argument(
        '-o', '--output',
        type=str,
        help='Output directory',
        default=None
    )
    parser.add_argument(
        '-f', '--filename',
        type=str,
        help='Output filename (default: auto-generated with timestamp)',
        default=None
    )
    parser.add_argument(
        '-c', '--config',
        type=str,
        help='Path to config file',
        default='config/config.yaml'
    )
    
    args = parser.parse_args()
    
    # Run pipeline
    pipeline = JSONToCSVPipeline(
        input_file=args.input_file,
        output_dir=args.output,
        config_path=args.config
    )
    
    if args.mode:
        # Direct mode specified
        pipeline.run_with_mode(args.mode, output_filename=args.filename)
    else:
        # Interactive mode
        pipeline.run_interactive(output_filename=args.filename)


if __name__ == "__main__":
    main()
