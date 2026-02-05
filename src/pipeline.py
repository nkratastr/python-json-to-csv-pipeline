"""
Main Pipeline Module
Converts ANY JSON file to CSV automatically - no configuration needed.
"""

import sys
from pathlib import Path
from typing import Optional, Dict
import logging

# Add project root to path
project_root = Path(__file__).parent.parent
sys.path.insert(0, str(project_root))

from src.extractor import DataExtractor
from src.transformer import DataTransformer
from src.loader import DataLoader
from src.logger_config import get_logger_from_config, load_config


class JSONToCSVPipeline:
    """
    Main pipeline class - converts ANY JSON file to CSV.
    
    Just provide the JSON file path, and it handles everything:
    - Auto-detects JSON structure (array, nested object, single record)
    - Extracts data to DataFrame
    - Cleans and transforms data
    - Outputs to CSV
    
    Usage:
        pipeline = JSONToCSVPipeline("data/input/any_file.json")
        output_file = pipeline.run()
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

        self.logger.info("=" * 70)
        self.logger.info("JSON to CSV Pipeline - Starting")
        self.logger.info("=" * 70)

        # Initialize components
        self.input_file = input_file
        self.output_dir = output_dir or self.config.get('paths', {}).get('output_dir', 'data/output')

        self.extractor = DataExtractor(input_file)
        self.transformer = DataTransformer()
        self.loader = DataLoader(self.output_dir)

        self.logger.info(f"Input:  {input_file}")
        self.logger.info(f"Output: {self.output_dir}")

    def _get_default_config(self) -> Dict:
        """Default configuration if config file is not available."""
        return {
            'paths': {'output_dir': 'data/output', 'log_dir': 'logs'},
            'output': {'csv_encoding': 'utf-8', 'csv_index': False, 'timestamp_suffix': True}
        }

    def run(self, output_filename: Optional[str] = None) -> Path:
        """
        Run the pipeline - converts JSON to CSV automatically.
        
        Args:
            output_filename: Custom output filename (optional)
        
        Returns:
            Path to the created CSV file
        """
        try:
            self.logger.info("Pipeline execution started")
            
            # EXTRACT - Read JSON file (any structure)
            self.logger.info("Step 1/3: Extracting data from JSON")
            df = self.extractor.extract_to_dataframe()
            
            if df.empty:
                raise ValueError("No data found in JSON file")
            
            self.logger.info(f"Extracted: {len(df)} records, {len(df.columns)} columns")
            self.logger.info(f"Columns: {', '.join(df.columns.tolist())}")
            
            # TRANSFORM - Clean data
            self.logger.info("Step 2/3: Transforming data")
            transformed_df = self.transformer.transform_dataframe(df, drop_duplicates=True)
            
            if transformed_df.empty:
                raise ValueError("No data after transformation")
            
            self.logger.info(f"Transformed: {len(transformed_df)} records")
            
            # LOAD - Write to CSV
            self.logger.info("Step 3/3: Writing to CSV")
            
            output_config = self.config.get('output', {})
            output_path = self.loader.load_to_csv(
                transformed_df,
                filename=output_filename,
                add_timestamp=output_config.get('timestamp_suffix', True),
                encoding=output_config.get('csv_encoding', 'utf-8'),
                index=output_config.get('csv_index', False)
            )
            
            # Summary
            self._print_summary(df, transformed_df, output_path)
            
            self.logger.info("=" * 70)
            self.logger.info("Pipeline completed successfully!")
            self.logger.info("=" * 70)
            
            return output_path
            
        except Exception as e:
            self.logger.error(f"Pipeline failed: {str(e)}")
            raise

    def _print_summary(self, original_df, final_df, output_path):
        """Print pipeline execution summary."""
        summary = f"""
Pipeline Execution Summary:
---------------------------
Input Records:      {len(original_df)}
Output Records:     {len(final_df)}
Records Filtered:   {len(original_df) - len(final_df)}
Columns:            {len(final_df.columns)}
Column Names:       {', '.join(final_df.columns.tolist())}
Output File:        {output_path}
File Size:          {output_path.stat().st_size / 1024:.2f} KB
"""
        self.logger.info(summary)
        print(summary)


def main():
    """
    Command line entry point.
    
    Usage:
        python -m src.pipeline data/input/file.json
        python -m src.pipeline data/input/file.json -o data/output
        python -m src.pipeline data/input/file.json -f custom_name.csv
    """
    import argparse
    
    parser = argparse.ArgumentParser(
        description='Convert any JSON file to CSV',
        epilog='Example: python -m src.pipeline data/input/sample.json'
    )
    parser.add_argument(
        'input_file',
        type=str,
        help='Path to input JSON file (any structure)'
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
    
    pipeline.run(output_filename=args.filename)


if __name__ == "__main__":
    main()
