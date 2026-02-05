"""
Data Loading Module
Handles writing data to CSV files with error handling and options.
"""

import pandas as pd
from pathlib import Path
from datetime import datetime
from typing import Optional, Union
import logging
from tqdm import tqdm
import time

logger = logging.getLogger(__name__)


class DataLoader:
    """
    Loads transformed data to CSV files with comprehensive options and error handling.
    """

    def __init__(self, output_dir: Union[str, Path]):
        """
        Initialize the DataLoader.
        
        Args:
            output_dir: Directory where CSV files will be saved
        """
        self.output_dir = Path(output_dir)
        self.output_file: Optional[Path] = None
        logger.info(f"DataLoader initialized with output directory: {self.output_dir}")
        
        # Create output directory if it doesn't exist
        self.output_dir.mkdir(parents=True, exist_ok=True)
        logger.debug(f"Output directory ensured: {self.output_dir}")

    def generate_output_filename(
        self,
        base_name: str = "output",
        add_timestamp: bool = True,
        extension: str = ".csv"
    ) -> str:
        """
        Generate output filename with optional timestamp.
        
        Args:
            base_name: Base name for the output file
            add_timestamp: Whether to add timestamp to filename
            extension: File extension (default: .csv)
        
        Returns:
            Generated filename
        """
        if add_timestamp:
            timestamp = datetime.now().strftime("%Y%m%d_%H%M%S")
            filename = f"{base_name}_{timestamp}{extension}"
        else:
            filename = f"{base_name}{extension}"
        
        logger.debug(f"Generated output filename: {filename}")
        return filename

    def load_to_csv(
        self,
        df: pd.DataFrame,
        filename: Optional[str] = None,
        add_timestamp: bool = True,
        encoding: str = "utf-8",
        index: bool = False,
        **csv_kwargs
    ) -> Path:
        """
        Write DataFrame to CSV file.
        
        Args:
            df: DataFrame to write
            filename: Output filename (optional, auto-generated if None)
            add_timestamp: Whether to add timestamp to filename
            encoding: File encoding (default: utf-8)
            index: Whether to write row indices
            **csv_kwargs: Additional arguments for pandas to_csv()
        
        Returns:
            Path to the created CSV file
        
        Raises:
            ValueError: If DataFrame is empty
            IOError: If file writing fails
        """
        logger.info("Starting CSV file creation")
        
        # Validate DataFrame
        if df is None or df.empty:
            error_msg = "Cannot write empty DataFrame to CSV"
            logger.error(error_msg)
            raise ValueError(error_msg)
        
        logger.info(f"DataFrame to write: {df.shape[0]} rows, {df.shape[1]} columns")
        
        try:
            # Generate filename if not provided
            if filename is None:
                filename = self.generate_output_filename(
                    base_name="output",
                    add_timestamp=add_timestamp
                )
            
            # Construct full output path
            self.output_file = self.output_dir / filename
            
            # Write to CSV with progress for large datasets
            logger.info(f"Writing DataFrame to: {self.output_file}")
            total_rows = len(df)
            
            if total_rows > 50000:
                # Chunked writing with progress bar for large files
                chunk_size = 10000
                print(f"    ├─ Writing {total_rows:,} rows to CSV")
                
                # Write header first
                df.head(0).to_csv(self.output_file, encoding=encoding, index=index, **csv_kwargs)
                
                # Write data in chunks with progress
                with tqdm(total=total_rows, desc="        Writing", 
                          unit="rows", bar_format="{l_bar}{bar}| {n_fmt}/{total_fmt} [{elapsed}<{remaining}]") as pbar:
                    for start in range(0, total_rows, chunk_size):
                        end = min(start + chunk_size, total_rows)
                        df.iloc[start:end].to_csv(
                            self.output_file,
                            mode='a',
                            header=False,
                            encoding=encoding,
                            index=index,
                            **csv_kwargs
                        )
                        pbar.update(end - start)
                print("        ✓ Write complete")
            else:
                # Standard write for smaller files
                print(f"    ├─ Writing {total_rows:,} rows...", end=" ", flush=True)
                df.to_csv(
                    self.output_file,
                    encoding=encoding,
                    index=index,
                    **csv_kwargs
                )
                print("✓")
            
            # Verify file was created
            if not self.output_file.exists():
                raise IOError("CSV file was not created successfully")
            
            file_size = self.output_file.stat().st_size
            logger.info(f"CSV file created successfully: {self.output_file}")
            logger.info(f"File size: {file_size} bytes ({file_size / 1024:.2f} KB)")
            
            return self.output_file
            
        except Exception as e:
            error_msg = f"Error writing CSV file: {str(e)}"
            logger.error(error_msg)
            raise IOError(error_msg)

    def load_to_multiple_formats(
        self,
        df: pd.DataFrame,
        base_filename: str = "output",
        formats: Optional[list] = None
    ) -> dict:
        """
        Export DataFrame to multiple formats (CSV, JSON, Excel).
        
        Args:
            df: DataFrame to export
            base_filename: Base name for output files
            formats: List of formats to export ('csv', 'json', 'excel')
        
        Returns:
            Dictionary with format: filepath mappings
        """
        if formats is None:
            formats = ['csv']
        
        logger.info(f"Exporting DataFrame to formats: {formats}")
        output_files = {}
        
        timestamp = datetime.now().strftime("%Y%m%d_%H%M%S")
        
        for fmt in formats:
            try:
                if fmt.lower() == 'csv':
                    filepath = self.load_to_csv(
                        df,
                        filename=f"{base_filename}_{timestamp}.csv"
                    )
                    output_files['csv'] = filepath
                    
                elif fmt.lower() == 'json':
                    filepath = self.output_dir / f"{base_filename}_{timestamp}.json"
                    df.to_json(filepath, orient='records', indent=2)
                    output_files['json'] = filepath
                    logger.info(f"JSON file created: {filepath}")
                    
                elif fmt.lower() == 'excel':
                    filepath = self.output_dir / f"{base_filename}_{timestamp}.xlsx"
                    df.to_excel(filepath, index=False, engine='openpyxl')
                    output_files['excel'] = filepath
                    logger.info(f"Excel file created: {filepath}")
                    
            except Exception as e:
                logger.error(f"Error exporting to {fmt}: {str(e)}")
        
        return output_files

    def get_output_info(self) -> dict:
        """
        Get information about the last created output file.
        
        Returns:
            Dictionary with output file information
        """
        if self.output_file is None or not self.output_file.exists():
            return {"status": "No output file created yet"}
        
        info = {
            "file_path": str(self.output_file),
            "file_name": self.output_file.name,
            "file_size_bytes": self.output_file.stat().st_size,
            "file_size_kb": round(self.output_file.stat().st_size / 1024, 2),
            "created_at": datetime.fromtimestamp(
                self.output_file.stat().st_mtime
            ).strftime("%Y-%m-%d %H:%M:%S")
        }
        
        logger.debug(f"Output file info: {info}")
        return info
