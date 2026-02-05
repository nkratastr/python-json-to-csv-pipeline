"""
Data Transformation Module
Handles data cleaning and transformation - works with ANY JSON structure.
Uses Polars for fast deduplication (10-100x faster than Pandas).
"""

import pandas as pd
import polars as pl
from typing import List, Dict, Optional
import logging
from tqdm import tqdm
import time
import sys

logger = logging.getLogger(__name__)


class DataTransformer:
    """
    Transforms data before loading to CSV.
    Works automatically with any JSON structure - no schema required.
    """

    def __init__(self):
        """Initialize the DataTransformer."""
        self.transformed_data: Optional[pd.DataFrame] = None
        logger.info("DataTransformer initialized")

    def transform_dataframe(
        self,
        df: pd.DataFrame,
        drop_duplicates: bool = True,
        drop_na_columns: Optional[List[str]] = None,
        fillna_value: Optional[Dict] = None
    ) -> pd.DataFrame:
        """
        Apply transformations to the DataFrame.
        Works with any DataFrame structure automatically.
        
        Args:
            df: Input DataFrame (any structure)
            drop_duplicates: Whether to drop duplicate rows
            drop_na_columns: Columns to check for NaN values and drop those rows
            fillna_value: Dictionary of {column: value} to fill NaN values
        
        Returns:
            Transformed DataFrame
        """
        logger.info(f"Starting DataFrame transformation. Initial shape: {df.shape}")
        
        if df.empty:
            logger.warning("Empty DataFrame provided for transformation")
            return df
        
        transformed_df = df.copy()
        
        try:
            # Drop duplicates if requested - using Polars for speed
            if drop_duplicates:
                initial_count = len(transformed_df)
                print(f"    ├─ Removing duplicates ({initial_count:,} rows)...", end=" ", flush=True)
                
                start_time = time.time()
                
                # Use Polars for fast deduplication (10-100x faster than Pandas)
                pl_df = pl.from_pandas(transformed_df)
                pl_unique = pl_df.unique()
                transformed_df = pl_unique.to_pandas()
                
                elapsed = time.time() - start_time
                duplicates_removed = initial_count - len(transformed_df)
                print(f"✓ (removed {duplicates_removed:,} in {elapsed:.2f}s)")
                    
                if duplicates_removed > 0:
                    logger.info(f"Removed {duplicates_removed} duplicate rows using Polars in {elapsed:.2f}s")
            else:
                print("    ├─ Removing duplicates... ✓ (skipped)")
            
            # Drop rows with NaN in specified columns
            if drop_na_columns:
                print("    ├─ Dropping NA rows...", end=" ", flush=True)
                for col in drop_na_columns:
                    if col in transformed_df.columns:
                        initial_count = len(transformed_df)
                        transformed_df = transformed_df.dropna(subset=[col])
                        rows_dropped = initial_count - len(transformed_df)
                        if rows_dropped > 0:
                            logger.info(f"Removed {rows_dropped} rows with NaN in column '{col}'")
                print("✓")
            
            # Fill NaN values
            if fillna_value:
                print("    ├─ Filling NA values...", end=" ", flush=True)
                for col, value in fillna_value.items():
                    if col in transformed_df.columns:
                        transformed_df[col].fillna(value, inplace=True)
                        logger.debug(f"Filled NaN values in column '{col}' with '{value}'")
                print("✓")
            
            # Clean string columns (strip whitespace)
            string_columns = transformed_df.select_dtypes(include=['object']).columns
            if len(string_columns) > 0:
                print(f"    └─ Cleaning {len(string_columns)} text columns...", end=" ", flush=True)
                for col in string_columns:
                    transformed_df[col] = transformed_df[col].apply(
                        lambda x: x.strip() if isinstance(x, str) else x
                    )
                print("✓")
            else:
                print("    └─ No text columns to clean ✓")
            
            logger.info(f"Transformation complete. Final shape: {transformed_df.shape}")
            self.transformed_data = transformed_df
            
            return transformed_df
            
        except Exception as e:
            logger.error(f"Error during DataFrame transformation: {str(e)}")
            raise

    def get_transformation_summary(self) -> Dict:
        """
        Get summary of transformation results.
        
        Returns:
            Dictionary with transformation statistics
        """
        if self.transformed_data is None:
            return {"status": "No transformation performed yet"}
        
        summary = {
            "rows": len(self.transformed_data),
            "columns": len(self.transformed_data.columns),
            "column_names": self.transformed_data.columns.tolist(),
            "memory_usage": f"{self.transformed_data.memory_usage(deep=True).sum() / 1024:.2f} KB"
        }
        
        logger.debug(f"Transformation summary: {summary}")
        return summary
