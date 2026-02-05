"""
JSON to CSV Converter - Interactive Runner

Run this script and enter the path to your JSON file.
Works with any JSON structure - no configuration needed!
"""

import os
from pathlib import Path
from src.pipeline import JSONToCSVPipeline


def get_file_size_mb(file_path: str) -> float:
    """Get file size in MB"""
    return os.path.getsize(file_path) / (1024 * 1024)


def convert_json_to_csv(input_file: str, output_dir: str = "data/output"):
    """
    Convert any JSON file to CSV.
    
    Args:
        input_file: Path to your JSON file
        output_dir: Where to save the CSV file
    
    Returns:
        Path to the created CSV file
    """
    pipeline = JSONToCSVPipeline(input_file=input_file, output_dir=output_dir)
    return pipeline.run()


def main():
    """Interactive runner - asks user for JSON file path"""
    print("=" * 60)
    print("  JSON to CSV Converter")
    print("=" * 60)
    print()
    
    # Ask for file path
    while True:
        file_path = input("Enter the path to your JSON file: ").strip()
        
        # Remove quotes if user added them
        file_path = file_path.strip('"').strip("'")
        
        if not file_path:
            print("Error: Please enter a file path.\n")
            continue
        
        if not os.path.exists(file_path):
            print(f"Error: File not found - {file_path}")
            print("Please check the path and try again.\n")
            continue
        
        if not file_path.lower().endswith('.json'):
            print("Warning: File does not have .json extension.")
            confirm = input("Continue anyway? (y/n): ").strip().lower()
            if confirm != 'y':
                continue
        
        break
    
    # Show file info
    file_size = get_file_size_mb(file_path)
    print(f"\nFile: {file_path}")
    print(f"Size: {file_size:.2f} MB")
    
    if file_size > 500:
        print("\nNote: Large file detected. Using chunked processing...")
    
    print("\nProcessing...\n")
    
    # Run the conversion
    try:
        output_file = convert_json_to_csv(file_path)
        print(f"\n{'=' * 60}")
        print(f"  SUCCESS! CSV created:")
        print(f"  {output_file}")
        print(f"{'=' * 60}")
    except FileNotFoundError:
        print(f"\nError: File not found - {file_path}")
    except Exception as e:
        print(f"\nError: {e}")
        print("Check logs/pipeline.log for details.")


if __name__ == "__main__":
    main()
