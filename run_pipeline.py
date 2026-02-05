"""
JSON to CSV Converter - Interactive Runner

Run this script and enter the path to your JSON file.
Works with any JSON structure - no configuration needed!
Supports multiple conversion modes for nested JSON.
"""

import os
import sys
import argparse
from pathlib import Path
from src.pipeline import JSONToCSVPipeline


def get_file_size_mb(file_path: str) -> float:
    """Get file size in MB"""
    return os.path.getsize(file_path) / (1024 * 1024)


def convert_json_to_csv(input_file: str, output_dir: str = "data/output", mode: int = None):
    """
    Convert any JSON file to CSV.
    
    Args:
        input_file: Path to your JSON file
        output_dir: Where to save the CSV file
        mode: Conversion mode (1=flat, 2=explode, 3=relational), None=interactive
    
    Returns:
        List of paths to created CSV files
    """
    pipeline = JSONToCSVPipeline(input_file=input_file, output_dir=output_dir)
    
    if mode:
        return pipeline.run_with_mode(mode)
    else:
        return pipeline.run_interactive()


def run_conversion(file_path: str, mode: int = None):
    """Run conversion on a given file path"""
    # Show file info
    file_size = get_file_size_mb(file_path)
    print(f"\nFile: {file_path}")
    print(f"Size: {file_size:.2f} MB")
    
    if file_size > 500:
        print("\nNote: Large file detected. Using chunked processing...")
    
    # Run the conversion
    try:
        output_files = convert_json_to_csv(file_path, mode=mode)
        if output_files:
            print(f"\n{'=' * 65}")
            print(f"  SUCCESS! CSV file(s) created:")
            for f in output_files:
                print(f"  - {f}")
            print(f"{'=' * 65}")
    except FileNotFoundError:
        print(f"\nError: File not found - {file_path}")
    except Exception as e:
        print(f"\nError: {e}")
        print("Check logs/pipeline.log for details.")


def interactive_file_selection(mode: int = None):
    """Interactive file selection when no file is provided"""
    print("=" * 65)
    print("  JSON to CSV Converter")
    print("=" * 65)
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
    
    run_conversion(file_path, mode)


def main():
    """Main entry point with argument parsing"""
    parser = argparse.ArgumentParser(
        description='Convert JSON to CSV with interactive mode selection',
        formatter_class=argparse.RawDescriptionHelpFormatter,
        epilog='''
Modes:
  1 = FLAT       - Arrays stored as JSON strings
  2 = EXPLODE    - One row per nested item
  3 = RELATIONAL - Separate linked CSVs

Examples:
  python run_pipeline.py                          # Interactive file selection
  python run_pipeline.py data.json                # Interactive mode selection
  python run_pipeline.py data.json --mode 3       # Direct relational mode
        '''
    )
    parser.add_argument(
        'input_file',
        type=str,
        nargs='?',
        help='Path to JSON file (optional, will prompt if not provided)'
    )
    parser.add_argument(
        '-m', '--mode',
        type=int,
        choices=[1, 2, 3],
        help='Conversion mode: 1=flat, 2=explode, 3=relational'
    )
    
    args = parser.parse_args()
    
    if args.input_file:
        # File provided via command line
        run_conversion(args.input_file, args.mode)
    else:
        # Interactive file selection
        interactive_file_selection(args.mode)


if __name__ == "__main__":
    main()
