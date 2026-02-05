"""
JSON to CSV Converter - Simple Runner

Just run this script and change the input_file path to convert any JSON to CSV.
No configuration needed!
"""

from src.pipeline import JSONToCSVPipeline


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


if __name__ == "__main__":
    # =========================================
    # CHANGE THIS TO YOUR JSON FILE PATH
    # =========================================
    INPUT_FILE = "data/input/sample.json"
    
    # Run the conversion
    try:
        output_file = convert_json_to_csv(INPUT_FILE)
        print(f"\n✓ Success! CSV created: {output_file}")
    except FileNotFoundError:
        print(f"\n✗ Error: File not found - {INPUT_FILE}")
        print("  Please check the file path and try again.")
    except Exception as e:
        print(f"\n✗ Error: {e}")
