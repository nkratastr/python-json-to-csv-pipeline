"""
Examples - How to use the JSON to CSV Pipeline

This pipeline converts ANY JSON file to CSV automatically.
Just provide the file path - no configuration needed!
"""

from pathlib import Path
from src.pipeline import JSONToCSVPipeline


# ============================================================================
# Example 1: Basic Usage - Just give it a JSON file
# ============================================================================

def example_basic():
    """The simplest way to convert JSON to CSV"""
    print("\n" + "="*60)
    print("Example 1: Basic Conversion")
    print("="*60)
    
    pipeline = JSONToCSVPipeline("data/input/sample.json")
    output_file = pipeline.run()
    
    print(f"Done! CSV created at: {output_file}")


# ============================================================================
# Example 2: Custom Output Directory
# ============================================================================

def example_custom_output():
    """Specify where to save the CSV"""
    print("\n" + "="*60)
    print("Example 2: Custom Output Directory")
    print("="*60)
    
    pipeline = JSONToCSVPipeline(
        input_file="data/input/products.json",
        output_dir="data/output"
    )
    output_file = pipeline.run()
    
    print(f"Done! CSV created at: {output_file}")


# ============================================================================
# Example 3: Custom Filename
# ============================================================================

def example_custom_filename():
    """Specify the output filename"""
    print("\n" + "="*60)
    print("Example 3: Custom Filename")
    print("="*60)
    
    pipeline = JSONToCSVPipeline("data/input/sample.json")
    output_file = pipeline.run(output_filename="my_data.csv")
    
    print(f"Done! CSV created at: {output_file}")


# ============================================================================
# Example 4: Process Multiple Files
# ============================================================================

def example_batch():
    """Convert all JSON files in a directory"""
    print("\n" + "="*60)
    print("Example 4: Batch Processing")
    print("="*60)
    
    input_dir = Path("data/input")
    json_files = list(input_dir.glob("*.json"))
    
    for json_file in json_files:
        print(f"\nConverting: {json_file.name}")
        pipeline = JSONToCSVPipeline(str(json_file))
        output_file = pipeline.run()
        print(f"  -> {output_file.name}")


# ============================================================================
# Example 5: Error Handling
# ============================================================================

def example_error_handling():
    """Handle errors gracefully"""
    print("\n" + "="*60)
    print("Example 5: Error Handling")
    print("="*60)
    
    try:
        pipeline = JSONToCSVPipeline("non_existent_file.json")
        pipeline.run()
    except FileNotFoundError as e:
        print(f"File not found: {e}")
    except Exception as e:
        print(f"Error: {e}")


# ============================================================================
# Run Examples
# ============================================================================

if __name__ == "__main__":
    print("\n" + "="*60)
    print("JSON to CSV Pipeline - Examples")
    print("="*60)
    
    try:
        example_basic()
        example_custom_output()
        # example_custom_filename()  # Uncomment to test
        # example_batch()            # Uncomment to test
        
    except Exception as e:
        print(f"Error: {e}")
    
    print("\n" + "="*60)
    print("Done! Check data/output/ for CSV files")
    print("Check logs/pipeline.log for detailed logs")
    print("="*60)
