import csv
import json
from pathlib import Path

def parse_mdrm_for_call_report(csv_path, output_json_path=None):
    """
    Parse MDRM CSV to extract RCON/RCFD codes and descriptions
    """
    print("🔍 Parsing MDRM CSV for Call Report codes...")
    
    # Dictionary to store our mappings
    code_definitions = {}
    
    # Counters for statistics
    total_rows = 0
    rcon_count = 0
    rcfd_count = 0
    other_rc_count = 0
    
    try:
        with open(csv_path, 'r', encoding='utf-8') as csvfile:
            # Skip the first line that just says "PUBLIC"
            next(csvfile)
            
            # Now read with CSV reader
            reader = csv.DictReader(csvfile)
            
            # Print headers to verify format
            print(f"CSV Headers: {reader.fieldnames}")
            
            for row in reader:
                total_rows += 1
                
                # Get the mnemonic and item code
                mnemonic = row.get('Mnemonic', '').strip().strip('"')
                item_code = row.get('Item Code', '').strip().strip('"')
                
                # Construct full code
                full_code = f"{mnemonic}{item_code}"
                
                # Check if it's a Call Report code (RCON, RCFD, RIAD, etc.)
                if mnemonic in ['RCON', 'RCFD', 'RIAD', 'RCFN', 'RCOA', 'RCOB', 'RCOC', 'RCOD']:
                    # Get the item name (description)
                    item_name = row.get('Item Name', '').strip().strip('"')
                    
                    if item_name:  # Only add if we have a description
                        code_definitions[full_code] = item_name
                        
                        # Count by type
                        if mnemonic == 'RCON':
                            rcon_count += 1
                        elif mnemonic == 'RCFD':
                            rcfd_count += 1
                        else:
                            other_rc_count += 1
                
                # Progress indicator
                if total_rows % 10000 == 0:
                    print(f"  Processed {total_rows} rows...")
        
        print(f"\n✓ Parsing complete!")
        print(f"  Total rows processed: {total_rows}")
        print(f"  RCON codes found: {rcon_count}")
        print(f"  RCFD codes found: {rcfd_count}")
        print(f"  Other RC codes found: {other_rc_count}")
        print(f"  Total Call Report codes: {len(code_definitions)}")
        
        # Save to JSON
        if output_json_path is None:
            output_json_path = Path(csv_path).parent / 'call_report_codes.json'
        
        with open(output_json_path, 'w', encoding='utf-8') as f:
            json.dump(code_definitions, f, indent=2, ensure_ascii=False)
        
        print(f"\n✓ Saved dictionary to: {output_json_path}")
        
        # Show some examples
        print("\n📋 Sample Call Report codes found:")
        examples = list(code_definitions.items())[:10]
        for code, desc in examples:
            print(f"  {code}: {desc}")
        
        # Show some specific important codes if they exist
        print("\n📋 Common codes check:")
        common_codes = ['RCON2170', 'RCFD2170', 'RCON2948', 'RCFD2948', 'RCON3210', 'RCFD3210']
        for code in common_codes:
            if code in code_definitions:
                print(f"  ✓ {code}: {code_definitions[code]}")
            else:
                print(f"  ✗ {code}: Not found")
        
        return code_definitions
        
    except Exception as e:
        print(f"❌ Error: {str(e)}")
        import traceback
        traceback.print_exc()
        return {}

# Run the parser
if __name__ == "__main__":
    # Path to your MDRM CSV
    mdrm_path = #Unzip MDRM file from Fed website and place MDRM_CSV file path here for code to run
    
    # Parse it
    definitions = parse_mdrm_for_call_report(mdrm_path)
    
    print(f"\n📊 Final summary: Found {len(definitions)} Call Report codes")