#!/usr/bin/env python3
"""
Script to fix CSV file by replacing new lines within the product_doc field
to make it compatible with Databricks drag and drop upload.
"""

import csv
import sys

def fix_csv_file(input_file, output_file):
    """
    Fix CSV file by replacing new lines within the product_doc field with spaces.
    
    Args:
        input_file (str): Path to input CSV file
        output_file (str): Path to output fixed CSV file
    """
    try:
        with open(input_file, 'r', encoding='utf-8') as infile, \
             open(output_file, 'w', encoding='utf-8', newline='') as outfile:
            
            # Read the CSV with proper quoting to handle embedded new lines
            reader = csv.reader(infile, quoting=csv.QUOTE_ALL)
            writer = csv.writer(outfile, quoting=csv.QUOTE_ALL)
            
            # Process each row
            for row_num, row in enumerate(reader):
                if row_num == 0:
                    # Write header row as-is
                    writer.writerow(row)
                else:
                    # Fix the product_doc field (assuming it's the 4th column, index 3)
                    if len(row) >= 4:
                        # Replace new lines with spaces in the product_doc field
                        product_doc = row[3].replace('\n', ' ').replace('\r', ' ')
                        # Clean up multiple spaces
                        product_doc = ' '.join(product_doc.split())
                        row[3] = product_doc
                    
                    writer.writerow(row)
                
                # Progress indicator for large files
                if row_num % 1000 == 0:
                    print(f"Processed {row_num} rows...")
            
            print(f"Successfully processed {row_num + 1} rows")
            print(f"Fixed CSV saved to: {output_file}")
            
    except Exception as e:
        print(f"Error processing file: {e}")
        sys.exit(1)

if __name__ == "__main__":
    input_file = "/Users/justin.monaldo/Documents/experimentation/agents_lab_product_product_docs.csv"
    output_file = "/Users/justin.monaldo/Documents/experimentation/agents_lab_product_product_docs_fixed.csv"
    
    print(f"Fixing CSV file: {input_file}")
    print(f"Output will be saved to: {output_file}")
    
    fix_csv_file(input_file, output_file) 