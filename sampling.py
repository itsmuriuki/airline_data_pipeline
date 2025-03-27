import pandas as pd
import os

def sample_flight_data(input_path='data/raw/flight_data_full.csv', 
                      output_path='data/raw/flight_data.csv',
                      n_rows=2000,
                      random_state=42):
    # Ensure output directory exists
    os.makedirs(os.path.dirname(output_path), exist_ok=True)
    
    print(f"Reading data from {input_path}...")
    try:
        # Read the original file with low_memory=False to avoid dtype warnings
        df = pd.read_csv(input_path, low_memory=False)
        
        # Take a random sample
        sampled_df = df.sample(n=n_rows, random_state=random_state)
        
        # Check actual column names
        print("Available columns:", sampled_df.columns.tolist())
        
        # Sort by date and time (using actual column names)
        if 'FL_DATE' in sampled_df.columns:
            sampled_df = sampled_df.sort_values(by=['FL_DATE'])
        
        # Write to CSV without index
        print(f"Writing {n_rows} sampled rows to {output_path}...")
        sampled_df.to_csv(output_path, index=False)
        
        print(f"Successfully sampled {n_rows} rows from {input_path} to {output_path}")
        
    except FileNotFoundError:
        print(f"Error: Input file not found at {input_path}")
        print("Make sure flight_data.csv is in the same directory as the script")
        exit(1)
    except Exception as e:
        print(f"Error: {str(e)}")
        exit(1)

if __name__ == "__main__":
    sample_flight_data() 