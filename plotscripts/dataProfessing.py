import os
import re
import csv
from collections import defaultdict

directory = '/mnt/nvme0/home/gxr/Myhash/log/2024-09-30-22-09-08'

file_pattern = re.compile(r'myhash\.(\d+)\.thread\.(true|false)\.(\d+)\.(\d+)\.(\d+s)\.log')

time_pattern = re.compile(r'\[report\] load_overall_throughput\s*:\s*([\d\.]+)')


# Results dictionary to store throughput for each combination of params
results = defaultdict(lambda: defaultdict(dict))

# Read and process each file in the directory
for filename in os.listdir(directory):
    print(f'Processing: {filename}')
    match = file_pattern.match(filename)

    if match:
        # Extract thread count, true/false, and unique combination
        thread_count = int(match.group(1))
        true_or_false = match.group(2)
        combo = f"{match.group(3)}_{match.group(4)}"  # e.g., "8_100" or "8_1024"
        
        filepath = os.path.join(directory, filename)
        
        # Read file content and extract throughput
        with open(filepath, 'r') as file:
            content = file.read()
            time_match = time_pattern.search(content)
            if time_match:
                throughput = time_match.group(1)
                results[combo][thread_count][true_or_false] = throughput

# Prepare output base directory
csv_base_dir = '../data'
folder_name = directory.split('/')[-1]
combined_dir = os.path.join(csv_base_dir, folder_name)
os.makedirs(combined_dir, exist_ok=True)

# Write results to CSV files, one per unique combo
for combo, thread_results in results.items():
    # Construct the output CSV path
    csv_filename = f'{combo}_mutex_overall_throughput.csv'
    combined_path = os.path.join(combined_dir, csv_filename)

    # Write the results to CSV
    with open(combined_path, 'w', newline='') as csvfile:
        writer = csv.writer(csvfile)
        # Header row
        writer.writerow(['Thread Count', 'true Throughput', 'false Throughput'])
        
        # Sort thread results by thread count
        sorted_thread_results = sorted(thread_results.items())
        
        # Data rows
        for thread_count, throughputs in sorted_thread_results:
            true_value = throughputs.get('true', 'N/A')
            false_value = throughputs.get('false', 'N/A')
            writer.writerow([thread_count, true_value, false_value])

    print(f'Results for {combo} have been saved to {combined_path}')
