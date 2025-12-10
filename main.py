import multiprocessing
import random
import time
import re
from collections import defaultdict
from functools import reduce

LOG_SIZE = 5000_000
NUM_WORKERS = multiprocessing.cpu_count()
CHUNK_SIZE = LOG_SIZE // NUM_WORKERS

IPS = [f"192.168.1.{i}" for i in range(1, 50)]
STATUS_CODES = [200, 200, 200, 404, 500, 301]

def generate_mock_log_chunk(n_lines):
    data = []
    weighted_ips = IPS + ['10.0.0.1'] * 10
    
    for _ in range(n_lines):
        ip = random.choice(weighted_ips)
        code = random.choice(STATUS_CODES)
        timestamp = time.time()
        line = f"{timestamp} - INFO - {ip} - REQUEST:GET /api/v1/data - STATUS:{code}"
        data.append(line)
    return data

def mapper_function(log_chunk):
    local_ip_counts = defaultdict(int)
    local_code_counts = defaultdict(int)
    
    log_pattern = re.compile(r"(\d+\.\d+\.\d+\.\d+).*STATUS:(\d+)")
    
    for line in log_chunk:
        match = log_pattern.search(line)
        if match:
            ip = match.group(1)
            code = match.group(2)
            
            local_ip_counts[ip] += 1
            local_code_counts[code] += 1
            
    return {'ips': local_ip_counts, 'codes': local_code_counts}

def reducer_function(accumulated_result, new_result):
    for ip, count in new_result['ips'].items():
        accumulated_result['ips'][ip] += count
        
    for code, count in new_result['codes'].items():
        accumulated_result['codes'][code] += count
        
    return accumulated_result

def run_map_reduce_job():
    print(f"--- STARTING LOG ANALYSIS ENGINE ---")
    print(f"Dataset Size: {LOG_SIZE} records")
    print(f"Active Workers: {NUM_WORKERS}")
    
    start_global = time.time()

    print(f"[Main] Generating Data Chunks...")
    chunks = [generate_mock_log_chunk(CHUNK_SIZE) for _ in range(NUM_WORKERS)]
    
    print(f"[Main] Dispatching to Workers (Mapping Phase)...")
    map_start = time.time()
    
    with multiprocessing.Pool(processes=NUM_WORKERS) as pool:
        mapped_results = pool.map(mapper_function, chunks)
        
    print(f"[Main] Mapping finished in {time.time() - map_start:.4f}s")

    print(f"[Main] Aggregating Results (Reducing Phase)...")
    
    initial_state = {'ips': defaultdict(int), 'codes': defaultdict(int)}
    
    final_result = reduce(reducer_function, mapped_results, initial_state)
    
    duration = time.time() - start_global

    print("-" * 50)
    print(f"ANALYSIS COMPLETE in {duration:.4f} seconds")
    print("-" * 50)
    
    sorted_ips = sorted(final_result['ips'].items(), key=lambda x: x[1], reverse=True)[:3]
    print(f"TOP 3 SUSPICIOUS IPs:")
    for rank, (ip, count) in enumerate(sorted_ips, 1):
        print(f"  {rank}. {ip} : {count} requests")
        
    total_requests = sum(final_result['codes'].values())
    error_500 = final_result['codes'].get('500', 0)
    print(f"\nSYSTEM HEALTH:")
    print(f"  Total Requests Processed: {total_requests}")
    print(f"  Server Errors (500): {error_500}")
    print(f"  Error Rate: {(error_500/total_requests)*100:.2f}%")
    print("-" * 50)

if __name__ == "__main__":
    run_map_reduce_job()
