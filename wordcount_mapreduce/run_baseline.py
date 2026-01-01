import subprocess
import os

# --- 配置 ---
script_path = "/gz-data/mapreduce_ver2.py"
input_dir = "/gz-data"
output_dir = "/gz-data/output_ver2_results_wo_4cores"

input_files = [
    "input_1K.txt",
    "input_1M.txt",
    "input_10M.txt",
    "input_100M.txt",
    "input_500M.txt",
    "input_1G.txt",
    "input_2G.txt",
]

cpu_list = [8, 16]
repeat = 5
serial_cores = 1

os.makedirs(output_dir, exist_ok=True)

def get_execution_time(output_text):
    """从 spark-submit 的输出中解析执行时间"""
    for line in output_text.splitlines():
        if "Job execution time:" in line:
            try:
                time_str = line.split(":")[1].strip().replace(" seconds", "")
                return float(time_str)
            except (ValueError, IndexError):
                return None
    return None

# --- 循环实验 ---
for file in input_files:
    input_path = os.path.join(input_dir, file)
    
    if not os.path.exists(input_path):
        print(f"Warning: Input file not found at '{input_path}', skipping.")
        continue

    file_name_base = os.path.splitext(file)[0]
    out_file_path = os.path.join(output_dir, f"{file_name_base}_results.txt")

    with open(out_file_path, "w") as f_out:
        f_out.write(f"Performance Results for {file}\n")
        f_out.write("="*40 + "\n")

        # --- 1. 计算基准串行时间 (只计算一次) ---
        print(f"Calculating baseline serial time for {file} (using {serial_cores} core)...")
        serial_time = None
        serial_run_failed = False
        
        cmd = f"spark-submit {script_path} {input_path} {serial_cores}"
        try:
            result = subprocess.run(cmd, shell=True, capture_output=True, text=True, check=True)
            serial_time = get_execution_time(result.stdout)
            if serial_time is None:
                print("    ERROR: Serial run succeeded but could not parse execution time.")
                f_out.write("FATAL: Baseline serial run FAILED to parse time.\n")
                serial_run_failed = True
        except subprocess.CalledProcessError as e:
            print(f"    FATAL ERROR: Serial run failed. Check logs in {out_file_path}.")
            f_out.write("FATAL: Baseline serial run failed.\n--- STDOUT ---\n" + e.stdout + "\n--- STDERR ---\n" + e.stderr + "\n")
            serial_run_failed = True

        if serial_run_failed:
            print(f"Skipping parallel tests for {file} due to serial run failure.\n")
            continue

        f_out.write(f"Baseline Serial Time ({serial_cores} core): {serial_time:.4f} seconds\n")
        print(f"  -> Serial Time: {serial_time:.4f} seconds\n")

        # --- 2. 进行并行测试并计算加速比 ---
        for cpu in cpu_list:
            f_out.write(f"\n--- Parallelism: {cpu} cores ---\n")
            print(f"Testing on {file} with {cpu} cores...")

            parallel_times = []
            speedups = []

            for r in range(1, repeat + 1):
                print(f"  -> Parallel Run {r}/{repeat}")
                cmd = f"spark-submit {script_path} {input_path} {cpu}"
                
                try:
                    result = subprocess.run(cmd, shell=True, capture_output=True, text=True, check=True)
                    parallel_time = get_execution_time(result.stdout)
                    
                    if parallel_time is not None and parallel_time > 0:
                        speedup = serial_time / parallel_time
                        parallel_times.append(parallel_time)
                        speedups.append(speedup)
                        f_out.write(f"Run {r}: Serial={serial_time:.4f}s, Parallel={parallel_time:.4f}s, Speedup={speedup:.2f}x\n")
                    else:
                        f_out.write(f"Run {r}: FAILED to parse time or time is zero.\n")

                except subprocess.CalledProcessError as e:
                    f_out.write(f"Run {r}: FAILED\n")
                    print(f"    ERROR: Spark job failed for {file} with {cpu} cores.")
                    f_out.write("--- STDOUT ---\n" + e.stdout + "\n--- STDERR ---\n" + e.stderr + "\n--------------\n")

            if parallel_times:
                avg_parallel_time = sum(parallel_times) / len(parallel_times)
                avg_speedup = sum(speedups) / len(speedups)
                f_out.write(f"Average: Parallel={avg_parallel_time:.4f}s, Speedup={avg_speedup:.2f}x\n")
            
            f_out.flush()

    print(f"All results for {file} saved to {out_file_path}\n")

print("All experiments finished.")