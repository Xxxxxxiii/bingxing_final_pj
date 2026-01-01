import re
import os
import pandas as pd
import matplotlib.pyplot as plt
import seaborn as sns
import numpy as np
from matplotlib.ticker import FuncFormatter

def parse_size_to_int(size_str):
    """将 '1K', '10M', '1G' 这样的字符串转换为整数。"""
    size_str = size_str.upper()
    if 'K' in size_str:
        return int(float(size_str.replace('K', '')) * 1_000)
    elif 'M' in size_str:
        return int(float(size_str.replace('M', '')) * 1_000_000)
    elif 'G' in size_str:
        return int(float(size_str.replace('G', '')) * 1_000_000_000)
    else:
        return int(size_str)

def parse_final_logs(directory):
    """解析最终结果目录中的所有日志文件。"""
    all_data = []
    
    size_pattern = re.compile(r"input_([\w.]+)_results.txt")
    serial_time_pattern = re.compile(r"Baseline Serial Time \(1 core\): ([\d.]+) seconds")
    parallelism_pattern = re.compile(r"--- Parallelism: (\d+) cores ---")
    speedup_pattern = re.compile(r"Run \d+: .*?Speedup=([\d.]+)x")
    run_pattern = re.compile(r"Run (\d+):")

    for filename in os.listdir(directory):
        if filename.endswith("_results.txt"):
            filepath = os.path.join(directory, filename)
            
            size_match = size_pattern.search(filename)
            if not size_match:
                continue
            
            data_size_str = size_match.group(1)
            data_size = parse_size_to_int(data_size_str)
            
            with open(filepath, 'r', encoding='utf-8') as f:
                content = f.read()

            serial_time_match = serial_time_pattern.search(content)
            serial_time = float(serial_time_match.group(1)) if serial_time_match else None

            sections = parallelism_pattern.split(content)
            
            for i in range(1, len(sections), 2):
                cores = int(sections[i])
                data_block = sections[i+1]
                
                run_matches = run_pattern.finditer(data_block)
                speedup_matches = list(speedup_pattern.finditer(data_block))

                for idx, run_match in enumerate(run_matches):
                    if idx < len(speedup_matches):
                        run_num = int(run_match.group(1))
                        speedup = float(speedup_matches[idx].group(1))
                        
                        all_data.append({
                            'run': run_num,
                            'data_size': data_size,
                            'threads': cores,
                            'speedup': speedup,
                            'serial_time': serial_time
                        })

    if not all_data:
        print("错误: 未能从日志文件中解析出任何数据。")
        return None

    return pd.DataFrame(all_data)

def plot_speedup_vs_size(df_all, output_filename='final_speedup_by_size.png'):
    """绘制加速比 vs. 数据规模的图表。"""
    if df_all is None or df_all.empty:
        print("没有可供绘制加速比图表的数据。")
        return

    df_avg = df_all.groupby(['data_size', 'threads'])['speedup'].mean().reset_index()

    sns.set_theme(style="whitegrid")
    plt.rcParams['font.sans-serif'] = ['SimHei']
    plt.rcParams['axes.unicode_minus'] = False

    fig, ax = plt.subplots(figsize=(14, 9))

    thread_counts = sorted(df_avg['threads'].unique())
    colors = plt.cm.viridis(np.linspace(0, 1, len(thread_counts)))
    color_map = dict(zip(thread_counts, colors))

    for core_count in thread_counts:
        subset = df_avg[df_avg['threads'] == core_count].sort_values('data_size')
        if not subset.empty:
            ax.plot(subset['data_size'], subset['speedup'],
                    marker='o', markersize=6, linestyle='-', linewidth=2.5,
                    color=color_map.get(core_count), label=f'{core_count} 核心')

    ax.axhline(y=1.0, color='red', linestyle='--', linewidth=1.5, label='无加速 (y=1)')
    ax.set_title('MapReduce 加速比分析 (按数据规模)', fontsize=18, pad=20)
    ax.set_xlabel('数据规模 (Data Size)', fontsize=12)
    ax.set_ylabel('加速比 (Speedup)', fontsize=12)

    ax.set_xscale('log')
    data_sizes = sorted(df_avg['data_size'].unique())
    ax.set_xticks(data_sizes)
    
    def format_size(val, pos):
        if val >= 1_000_000_000: return f'{val/1_000_000_000:.0f}G'
        if val >= 1_000_000: return f'{val/1_000_000:.0f}M'
        if val >= 1_000: return f'{val/1_000:.0f}K'
        return str(val)
        
    ax.get_xaxis().set_major_formatter(FuncFormatter(format_size))

    legend = ax.legend(title='核心数 (Cores)', fontsize=10, loc='best')
    plt.setp(legend.get_title(), fontsize=12)
    ax.grid(True, which="both", ls="--", linewidth=0.5)
    plt.tight_layout()

    plt.savefig(output_filename, dpi=300)
    print(f"加速比图表已成功保存为 '{output_filename}'")

def plot_serial_time_vs_size(df_all, output_filename='final_serial_time_by_size.png'):
    """绘制串行时间 vs. 数据规模的图表。"""
    if df_all is None or df_all.empty or 'serial_time' not in df_all.columns:
        print("没有可供绘制串行时间图表的数据。")
        return

    df_serial = df_all[['data_size', 'serial_time']].drop_duplicates().sort_values('data_size')

    sns.set_theme(style="whitegrid")
    plt.rcParams['font.sans-serif'] = ['SimHei']
    plt.rcParams['axes.unicode_minus'] = False

    fig, ax = plt.subplots(figsize=(12, 8))

    ax.plot(df_serial['data_size'], df_serial['serial_time'],
            marker='o', markersize=6, linestyle='-', linewidth=2.5)

    ax.set_title('串行时间随数据规模的变化', fontsize=18, pad=20)
    ax.set_xlabel('数据规模 (Data Size)', fontsize=12)
    ax.set_ylabel('串行时间 (秒)', fontsize=12)

    ax.set_xscale('log')
    data_sizes = sorted(df_serial['data_size'].unique())
    ax.set_xticks(data_sizes)

    def format_size(val, pos):
        if val >= 1_000_000_000: return f'{val/1_000_000_000:.0f}G'
        if val >= 1_000_000: return f'{val/1_000_000:.0f}M'
        if val >= 1_000: return f'{val/1_000:.0f}K'
        return str(val)

    ax.get_xaxis().set_major_formatter(FuncFormatter(format_size))
    
    # 设置Y轴也为对数刻度，因为时间跨度可能也很大
    ax.set_yscale('log')

    ax.grid(True, which="both", ls="--", linewidth=0.5)
    plt.tight_layout()

    plt.savefig(output_filename, dpi=300)
    print(f"串行时间图表已成功保存为 '{output_filename}'")


if __name__ == "__main__":
    log_directory = 'D:\\2025fall\\bingxing\\pj\\pj\\mapreduce_results\\FINAL-RES\\all'
    df_final_runs = parse_final_logs(log_directory)
    
    if df_final_runs is not None:
        # 过滤掉核心数为1的结果（如果存在）
        df_to_plot = df_final_runs[df_final_runs['threads'] != 1].copy()
        
        # 绘制第一张图
        plot_speedup_vs_size(df_to_plot)
        
        # 绘制第二张图
        plot_serial_time_vs_size(df_final_runs)