
import re
import pandas as pd
import matplotlib.pyplot as plt
import seaborn as sns
import numpy as np


def parse_log_file(filepath):
    '''解析日志文件，提取所有轮次的实验数据和平均数据。'''
    all_runs_data = []
    average_data = []

    run_pattern = re.compile(r"实验轮次: (\d+)/(\d+)")
    data_size_pattern = re.compile(r"数据规模: (\d+)")

    result_pattern = re.compile(r"线程数: \s*(\d+)\s*\|.*?加速比: ([\d.]+)")
    avg_result_pattern = re.compile(r"线程数: \s*(\d+)\s*\|\s*平均加速比: ([\d.]+)")

    current_run = 0
    current_data_size = 0
    in_summary_section = False

    try:
        with open(filepath, 'r', encoding='utf-8') as f:
            for line in f:
                if "实验总结" in line:
                    in_summary_section = True
                    continue

                run_match = run_pattern.search(line)
                if run_match:
                    current_run = int(run_match.group(1))
                    continue

                data_size_match = data_size_pattern.search(line)
                if data_size_match:
                    current_data_size = int(data_size_match.group(1))
                    continue

                if not in_summary_section:
                    result_match = result_pattern.search(line)
                    if result_match and current_run > 0 and current_data_size > 0:
                        threads = int(result_match.group(1))
                        speedup = float(result_match.group(2))
                        all_runs_data.append({
                            'run': current_run,
                            'data_size': current_data_size,
                            'threads': threads,
                            'speedup': speedup
                        })
                else:
                    avg_match = avg_result_pattern.search(line)
                    if avg_match and current_data_size > 0:
                        threads = int(avg_match.group(1))
                        avg_speedup = float(avg_match.group(2))
                        average_data.append({
                            'data_size': current_data_size,
                            'threads': threads,
                            'speedup': avg_speedup
                        })
    except FileNotFoundError:
        print(f"错误: 日志文件 '{filepath}' 未找到。")
        return None, None

    return pd.DataFrame(all_runs_data), pd.DataFrame(average_data)


def plot_speedup(df_all, df_avg, output_filename='quicksort_cpp_visualization.png'):
    '''根据解析出的数据绘制加速比折线图。'''
    if df_avg is None or df_avg.empty:
        print("没有可供可视化的平均数据。")
        return

    sns.set_theme(style="whitegrid")
    plt.rcParams['font.sans-serif'] = ['SimHei']
    plt.rcParams['axes.unicode_minus'] = False

    fig, ax = plt.subplots(figsize=(14, 9))

    data_sizes = sorted(df_avg['data_size'].unique())
    colors = plt.cm.viridis(np.linspace(0, 1, len(data_sizes)))
    color_map = dict(zip(data_sizes, colors))

    if df_all is not None and not df_all.empty:
        runs = df_all['run'].unique()
        for run in runs:
            for size in data_sizes:
                subset = df_all[(df_all['run'] == run) & (df_all['data_size'] == size)]
                if not subset.empty:
                    ax.plot(subset['threads'], subset['speedup'],
                            marker='', linestyle='-', color=color_map.get(size),
                            alpha=0.15, label='_nolegend_')

    for size in data_sizes:
        subset = df_avg[df_avg['data_size'] == size]
        if not subset.empty:
            ax.plot(subset['threads'], subset['speedup'],
                    marker='o', markersize=6, linestyle='-', linewidth=3.5,
                    color=color_map.get(size), label=f'{size:,}')

    ax.axhline(y=1.0, color='red', linestyle='--', linewidth=1.5, label='无加速 (No Speedup)')

    ax.set_title('C++ OpenMP 快速排序加速比分析', fontsize=18, pad=20)
    ax.set_xlabel('线程数 (Number of Threads)', fontsize=12)
    ax.set_ylabel('加速比 (Speedup)', fontsize=12)

    thread_counts = sorted(df_avg['threads'].unique())
    if len(thread_counts) > 0:
        ax.set_xscale('log', base=2)
        ax.set_xticks(thread_counts)
        ax.get_xaxis().set_major_formatter(plt.ScalarFormatter())

    legend = ax.legend(title='数据规模 (Data Size)', fontsize=10, loc='best')
    plt.setp(legend.get_title(), fontsize=12)

    ax.grid(True, which="both", ls="--", linewidth=0.5)
    plt.tight_layout()

    plt.savefig(output_filename, dpi=300)
    print(f"可视化图表已成功保存为 '{output_filename}'")


if __name__ == "__main__":
    log_file = 'quicksort_cpp_log.txt'
    df_all_runs, df_averages = parse_log_file(log_file)
    plot_speedup(df_all_runs, df_averages)
