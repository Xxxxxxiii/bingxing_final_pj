#include <iostream>
#include <vector>
#include <random>
#include <chrono>
#include <algorithm>
#include <omp.h>
#include <fstream>
#include <map>
#include <iomanip>
#include <windows.h> // For SetConsoleOutputCP

// =====================
// 串行快速排序 (基于Lomuto分区方案)
// =====================
int partition(std::vector<int>& arr, int low, int high) {
    if (low >= high) return low;
    int pivot = arr[high];
    int i = (low - 1);
    for (int j = low; j <= high - 1; j++) {
        if (arr[j] < pivot) {
            i++;
            std::swap(arr[i], arr[j]);
        }
    }
    std::swap(arr[i + 1], arr[high]);
    return (i + 1);
}

void serial_quicksort(std::vector<int>& arr, int low, int high) {
    if (low < high) {
        int pi = partition(arr, low, high);
        serial_quicksort(arr, low, pi - 1);
        serial_quicksort(arr, pi + 1, high);
    }
}

// =====================
// 并行快速排序 (APRAM-CRCW 模拟: 分块-排序-归并)
// =====================
void parallel_quicksort_apram(std::vector<int>& arr, int num_threads) {
    long long n = arr.size();
    if (n <= 1) {
        return;
    }

    if (num_threads >= n || num_threads <= 1) {
        serial_quicksort(arr, 0, n - 1);
        return;
    }

    std::vector<std::pair<int, int>> chunks;
    long long chunk_size = (n + num_threads - 1) / num_threads;
    for (int i = 0; i < num_threads; ++i) {
        long long low = i * chunk_size;
        long long high = std::min(low + chunk_size - 1, n - 1);
        if (low <= high) {
            chunks.push_back({(int)low, (int)high});
        }
    }

    #pragma omp parallel for
    for (size_t i = 0; i < chunks.size(); ++i) {
        serial_quicksort(arr, chunks[i].first, chunks[i].second);
    }

    for (size_t i = 1; i < chunks.size(); ++i) {
        std::inplace_merge(arr.begin(), arr.begin() + chunks[i].first, arr.begin() + chunks[i].second + 1);
    }
}

// =====================
// 性能测试 (单次运行)
// =====================
std::map<int, double> test_speedup_single_run(long long data_size, const std::vector<int>& thread_list, unsigned int seed) {
    std::cout << "\n数据规模: " << data_size << std::endl;

    std::vector<int> data(data_size);
    std::mt19937 gen(seed);
    std::uniform_int_distribution<> distrib(0, 1000000);
    for (long long i = 0; i < data_size; ++i) {
        data[i] = distrib(gen);
    }

    std::vector<int> data_copy_serial = data;
    auto start_serial = std::chrono::high_resolution_clock::now();
    serial_quicksort(data_copy_serial, 0, data_size - 1);
    auto end_serial = std::chrono::high_resolution_clock::now();
    std::chrono::duration<double> serial_time_duration = end_serial - start_serial;
    double serial_time = serial_time_duration.count();
    if (serial_time == 0) serial_time = 1e-9;
    std::cout << "  串行时间: " << std::fixed << std::setprecision(4) << serial_time << "s" << std::endl;

    std::map<int, double> speedups;
    for (int t : thread_list) {
        std::vector<int> data_copy_parallel = data;
        omp_set_num_threads(t);

        auto start_parallel = std::chrono::high_resolution_clock::now();
        parallel_quicksort_apram(data_copy_parallel, t);
        auto end_parallel = std::chrono::high_resolution_clock::now();
        std::chrono::duration<double> parallel_time_duration = end_parallel - start_parallel;
        double parallel_time = parallel_time_duration.count();
        if (parallel_time == 0) parallel_time = 1e-9;
        
        double speedup = serial_time / parallel_time;
        speedups[t] = speedup;

        std::cout << "  线程数: " << std::setw(2) << t << " | "
                  << "时间: " << std::fixed << std::setprecision(4) << parallel_time << "s | "
                  << "加速比: " << std::fixed << std::setprecision(2) << speedup << std::endl;
    }
    return speedups;
}

// =====================
// 主函数
// =====================
int main() {
    SetConsoleOutputCP(65001); // 设置控制台输出编码为 UTF-8

    const int N_REPETITIONS = 10;
    const std::string LOG_FILENAME = "quicksort_cpp_log.txt";

    std::vector<long long> sizes = {1000, 5000, 10000, 100000, 1000000, 10000000, 100000000};
    // std::vector<long long> sizes = {1000000, 10000000}; 
    std::vector<int> thread_list = {1, 2, 4, 8, 16};

    int max_threads = omp_get_max_threads();
    
    // 数据结构：map<long long, map<int, vector<double>>> all_speedups;
    // all_speedups[data_size][thread_count] = vector_of_speedups
    std::map<long long, std::map<int, std::vector<double>>> all_speedups;
    for (long long size : sizes) {
        for (int t : thread_list) {
            all_speedups[size][t] = {};
        }
    }

    std::streambuf* original_cout = std::cout.rdbuf();
    std::ofstream log_file(LOG_FILENAME);

    std::cout.rdbuf(log_file.rdbuf()); // 重定向 cout 到文件

    std::cout << "CPU 最大线程数: " << max_threads << std::endl;
    std::cout << "开始进行 " << N_REPETITIONS << " 轮实验..." << std::endl;

    unsigned int base_seed = static_cast<unsigned int>(std::chrono::system_clock::now().time_since_epoch().count());

    for (int i = 0; i < N_REPETITIONS; ++i) {
        unsigned int current_seed = base_seed + i;
        std::cout << "\n" << std::string(20, '=') << " 实验轮次: " << (i + 1) << "/" << N_REPETITIONS 
                  << " (种子: " << current_seed << ") " << std::string(20, '=') << std::endl;
        
        for (long long size : sizes) {
            std::map<int, double> speedups_this_run = test_speedup_single_run(size, thread_list, current_seed);
            for (auto const& [t, speedup] : speedups_this_run) {
                all_speedups[size][t].push_back(speedup);
            }
            
            // 临时切换回控制台输出
            std::cout.rdbuf(original_cout);
            std::cout << "已完成第 " << (i + 1) << "/" << N_REPETITIONS << " 轮，数据规模为 " << size << " 的测试。" << std::endl;
            // 切换回文件输出
            std::cout.rdbuf(log_file.rdbuf());
        }
    }

    std::cout << "\n\n" << std::string(25, '=') << " 实验总结：平均加速比 (" << N_REPETITIONS << " 轮) " << std::string(25, '=') << std::endl;

    for (long long size : sizes) {
        std::cout << "\n数据规模: " << size << std::endl;
        if (all_speedups[size][thread_list[0]].empty()) {
            std::cout << "  没有足够的实验数据来计算平均值。" << std::endl;
            continue;
        }
        for (int t : thread_list) {
            double sum_speedup = 0;
            for (double s : all_speedups[size][t]) {
                sum_speedup += s;
            }
            double avg_speedup = sum_speedup / all_speedups[size][t].size();
            std::cout << "  线程数: " << std::setw(2) << t << " | 平均加速比: " << std::fixed << std::setprecision(2) << avg_speedup << std::endl;
        }
    }

    std::cout.rdbuf(original_cout); // 恢复 cout
    std::cout << "实验完成。详细日志已写入文件: '" << LOG_FILENAME << "'" << std::endl;

    return 0;
}
