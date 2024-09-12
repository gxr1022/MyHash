#include <vector>
#include <string>
#include <optional>
#include <cmath>
#include <cstdint>
#include "Myhash.h"
#include <mutex>
#include <atomic>
#include <fstream>
#include <unistd.h>
#include <cassert>
#include <iomanip>
#include <thread>

DEFINE_uint64(str_key_size, 8, "size of key (bytes)");
DEFINE_uint64(str_value_size, 56, "size of value (bytes)");
DEFINE_uint64(num_threads, 1, "the number of threads");
DEFINE_uint64(num_of_ops, 100000, "the number of operations");
DEFINE_uint64(time_interval, 10, "the time interval of insert operations");
DEFINE_uint64(chunk_size, 64, "the size of chunck size");
DEFINE_uint64(num_chunks, 10000000, "the number of chunks in a memory block");
DEFINE_string(report_prefix, "[report] ", "prefix of report data");

class Client
{
private:
    /* data */
public:
    uint64_t key_size_;
    uint64_t value_size_;
    uint64_t num_of_ops_;
    uint64_t num_threads_;
    uint64_t time_interval_;
    size_t chunk_size_;
    size_t num_chunks_;

    std::unique_ptr<Myhash> myhash;
    std::string load_benchmark_prefix = "load";

    std::string common_value_;

    Client(int argc, char **argv);
    ~Client();
    void client_ops_cnt(uint32_t ops_num);
    void load_and_run();

    std::string from_uint64_to_string(uint64_t value,uint64_t value_size);
    void benchmark_report(const std::string benchmark_prefix, const std::string &name, const std::string &value)
    {
        standard_report(benchmark_prefix, name, value);
    }
    void init_myhash(size_t chunk_size,size_t num_chunks)
    {
        myhash =  std::make_unique<Myhash>(chunk_size,num_chunks);
    }
    void init_common_value()
    {
        for (int i = 0; i < value_size_; i++)
        {
            common_value_ += (char)('a' + (i % 26));
        }
    }
};

Client::Client(int argc, char **argv)
{
    google::ParseCommandLineFlags(&argc, &argv, false);

    this->num_threads_ = FLAGS_num_threads;
    this->num_of_ops_ = FLAGS_num_of_ops;
    this->key_size_ = FLAGS_str_key_size;
    this->value_size_ = FLAGS_str_value_size;
    this->time_interval_ = FLAGS_time_interval;
    this->chunk_size_ = FLAGS_chunk_size;
    this->num_chunks_ = FLAGS_num_chunks;
    
    init_common_value();
    init_myhash(chunk_size_,num_chunks_);

}

Client::~Client()
{

}

std::string Client::from_uint64_to_string(uint64_t value,uint64_t value_size)
{
    std::stringstream ss;
    ss << std::setfill('0') << std::setw(value_size) << std::hex << value;
    std::string str = ss.str();
    if (str.length() > value_size) {
        str = str.substr(str.length() - value_size);
    }
    return ss.str();
}

void Client::load_and_run()
{
    // Create and start client threads
    std::vector<std::thread> threads;
    uint32_t num_of_ops_per_thread = num_of_ops_ / num_threads_;
    for (int i = 0; i < num_threads_; i++)
    {
        threads.emplace_back([this, num_of_ops_per_thread]() {
            this->client_ops_cnt(num_of_ops_per_thread);
        });
    }

    // Wait for all client threads to finish
    auto start_time = std::chrono::high_resolution_clock::now();
    for (auto &thread : threads)
    {
        thread.join();
    }
    double duration_ns = std::chrono::duration_cast<std::chrono::nanoseconds>(std::chrono::system_clock::now() - start_time).count();
    double duration_s = duration_ns / 1000000000;
    double throughput = num_of_ops_ / duration_s;
    double average_latency_ns = (double)duration_ns / num_of_ops_;

    // benchmark_report(load_benchmark_prefix, "overall_duration_ns", std::to_string(duration_ns));
    benchmark_report(load_benchmark_prefix, "overall_duration_s", std::to_string(duration_s));
    benchmark_report(load_benchmark_prefix, "overall_throughput", std::to_string(throughput));
    benchmark_report(load_benchmark_prefix, "overall_average_latency_ns", std::to_string(average_latency_ns));
}

void Client::client_ops_cnt(uint32_t ops_num) {

    uint32_t num_failed = 0;
    int ret = 0;
    void * search_addr = NULL;
    
    uint64_t rand=0;
    std::string key;
    for (int i = 0; i < ops_num; i ++) {
        key=from_uint64_to_string(rand,key_size_);
        KVInfo *kv_info;
        kv_info->key_addr = &key;
        kv_info->key_len = key_size_;
        kv_info->value_len = value_size_;
        kv_info->ops_type = KV_REQ_INSERT;

        switch (kv_info->ops_type) {
        case KV_REQ_SEARCH:
            search_addr = myhash->kv_search(kv_info);
            if (search_addr == NULL) {
                num_failed ++;
            }
            break;
        case KV_REQ_INSERT:
            ret = myhash->kv_insert(kv_info);
            if (ret == KV_OPS_FAIL_RETURN) {
                num_failed++;
            }
            break;
        case KV_REQ_UPDATE:
            myhash->kv_update(kv_info);
            break;
        case KV_REQ_DELETE:
            myhash->kv_delete(kv_info);
            break;
        default:
            myhash->kv_search(kv_info);
            break;
        }
    }
    // fiber_args->num_failed = num_failed;
    rand++;
    return;
}

void standard_report(const std::string &prefix, const std::string &name, const std::string &value)
{
    std::cout << FLAGS_report_prefix << prefix + "_" << name << " : " << value << std::endl;
}





