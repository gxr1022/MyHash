#include <vector>
#include <string>
#include <optional>
#include <cmath>
#include <cstdint>
#include "Myhash.h"
#include "mm.h"

struct KVReqCtx {
    // input
    uint8_t          req_type;

    Bucket * local_bucket_addr;  // the first bucket of target combination buckets. 


    // bucket_info
    Bucket * bucket_arr[4]; // each key value pair has four corresponding buckets,  i.e., two combined buckets.
    Bucket * f_com_bucket; 
    Bucket * s_com_bucket;
    Slot   * slot_arr[4]; 

    KVInfo         * kv_info;

    // for preparation
    KVHashInfo      hash_info;
    KVTableAddrInfo tbl_addr_info;
    
    
    // for key-value pair read
    std::vector<KVRWAddr>                     kv_read_addr_list;
    std::vector<std::pair<int32_t, int32_t> > kv_idx_list;

    // for kv insert/update/delete
    MMAllocCtx mm_alloc_ctx;
    uint8_t consensus_state;
    std::vector<KVCASAddr> kv_modify_pr_cas_list;
    std::vector<KVCASAddr> kv_modify_bk_0_cas_list; // all backup cas
    std::vector<KVCASAddr> kv_modify_bk_1_cas_list; // all not modified backups

    // for insert
    int32_t bucket_idx;
    int32_t slot_idx;

    // for kv update/delete
    KVRWAddr kv_invalid_addr;
    std::vector<KVRWAddr> log_commit_addr_list;
    std::vector<KVRWAddr> write_unused_addr_list;
    std::vector<std::pair<int32_t, int32_t> > recover_match_idx_list;

    bool is_finished;
    bool is_cache_hit;
    bool is_local_cache_hit;
    bool committed_need_cas_pr;
    bool has_modified_bk_idx;
    bool failed_pr_index; // set when the primary index failed
    bool failed_pr_addr;  // set when the primary data failed
    union {
        void * value_addr;    // for search return value
        int    ret_code;
    } ret_val;

    volatile bool * should_stop;

    std::string key_str;

};


class Client {
private:
    MemoryPool * mm_;

    // uint64_t hash_root_addr_;

    HashRoot * root_;


// private inline methods
private:
    inline int get_race_root() {      
        root_ = (HashRoot *)malloc(sizeof(HashRoot));
        return 0;
    }

    inline char * get_key(KVInfo * kv_info) {
        return (char *)((uint64_t)kv_info->l_addr + sizeof(KVLogHeader));
    }

    inline char * get_value(KVInfo * kv_info) {
        return (char *)((uint64_t)kv_info->l_addr + sizeof(KVLogHeader) + kv_info->key_len);
    }

    inline KVLogHeader * get_header(KVInfo * kv_info) {
        return (KVLogHeader *)kv_info->l_addr;
    }

    inline bool check_key(KVLogHeader * log_header, KVInfo * kv_info) {
        uint64_t r_key_addr = (uint64_t)log_header + sizeof(log_header);
        uint64_t l_key_addr = (uint64_t)kv_info->l_addr + sizeof(KVLogHeader);
        return CheckKey((void *)r_key_addr, log_header->key_length, (void *)l_key_addr, kv_info->key_len);
    }

    int init_hash_table();
    void init_root();
    void init_subtable();

    void init_kv_req_ctx(KVReqCtx * req_ctx, KVInfo * kv_info, char * operation);
    void update_log_tail(KVLogTail * log_header, ClientMMAllocCtx * alloc_ctx);

    int  client_recovery();
    void init_recover_req_ctx(KVInfo * kv_info, __OUT KVReqCtx * rec_ctx);



    // public methods
public:
    Client();
    ~Client();

    KVInfo   * kv_info_list_;
    KVReqCtx * kv_req_ctx_list_;
    uint32_t   num_total_operations_;
    uint32_t   num_local_operations_;
    uint32_t   num_coroutines_;
    int        workload_run_time_;
    int        micro_workload_num_;

    bool stop_gc_;

    int kv_update(KVInfo * kv_info);
    int kv_update(KVReqCtx * ctx);
    int kv_update_w_cache(KVInfo * kv_info);
    int kv_update_w_crash(KVReqCtx * ctx, int crash_point);
    int kv_update_sync(KVReqCtx * ctx);
    
    int kv_insert(KVInfo * kv_info);
    int kv_insert(KVReqCtx * ctx);
    int kv_insert_w_cache(KVInfo * kv_info);
    int kv_insert_w_crash(KVReqCtx * ctx, int crash_point);
    int kv_insert_sync(KVReqCtx * ctx);

    void * kv_search(KVInfo * kv_info);
    void * kv_search(KVReqCtx * ctx);
    void * kv_search_w_cache(KVInfo * kv_info);
    void * kv_search_on_crash(KVReqCtx * ctx);
    void * kv_search_sync(KVReqCtx * ctx);
    
    int kv_delete(KVInfo * kv_info);
    int kv_delete(KVReqCtx * ctx);
    int kv_delete_w_cache(KVInfo * kv_info);
    int kv_delete_sync(KVReqCtx * ctx);

    int kv_recover(KVReqCtx * ctx);
    int kv_recover_update(KVReqCtx * ctx);
    int kv_recover_insert(KVReqCtx * ctx);
    
    pthread_t start_polling_thread();
    boost::fibers::fiber start_polling_fiber();
    void stop_polling_thread();
    void start_gc_fiber();
    void stop_gc_fiber();

    void free_batch();

    void init_kvreq_space(uint32_t coro_id, uint32_t kv_req_st_idx, uint32_t num_ops);
    void init_kv_insert_space(void * coro_local_addr, uint32_t kv_req_idx);
    void init_kv_insert_space(void * coro_local_addr, KVReqCtx * kv_req_ctx);
    void init_kv_search_space(void * coro_local_addr, uint32_t kv_req_idx);
    void init_kv_search_space(void * coro_local_addr, KVReqCtx * kv_req_ctx);
    void init_kv_update_space(void * coro_local_addr, uint32_t kv_req_idx);
    void init_kv_update_space(void * coro_local_addr, KVReqCtx * kv_req_ctx);
    void init_kv_delete_space(void * coro_local_addr, uint32_t kv_req_idx);
    void init_kv_delete_space(void * coro_local_addr, KVReqCtx * kv_req_ctx);

    void crash_server(const std::vector<uint8_t> & fail_server_list);

    void dump_cache();
    void load_cache();
    int  load_seq_kv_requests(uint32_t num_keys, char * op_type);
    int  load_kv_requests(const char * fname, uint32_t st_idx, int32_t num_ops);

    int get_num_rep();
    int get_my_server_id();
    int get_num_memory();
    int get_num_idx_rep();

    void get_recover_time(std::vector<struct timeval> & recover_time);

// for testing
public:
    int test_get_root(__OUT RaceHashRoot * race_root);
    int test_get_log_meta_info(__OUT ClientLogMetaInfo * remote_log_meta_info_list, 
            __OUT ClientLogMetaInfo * local_meta);
    int test_get_pr_log_meta_info(__OUT ClientLogMetaInfo * pr_log_meta_info);
    int test_get_remote_log_header(uint8_t server_id, uint64_t raddr, uint32_t buf_size,
            __OUT void * buf);
    int test_get_local_mm_blocks(__OUT ClientMMBlock * mm_block_list, __OUT uint64_t * list_len);
    ClientMetaAddrInfo ** test_get_meta_addr_info(__OUT uint64_t * list_len);

    inline ClientMM * get_mm() {
        return mm_;
    }

    inline UDPNetworkManager * get_nm() {
        return nm_;
    }
};


