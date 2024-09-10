#include <vector>
#include <string>
#include <optional>
#include <cmath>
#include <cstdint>
#include "Myhash.h"


struct KVReqCtx {
    // input
    uint8_t  req_type;
    uint16_t key_len;
    uint32_t value_len;
    uint64_t key;
    // std::string key_str;

    // bucket_info
    Bucket * f_com_bucket; 
    Bucket * s_com_bucket;
    Slot   * slot_arr[4]; 

    KVInfo * kv_info;

    KVHashInfo      hash_info;
    KVTableAddrInfo tbl_addr_info;

    // for kv block allocation
    MMAllocCtx mm_alloc_ctx;

    // for insert
    int32_t bucket_idx;
    int32_t slot_idx;

    union {
        void * value_addr;    // for search return value
        int    ret_code;
    } ret_val;
};


