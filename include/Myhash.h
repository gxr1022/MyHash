// #include <verona.h>
#pragma once
#include <vector>
#include <string>
#include <optional>
#include <cmath>
#include <iostream>
#include <cstdint>
#include <atomic>
#include <thread>
#include <mutex>
#include "mm.h"
#include "../util/util.hpp"
#include "../util/bitmap.hpp"
#include "../util/checksum.hpp"

#define HASH_GLOBAL_DEPTH              (5)
#define HASH_INIT_LOCAL_DEPTH          (5)
#define HASH_SUBTABLE_NUM              (1 << HASH_GLOBAL_DEPTH)
#define HASH_INIT_SUBTABLE_NUM         (1 << HASH_INIT_LOCAL_DEPTH)
#define HASH_MAX_GLOBAL_DEPTH          (5)
#define HASH_MAX_SUBTABLE_NUM          (1 << HASH_MAX_GLOBAL_DEPTH)
#define HASH_ASSOC_NUM                 (7)
#define MAX_REP_NUM                    (10)
#define HASH_INIT_SUBTABLE_NUM         (1 << HASH_INIT_LOCAL_DEPTH)
#define HASH_ADDRESSABLE_BUCKET_NUM    (34000ULL)
#define HASH_SUBTABLE_BUCKET_NUM       (HASH_ADDRESSABLE_BUCKET_NUM * 3 / 2) //Combined buckets
#define SUBTABLE_LEN          (HASH_ADDRESSABLE_BUCKET_NUM * sizeof(Bucket))
#define SUBTABLE_RES_LEN      (HASH_MAX_SUBTABLE_NUM * SUBTABLE_LEN)
#define SUBTABLE_USED_HASH_BIT_NUM          (32)
#define HASH_MASK(n)                   ((1 << n) - 1)


struct Slot
{
  uint8_t fp;
  uint8_t len;
  uint8_t pointer[6];
};

struct Header
{
  uint32_t local_depth;
  uint32_t prefix;
};

struct Bucket 
{
  Slot slots[HASH_ASSOC_NUM];
  Header h;
};

struct Subtable
{  
  uint8_t lock; 
  uint8_t local_depth;
  uint8_t pointer[6];
};

struct KVInfo {
    void   * key_addr;
    void   * value_addr;
    uint16_t key_len;
    uint32_t value_len;
    uint8_t ops_type;
};

struct KVHashInfo {
    uint64_t hash_value;
    uint64_t prefix; // index the subtable
    uint8_t  fp; 
    uint8_t  local_depth;
};

struct KVTableAddrInfo {
    uint64_t    f_bucket_addr;
    uint64_t    s_bucket_addr; // they are calculated by s_idx; represents the start address of the combined buckets.

    uint32_t    f_main_idx;
    uint32_t    s_main_idx;

    uint32_t    f_idx;
    uint32_t    s_idx;
};

struct KVRWAddr {
    uint64_t addr;
    uint32_t len; // the number of KVblocks
};

struct HashRoot{
    uint64_t global_depth;
    uint64_t max_global_depth;
    uint64_t init_local_depth;

    uint64_t prefix_num;

    uint64_t subtable_init_num;
    uint64_t subtable_hash_range;
    uint64_t subtable_bucket_num;
    uint64_t seed;

    uint64_t kv_offset; 
    // uint64_t kv_len; 
    Subtable subtable_entry[HASH_MAX_SUBTABLE_NUM]; // it's like a directory of a extensible hash
};

enum KVRequestType {
    KV_REQ_SEARCH,
    KV_REQ_INSERT,
    KV_REQ_UPDATE,
    KV_REQ_DELETE,
};

enum KVOpsRetCode {
    KV_OPS_SUCCESS = 0,
    KV_OPS_FAIL_RETURN,
};

struct KVReqCtx {
    // bucket_info
    Bucket * f_com_bucket; 
    Bucket * s_com_bucket;
    Slot   * slot_arr[4]; 
    uint64_t checksum;

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

class Myhash{
private:
    MemoryPool * mm_;
    HashRoot * root_; // get the directory address and  KV block address.

    void get_comb_bucket_info(KVReqCtx * ctx);
    void get_comb_bucket_info(KVTableAddrInfo *tbl_addr_info,Bucket* f_com_bucket, Bucket* s_com_bucket );
    void get_kv_addr_info(KVHashInfo * a_kv_hash_info, KVTableAddrInfo * a_kv_addr_info);
    void get_kv_hash_info( uint64_t* key_addr, uint64_t key_len, KVHashInfo * a_kv_hash_info);
    int fill_slot(MMAllocCtx * mm_alloc_ctx, KVHashInfo * a_kv_hash_info, Slot* target_slot);
    int fill_slot(uint64_t iter_kv_subblock_addr, uint8_t iter_kv_subblock_num,  KVHashInfo * a_kv_hash_info, Slot* target_slot);
    void find_empty_slot(KVReqCtx * ctx);
    void find_empty_slot(KVTableAddrInfo *tbl_addr_info, Bucket* f_com_bucket,Bucket* s_com_bucket,int32_t &bucket_idx,int32_t &slot_idx);
    void find_kv_in_buckets(KVReqCtx * ctx);
    void check_kv_in_candidate_buckets(KVReqCtx * ctx);
    void find_slot_in_buckets(KVReqCtx * ctx, Slot* target_slot);
    int atomic_write_to_slot(Slot* target_slot,uint64_t expected, uint64_t new_value);
    uint32_t GetFreeSlotNum(Bucket * bucket, uint32_t * free_idx);

    inline char * get_key(KVInfo * kv_info) {
        return (char *)((uint64_t)kv_info->key_addr);
    }

    inline char * get_value(KVInfo * kv_info) {
        return (char *)((uint64_t)kv_info->key_addr + kv_info->key_len);
    }

public:
    Myhash(){};
    Myhash(size_t chunk_size, size_t num_chunks);
    ~Myhash(){}

    KVInfo   * kv_info_list_;
    KVReqCtx * kv_req_ctx_list_;
    uint32_t   num_total_operations_;
    std::mutex subtable_entry_mutex;

    void init_hash_table();
    void init_root();
    void init_subtable();

    void free_batch();

    int kv_update(KVInfo * kv_info);
    int kv_insert(KVInfo * kv_info);
    void kv_insert_read_buckets_and_write_kv(KVReqCtx * ctx);
    void * kv_search(KVInfo * kv_info);
    int kv_delete(KVInfo * kv_info);
    void kv_resize(uint64_t cur_prefix, uint8_t cur_local_depth);
    void kv_resize_update_slot(uint64_t iter_kv_subblock_addr, uint8_t iter_kv_subblock_num, KVHashInfo *kv_hash_info, KVTableAddrInfo *tbl_addr_info );

    inline MemoryPool * get_mm() {
        return mm_;
    }
};

static inline void ConvertSlotToAddr(Slot * slot, KVRWAddr * kv_addr) {

    kv_addr->addr = HashIndexConvert48To64Bits(slot->pointer);
}

static inline uint64_t ConvertSlotTo64Bits(const Slot& slot) {
    uint64_t packed = 0;
    packed |= static_cast<uint64_t>(slot.fp) << 56;
    packed |= static_cast<uint64_t>(slot.len) << 48;

    uint64_t pointer_value = 0;
    memcpy(&pointer_value, slot.pointer, 6);
    packed |= pointer_value & 0xFFFFFFFFFFFF;  

    return packed;
}



