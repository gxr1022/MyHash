// #include <verona.h>
#include <vector>
#include <string>
#include <optional>
#include <cmath>
#include <iostream>
#include <cstdint>
#include <atomic>
#include <thread>
#include <mutex>

#define NUMBER64_1 11400714785074694791ULL
#define NUMBER64_2 14029467366897019727ULL
#define NUMBER64_3 1609587929392839161ULL
#define NUMBER64_4 9650029242287828579ULL
#define NUMBER64_5 2870177450012600261ULL
#define hash_get64bits(x) hash_read64_align(x, align)
#define hash_get32bits(x) hash_read32_align(x, align)
#define shifting_hash(x, r) ((x << r) | (x >> (64 - r)))
#define TO64(x) (((U64_INT *)(x))->v)
#define TO32(x) (((U32_INT *)(x))->v)


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




typedef struct U64_INT {
    uint64_t v;
} U64_INT;

typedef struct U32_INT {
    uint32_t v;
} U32_INT;

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

typedef struct KVHashInfo {
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
    void get_kv_addr_info(KVHashInfo * a_kv_hash_info, KVTableAddrInfo * a_kv_addr_info);
    void get_kv_hash_info( uint64_t* key_addr, uint64_t key_len, KVHashInfo * a_kv_hash_info);
    int fill_slot(MMAllocCtx * mm_alloc_ctx, KVHashInfo * a_kv_hash_info, Slot* target_slot);
    void find_empty_slot(KVReqCtx * ctx);
    void find_kv_in_buckets(KVReqCtx * ctx);
    void check_kv_in_candidate_buckets(KVReqCtx * ctx);
    void find_slot_in_buckets(KVReqCtx * ctx, Slot* target_slot);
    int atomic_write_to_slot(Slot* target_slot, uint64_t atomic_slot_val);

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
    void kv_resize(KVReqCtx * ctx);

    inline MemoryPool * get_mm() {
        return mm_;
    }
};

static inline uint64_t HashIndexConvert40To64Bits(uint8_t * addr) {
    uint64_t ret = 0;
    return ret | ((uint64_t)addr[0] << 40) | ((uint64_t)addr[1] << 32)
        | ((uint64_t)addr[2] << 24) | ((uint64_t)addr[3] << 16) 
        | ((uint64_t)addr[4] << 8);
}

static inline uint64_t HashIndexConvert48To64Bits(uint8_t * addr) {
    uint64_t ret = 0;
    return ret | ((uint64_t)addr[0] << 48) | ((uint64_t)addr[1] << 40)
        | ((uint64_t)addr[2] << 32) | ((uint64_t)addr[3] << 24) 
        | ((uint64_t)addr[4] << 16) | ((uint64_t)addr[5] << 8);
}

static inline void HashIndexConvert64To40Bits(uint64_t addr, uint8_t * o_addr) {
    o_addr[0] = (uint8_t)((addr >> 40) & 0xFF);
    o_addr[1] = (uint8_t)((addr >> 32) & 0xFF);
    o_addr[2] = (uint8_t)((addr >> 24) & 0xFF);
    o_addr[3] = (uint8_t)((addr >> 16) & 0xFF);
    o_addr[4] = (uint8_t)((addr >> 8)  & 0xFF);
}

static inline void HashIndexConvert64To48Bits(uint64_t addr, uint8_t * o_addr) {
    o_addr[0] = (uint8_t)((addr >> 48) & 0xFF);
    o_addr[1] = (uint8_t)((addr >> 40) & 0xFF);
    o_addr[2] = (uint8_t)((addr >> 32) & 0xFF);
    o_addr[3] = (uint8_t)((addr >> 24) & 0xFF);
    o_addr[4] = (uint8_t)((addr >> 16) & 0xFF);
    o_addr[5] = (uint8_t)((addr >> 8)  & 0xFF);
}

// uint64_t HashIndexConvert64To48Bits(uint64_t full_addr) {
//     return full_addr & 0xFFFFFFFFFFFF;
// }

static uint64_t string_key_hash_computation(const void * data, uint64_t length, uint64_t seed, uint32_t align) {
    const uint8_t * p = (const uint8_t *)data;
    const uint8_t * end = p + length;
    uint64_t hash;

    if (length >= 32) {
        const uint8_t * const limitation  = end - 32;
        uint64_t v1 = seed + NUMBER64_1 + NUMBER64_2;
        uint64_t v2 = seed + NUMBER64_2;
        uint64_t v3 = seed + 0;
        uint64_t v4 = seed - NUMBER64_1;

        do {
            v1 += hash_get64bits(p) * NUMBER64_2;
            p += 8;
            v1 = shifting_hash(v1, 31);
            v1 *= NUMBER64_1;
            v2 += hash_get64bits(p) * NUMBER64_2;
            p += 8;
            v2 = shifting_hash(v2, 31);
            v2 *= NUMBER64_1;
            v3 += hash_get64bits(p) * NUMBER64_2;
            p += 8;
            v3 = shifting_hash(v3, 31);
            v3 *= NUMBER64_1;
            v4 += hash_get64bits(p) * NUMBER64_2;
            p += 8;
            v4 = shifting_hash(v4, 31);
            v4 *= NUMBER64_1;
        } while (p <= limitation);

        hash = shifting_hash(v1, 1) + shifting_hash(v2, 7) + shifting_hash(v3, 12) + shifting_hash(v4, 18);

        v1 *= NUMBER64_2;
        v1 = shifting_hash(v1, 31);
        v1 *= NUMBER64_1;
        hash ^= v1;
        hash = hash * NUMBER64_1 + NUMBER64_4;

        v2 *= NUMBER64_2;
        v2 = shifting_hash(v2, 31);
        v2 *= NUMBER64_1;
        hash ^= v2;
        hash = hash * NUMBER64_1 + NUMBER64_4;

        v3 *= NUMBER64_2;
        v3 = shifting_hash(v3, 31);
        v3 *= NUMBER64_1;
        hash ^= v3;
        hash = hash * NUMBER64_1 + NUMBER64_4;

        v4 *= NUMBER64_2;
        v4 = shifting_hash(v4, 31);
        v4 *= NUMBER64_1;
        hash ^= v4;
        hash = hash * NUMBER64_1 + NUMBER64_4;
    } else {
        hash = seed + NUMBER64_5;
    }

    hash += (uint64_t)length;

    while (p + 8 <= end) {
        uint64_t k1 = hash_get64bits(p);
        k1 *= NUMBER64_2;
        k1 = shifting_hash(k1, 31);
        k1 *= NUMBER64_1;
        hash ^= k1;
        hash = shifting_hash(hash, 27) * NUMBER64_1 + NUMBER64_4;
        p += 8;
    }

    if (p + 4 <= end) {
        hash ^= (uint64_t)(hash_get32bits(p)) * NUMBER64_1;
        hash = shifting_hash(hash, 23) * NUMBER64_2 + NUMBER64_3;
        p += 4;
    }

    while (p < end) {
        hash ^= (*p) * NUMBER64_5;
        hash = shifting_hash(hash, 11) * NUMBER64_1;
        p ++;
    }

    hash ^= hash >> 33;
    hash *= NUMBER64_2;
    hash ^= hash >> 29;
    hash *= NUMBER64_3;
    hash ^= hash >> 32;

    return hash;
}

static inline uint64_t SubtableFirstIndex(uint64_t hash_value, uint64_t capacity) {
    return hash_value % (capacity / 2);
}

static inline uint64_t SubtableSecondIndex(uint64_t hash_value, uint64_t f_index, uint64_t capacity) {
    uint32_t hash = hash_value;
    uint16_t partial = (uint16_t)(hash >> 16);
    uint16_t non_sero_tag = (partial >> 1 << 1) + 1;
    uint64_t hash_of_tag = (uint64_t)(non_sero_tag * 0xc6a4a7935bd1e995);
    return (uint64_t)(((uint64_t)(f_index) ^ hash_of_tag) % (capacity / 2) + capacity / 2);
}

static inline void ConvertSlotToAddr(Slot * slot, KVRWAddr * kv_addr) {

    kv_addr->addr = HashIndexConvert48To64Bits(slot->pointer);
}

static uint64_t hash_read64_align(const void * ptr, uint32_t align) {
    if (align == 0) {
        return TO64(ptr);
    }
    return *(uint64_t *)ptr;
}

static uint32_t hash_read32_align(const void * ptr, uint32_t align) {
    if (align == 0) {
        return TO32(ptr);
    }
    return *(uint32_t *)ptr;
}

bool IsEmptyPointer(uint8_t * pointer, uint32_t num) {
    for (int i = 0; i < num; i ++) {
        if (pointer[i] != 0) {
            return false;
        }
    }
    return true;
}

uint32_t GetFreeSlotNum(Bucket * bucket, uint32_t * free_idx) {
    *free_idx = HASH_ASSOC_NUM;
    uint32_t free_num = 0;
    for (int i = 0; i < HASH_ASSOC_NUM; i++) {
      // when initialize the subtable, set all bucket to 0.
        if (bucket->slots[i].fp == 0 && bucket->slots[i].len == 0 && IsEmptyPointer(bucket->slots[i].pointer, 5)) {
            // free_idx_list[free_num] = i;
            free_num ++;
            *free_idx = i;
        }
    }
    return free_num;
}

uint64_t VariableLengthHash(const void * data, uint64_t length, uint64_t seed) {
    if ((((uint64_t)data) & 7) == 0) {
        return string_key_hash_computation(data, length, seed, 1);
    }
    return string_key_hash_computation(data, length, seed, 0);
}

uint8_t HashIndexComputeFp(uint64_t hash) {
    uint8_t fp = 0;
    hash >>= 48;
    fp ^= hash;
    hash >>= 8;
    fp ^= hash;
    return fp;
}

uint64_t ConvertSlotTo64Bits(const Slot& slot) {
    uint64_t packed = 0;
    packed |= static_cast<uint64_t>(slot.fp) << 56;
    packed |= static_cast<uint64_t>(slot.len) << 48;

    uint64_t pointer_value = 0;
    memcpy(&pointer_value, slot.pointer, 6);
    packed |= pointer_value & 0xFFFFFFFFFFFF;  

    return packed;
}

uint64_t CRC64(const void* data, size_t length) {
    uint64_t crc = 0;
    const uint8_t* byte_data = static_cast<const uint8_t*>(data);
    for (size_t i = 0; i < length; ++i) {
        crc ^= static_cast<uint64_t>(byte_data[i]);
        for (int j = 0; j < 8; ++j) {
            if (crc & 1) {
                crc = (crc >> 1) ^ 0xC96C5795D7870F42ULL; // 这是一个典型的CRC64多项式
            } else {
                crc >>= 1;
            }
        }
    }
    return crc;
}


uint64_t SetChecksum(const void* key_data, size_t key_len, const void* value_data, size_t value_len) {
    uint64_t checksum;
    checksum = CRC64(key_data,key_len);
    checksum ^= CRC64(value_data,value_len);  // 将 key 和 value 的校验和合并
}


bool VerifyChecksum(const void* key_data, size_t key_len, const void* value_data, size_t value_len, uint64_t checksum) {
    uint64_t computed_checksum = CRC64(key_data, key_len);
    computed_checksum ^= CRC64(value_data, value_len);
    return computed_checksum == checksum;
}

void set_bit_atomic(uint64_t* bmap_addr, uint64_t add_value) {
    uint64_t old_val, new_val;

    do {
        old_val = *bmap_addr; // Load the current value at bmap_addr
        new_val = old_val | add_value; // Set the bit corresponding to add_value

        // Atomically compare and swap
        // If *bmap_addr == old_val, then set it to new_val
        // Otherwise, retry with the updated *bmap_addr
    } while (!std::atomic_compare_exchange_weak(
        reinterpret_cast<std::atomic<uint64_t>*>(bmap_addr), &old_val, new_val));
}

void reset_bit_atomic(uint64_t* bmap_addr, uint64_t add_value) {
    uint64_t old_val, new_val;
    uint64_t reset_mask = ~add_value;
    do {
        old_val = *bmap_addr; // Load the current value at bmap_addr
        new_val = old_val & add_value; // Set the bit corresponding to add_value

        // Atomically compare and swap
        // If *bmap_addr == old_val, then set it to new_val
        // Otherwise, retry with the updated *bmap_addr
    } while (!std::atomic_compare_exchange_weak(
        reinterpret_cast<std::atomic<uint64_t>*>(bmap_addr), &old_val, new_val));
}
