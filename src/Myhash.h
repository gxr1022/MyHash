// #include <verona.h>
#include <vector>
#include <string>
#include <optional>
#include <cmath>
#include <cstdint>


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


// #define ROOT_RES_LEN          (sizeof(RaceHashRoot))
#define HASH_ADDRESSABLE_BUCKET_NUM    (34000ULL)
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

struct KVblock
{
  uint8_t k_len;
  uint8_t v_len;
  uint64_t key;
  uint64_t value;
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
  // uint8_t lock; //?
  uint8_t local_depth;
  uint8_t pointer[6];
};

struct KVInfo {
    void   * key_addr;
    uint32_t key_len;
    uint32_t value_len;
};

typedef struct KVHashInfo {
    uint64_t hash_value;
    uint64_t prefix;
    uint8_t  fp;
    uint8_t  local_depth;
};

struct KVTableAddrInfo {
    uint64_t    f_bucket_addr;
    uint64_t    s_bucket_addr; // they are calculated by s_idx;

    uint32_t    f_main_idx;
    uint32_t    s_main_idx;

    uint32_t    f_idx;
    uint32_t    s_idx;
};

struct KVRWAddr {
    uint64_t kv_addr;
    uint32_t len;
};

struct KVLogHeader {
    uint16_t key_length;
    uint32_t value_length;
};

struct KVLogTail {
    uint8_t  crc;
};

struct HashRoot{
    uint64_t global_depth;
    uint64_t max_global_depth;
    uint64_t init_local_depth;

    uint64_t prefix_num;

    uint64_t subtable_init_num;
    // uint64_t subtable_hash_range;
    uint64_t subtable_bucket_num;
    uint64_t seed;

    // uint64_t root_offset;
    // uint64_t subtable_offset;
    uint64_t kv_offset; 
    uint64_t kv_len; 
    
    // uint64_t lock;
    Subtable subtable_entry[HASH_MAX_SUBTABLE_NUM];
};











void kv_put(const ExtendableHashTable& table, const std::string& key, const std::string& value)
{
  auto bucket_cown = table.get_bucket(key);
  verona::cpp::when(bucket_cown) << [key, value, &table](auto bucket) {
    // Check if the key already exists
    for (auto& entry : bucket->entries)
    {
      if (entry.first == key)
      {
        entry.second = value;
        return;
      }
    }

    // Insert new key-value pair
    if (bucket->has_space())
    {
      bucket->entries.push_back({key, value});
    }
    else
    {
      // Bucket is full, need to split and rehash
      table.split_bucket(table.get_directory_index(table.hash(key)));
      kv_put(table, key, value); // Retry insertion after split
    }
  };
}

void kv_get(const ExtendableHashTable& table, const std::string& key, const std::function<void(std::optional<std::string>)>& callback)
{
  auto bucket_cown = table.get_bucket(key);
  verona::cpp::when(bucket_cown) << [key, callback](auto bucket) {
    for (const auto& entry : bucket->entries)
    {
      if (entry.first == key)
      {
        callback(entry.second);
        return;
      }
    }
    callback(std::nullopt); // Key not found
  };
}

void kv_erase(const ExtendableHashTable& table, const std::string& key)
{
  auto bucket_cown = table.get_bucket(key);
  verona::cpp::when(bucket_cown) << [key](auto bucket) {
    auto it = std::remove_if(bucket->entries.begin(), bucket->entries.end(),
                             [&key](const std::pair<std::string, std::string>& entry) {
                               return entry.first == key;
                             });
    if (it != bucket->entries.end())
    {
      bucket->entries.erase(it, bucket->entries.end());
      std::cout << "Removed key: " << key << std::endl;
    }
    else
    {
      std::cout << "Key: " << key << " not found for removal" << std::endl;
    }
  };
}

static inline uint64_t HashIndexConvert40To64Bits(uint8_t * addr) {
    uint64_t ret = 0;
    return ret | ((uint64_t)addr[0] << 40) | ((uint64_t)addr[1] << 32)
        | ((uint64_t)addr[2] << 24) | ((uint64_t)addr[3] << 16) 
        | ((uint64_t)addr[4] << 8);
}

static inline void HashIndexConvert64To40Bits(uint64_t addr, uint8_t * o_addr) {
    o_addr[0] = (uint8_t)((addr >> 40) & 0xFF);
    o_addr[1] = (uint8_t)((addr >> 32) & 0xFF);
    o_addr[2] = (uint8_t)((addr >> 24) & 0xFF);
    o_addr[3] = (uint8_t)((addr >> 16) & 0xFF);
    o_addr[4] = (uint8_t)((addr >> 8)  & 0xFF);
}

static uint64_t string_key_hash_computation(const void * data, uint64_t length, 
        uint64_t seed, uint32_t align) {
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

    kv_addr->kv_addr = HashIndexConvert40To64Bits(slot->pointer);
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

bool CheckKey(void * r_key_addr, uint32_t r_key_len, void * l_key_addr, uint32_t l_key_len) {
