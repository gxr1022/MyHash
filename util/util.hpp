#pragma once
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

struct U64_INT {
    uint64_t v;
};

struct U32_INT {
    uint32_t v;
};

static inline uint64_t hash_read64_align(const void * ptr, uint32_t align) {
    if (align == 0) {
        return TO64(ptr);
    }
    return *(uint64_t *)ptr;
}

static inline uint32_t hash_read32_align(const void * ptr, uint32_t align) {
    if (align == 0) {
        return TO32(ptr);
    }
    return *(uint32_t *)ptr;
}

inline uint64_t HashIndexConvert40To64Bits(uint8_t * addr) {
    uint64_t ret = 0;
    return ret | ((uint64_t)addr[0] << 40) | ((uint64_t)addr[1] << 32)
        | ((uint64_t)addr[2] << 24) | ((uint64_t)addr[3] << 16) 
        | ((uint64_t)addr[4] << 8) | ((uint64_t)addr[5]);
}


inline uint64_t HashIndexConvert48To64Bits(uint8_t * addr) {
    uint64_t ret = 0;
    return ret | ((uint64_t)addr[0] << 40)  // addr[0] 应该放在最高位
               | ((uint64_t)addr[1] << 32)
               | ((uint64_t)addr[2] << 24)
               | ((uint64_t)addr[3] << 16)
               | ((uint64_t)addr[4] << 8)
               | ((uint64_t)addr[5]);
}


inline void HashIndexConvert64To48Bits(uint64_t addr, uint8_t * o_addr) {
    o_addr[0] = (uint8_t)((addr >> 40) & 0xFF);
    o_addr[1] = (uint8_t)((addr >> 32) & 0xFF);
    o_addr[2] = (uint8_t)((addr >> 24) & 0xFF);
    o_addr[3] = (uint8_t)((addr >> 16) & 0xFF);
    o_addr[4] = (uint8_t)((addr >> 8) & 0xFF);
    o_addr[5] = (uint8_t)(addr & 0xFF);
}

inline bool isLittleEndian() {
    int num = 1;
    return *(char *)&num == 1;
}

// inline void HashIndexConvert64To48Bits(uint64_t addr, uint8_t *o_addr) {
//     o_addr[0] = (uint8_t)((addr >> 40) & 0xFF);
//     o_addr[1] = (uint8_t)((addr >> 32) & 0xFF);
//     o_addr[2] = (uint8_t)((addr >> 24) & 0xFF);
//     o_addr[3] = (uint8_t)((addr >> 16) & 0xFF);
//     o_addr[4] = (uint8_t)((addr >> 8)  & 0xFF);
//     o_addr[5] = (uint8_t)(addr & 0xFF);
// }



inline uint64_t string_key_hash_computation(const void * data, uint64_t length, uint64_t seed, uint32_t align) {
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

inline uint64_t SubtableFirstIndex(uint64_t hash_value, uint64_t capacity) {
    return hash_value % (capacity / 2);
}

inline uint64_t SubtableSecondIndex(uint64_t hash_value, uint64_t f_index, uint64_t capacity) {
    uint32_t hash = hash_value;
    uint16_t partial = (uint16_t)(hash >> 16);
    uint16_t non_sero_tag = (partial >> 1 << 1) + 1;
    uint64_t hash_of_tag = (uint64_t)(non_sero_tag * 0xc6a4a7935bd1e995);
    return (uint64_t)(((uint64_t)(f_index) ^ hash_of_tag) % (capacity / 2) + capacity / 2);
}

inline bool IsEmptyPointer(uint8_t * pointer, uint32_t num) {
    for (int i = 0; i < num; i ++) {
        if (pointer[i] != 0) {
            return false;
        }
    }
    return true;
}

inline uint8_t HashIndexComputeFp(uint64_t hash) {
    uint8_t fp = 0;
    hash >>= 48;
    fp ^= hash;
    hash >>= 8;
    fp ^= hash;
    return fp;
}

inline uint64_t VariableLengthHash(const void * data, uint64_t length, uint64_t seed) {
    if ((((uint64_t)data) & 7) == 0) {
        return string_key_hash_computation(data, length, seed, 1);
    }
    return string_key_hash_computation(data, length, seed, 0);
}