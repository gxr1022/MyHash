#pragma once
#include <vector>
#include <string>
#include <optional>
#include <cmath>
#include <cstdint>

inline uint64_t CRC64(const void* data, size_t length) {
    uint64_t crc = 0;
    const uint8_t* byte_data = static_cast<const uint8_t*>(data);
    for (size_t i = 0; i < length; ++i) {
        crc ^= static_cast<uint64_t>(byte_data[i]);
        for (int j = 0; j < 8; ++j) {
            if (crc & 1) {
                crc = (crc >> 1) ^ 0xC96C5795D7870F42ULL; 
            } else {
                crc >>= 1;
            }
        }
    }
    return crc;
}


inline uint64_t SetChecksum(const void* key_data, size_t key_len, const void* value_data, size_t value_len) {
    uint64_t checksum;
    checksum = CRC64(key_data,key_len);
    checksum ^= CRC64(value_data,value_len);  
    return checksum;
}


inline bool VerifyChecksum(const void* key_data, size_t key_len, const void* value_data, size_t value_len, uint64_t checksum) {
    uint64_t computed_checksum = CRC64(key_data, key_len);
    computed_checksum ^= CRC64(value_data, value_len);
    return computed_checksum == checksum;
}