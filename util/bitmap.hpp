#pragma once
#include <vector>
#include <string>
#include <optional>
#include <cmath>
#include <atomic>
#include <cstdint>

inline void set_bit_atomic(uint64_t* bmap_addr, uint64_t add_value) {
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

inline void reset_bit_atomic(uint64_t* bmap_addr, uint64_t add_value) {
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
