#include <iostream>
#include <cstdint>

struct Slot
{
  uint8_t fp;
  uint8_t len;
  uint8_t pointer[6];
}; 

uint64_t combineSlot(const Slot &slot) {
    uint64_t atomic_slot_val = 0;

    // Combine pointer array into atomic_slot_val by shifting each byte into place
    atomic_slot_val |= (static_cast<uint64_t>(slot.pointer[5]) << 56);
    atomic_slot_val |= (static_cast<uint64_t>(slot.pointer[4]) << 48);
    atomic_slot_val |= (static_cast<uint64_t>(slot.pointer[3]) << 40);
    atomic_slot_val |= (static_cast<uint64_t>(slot.pointer[2]) << 32);
    atomic_slot_val |= (static_cast<uint64_t>(slot.pointer[1]) << 24);
    atomic_slot_val |= (static_cast<uint64_t>(slot.pointer[0]) << 16);
    
    // Add len into the 7th byte
    atomic_slot_val |= (static_cast<uint64_t>(slot.len) << 8);
    
    // Add fp into the lowest byte
    atomic_slot_val |= static_cast<uint64_t>(slot.fp);

    return atomic_slot_val;
}

int main() {
    // Example Slot struct values
    Slot slot = {74, 1, {0x10, 0xA0, 0x7F, 0x03, 0xFF, 0x7F}};

    uint64_t result = combineSlot(slot);

    std::cout << "Combined 64-bit value: 0x" << result << std::endl;
    return 0;
}
