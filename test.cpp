#include <cstdint>
#include <iostream>
#include <cstring>  // 用于 memcpy

struct Slot {
    uint8_t fp;        // 1 byte
    uint8_t len;       // 1 byte
    uint8_t pointer[6]; // 6 bytes
};

// 判断当前系统的字节序（返回 true 表示小端字节序）
bool isLittleEndian() {
    int num = 1;
    return *(char *)&num == 1;
}

// 将 Slot 转换为 int64_t
int64_t SlotToInt64(const Slot &slot) {
    int64_t result = 0;

    if (isLittleEndian()) {
        result |= (static_cast<int64_t>(slot.fp) << 56);
        result |= (static_cast<int64_t>(slot.len) << 48);
        result |= (static_cast<int64_t>(slot.pointer[0]) << 40);
        result |= (static_cast<int64_t>(slot.pointer[1]) << 32);
        result |= (static_cast<int64_t>(slot.pointer[2]) << 24);
        result |= (static_cast<int64_t>(slot.pointer[3]) << 16);
        result |= (static_cast<int64_t>(slot.pointer[4]) << 8);
        result |= static_cast<int64_t>(slot.pointer[5]);
    } else {
        result |= (static_cast<int64_t>(slot.pointer[5]) << 56);
        result |= (static_cast<int64_t>(slot.pointer[4]) << 48);
        result |= (static_cast<int64_t>(slot.pointer[3]) << 40);
        result |= (static_cast<int64_t>(slot.pointer[2]) << 32);
        result |= (static_cast<int64_t>(slot.pointer[1]) << 24);
        result |= (static_cast<int64_t>(slot.pointer[0]) << 16);
        result |= (static_cast<int64_t>(slot.len) << 8);
        result |= static_cast<int64_t>(slot.fp);
    }

    return result;
}

int main() {
    // 初始化 Slot 结构体
    Slot slot = {0x7F, 0xAA, {0x01, 0x02, 0x03, 0x04, 0x05, 0x06}};

    // 将 Slot 结构体转换为 int64_t
    int64_t slot_as_int = SlotToInt64(slot);

    // 将 int64_t 写回 slot 的地址
    std::memcpy(&slot, &slot_as_int, sizeof(int64_t));

    // 打印 slot.fp 和 slot.len
    std::cout << "slot.fp: " << std::hex << static_cast<int>(slot.fp) << std::endl;
    std::cout << "slot.len: " << std::hex << static_cast<int>(slot.len) << std::endl;

    return 0;
}
