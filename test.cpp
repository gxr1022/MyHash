#include <atomic>
#include <iostream>
#include <cstring>
#include <iomanip>

// 假设的 Slot 结构体，8字节大小
struct Slot {
    uint8_t fp;        // 1 byte
    uint8_t len;       // 1 byte
    uint8_t pointer[6]; // 6 bytes (48 bits)
}__attribute__((aligned(8)));


void print_slot_bytes(const Slot* slot) {
    // 将 Slot 结构体的内容视为 uint64_t
    uint64_t slot_value;
    std::memcpy(&slot_value, slot, sizeof(Slot));  // 将 Slot 转为 64 位整数

    // 打印 64 位整数的每个字节
    std::cout << "Slot contents (in hex): ";
    for (size_t i = 0; i < sizeof(slot_value); ++i) {
        std::cout << std::hex << std::setw(2) << std::setfill('0')
                  << static_cast<int>(reinterpret_cast<const uint8_t*>(&slot_value)[i]) << " ";
    }
    std::cout << std::dec << std::endl;  // 恢复到十进制输出
}

// 原子性地将 8 字节数据写入 Slot 指向的地址
void atomic_write_to_slot(Slot* target_slot, uint64_t new_value) {
    // 通过 reinterpret_cast 将 Slot* 转换为 std::atomic<uint64_t>*
    std::atomic<uint64_t>* atomic_slot = reinterpret_cast<std::atomic<uint64_t>*>(target_slot);

    // 使用 std::atomic 的 store 方法进行原子性写入
    atomic_slot->store(new_value, std::memory_order_relaxed);
}

int main() {
    // 假设我们有一个 Slot 结构体，位于某个内存地址
    Slot* slot_ptr = new Slot();  // 分配内存并初始化 Slot 结构体
    uint64_t new_value = 0xDEADBEEFCAFEBABE;  // 要写入的8字节值

    // 原子性地写入数据到 slot_ptr 指向的地址
    atomic_write_to_slot(slot_ptr, new_value);

    // 打印写入结果
    std::atomic<uint64_t>* atomic_slot = reinterpret_cast<std::atomic<uint64_t>*>(slot_ptr);
    uint64_t result = atomic_slot->load(std::memory_order_relaxed);
    std::cout << "Written value: 0x" << std::hex << result << std::endl;
    print_slot_bytes(slot_ptr);
    delete slot_ptr;  // 释放内存
    return 0;
}
