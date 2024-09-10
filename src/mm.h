#include <vector>
#include <string>
#include <optional>
#include <cmath>
#include <cstdint>
#include <cassert>
#include "Myhash.h"
#include <iostream>
#include <cassert>
#include <deque>
#include <assert.h>

//Different MMBlock represents Memory Zone with different type of data, such as KV block and subtable.
// struct MMBlock {
//     bool * bmap;
//     uint32_t num_allocated;
//     int32_t  prev_free_subblock_idx;
//     int32_t  next_free_subblock_idx;
//     int32_t  next_free_subblock_cnt;
//     uint64_t next_mmblock_addr;
// };

enum AllocType {
    TYPE_SUBTABLE = 1,
    TYPE_KVBLOCK  = 2,
};

struct MMAllocSubtableCtx {
    uint64_t addr;
    uint64_t subtable_idx;
};

struct MMAllocCtx {
    uint64_t addr;
    uint64_t prev_addr;
    uint64_t next_addr;
    uint32_t num_subblocks;
    bool     need_change_prev;
};

struct Chunk {
    uint64_t addr;   
};

struct MetaAddrInfo {
    uint8_t  meta_info_type;
    uint64_t addr;
};

class MemoryPool {
private:
    std::vector<MetaAddrInfo*> meta_info_; 
    size_t chunk_size_;  
    size_t num_chunks_; 
    uint64_t mm_block_size_; 

    uint32_t bmap_block_num_;
    uint32_t cur_mm_block_idx_;

    // std::vector<MMBlock *> mm_blocks_;
    
    std::deque<Chunk> subblock_free_queue_;
    Chunk last_allocated_info_;

public:
    MemoryPool(size_t chunk_size, size_t num_chunks);

    void initial_alloc_KVblock();
    // void initial_alloc_subtable();

    void mm_alloc_subblock(size_t size, MMAllocCtx * ctx);
    void mm_free(uint64_t orig_slot_val);
    void mm_free_cur(const MMAllocCtx * ctx);

    void mm_alloc_subtable(MMAllocSubtableCtx * ctx);
    
    ~MemoryPool() {
        for(int i=0;i<meta_info_.size();i++)
        {
            free(void*(meta_info_[i]->addr));
        }
    }

    inline size_t get_aligned_size(size_t size) {
        if ((size % chunk_size_) == 0) {
            return size;
        }
        size_t aligned = ((size / chunk_size_) + 1) * chunk_size_;
        return aligned;
    }


};

MemoryPool::MemoryPool(size_t chunk_size, size_t num_chunks) : chunk_size_(chunk_size), num_chunks_(num_chunks) {
    cur_mm_block_idx_=0;
    mm_block_size_ = num_chunks_ * chunk_size_;

    // Calculate the number of chuncks occupied by bitmap
    bmap_block_num_ = num_chunks_ / 8 / chunk_size_;
    if (bmap_block_num_ * 8 * chunk_size_ < num_chunks_)
        bmap_block_num_ += 1;
    initial_alloc_KVblock(); // initialize KV space
    // initial_alloc_subtable(); //initialize subtables
}

void MemoryPool::initial_alloc_KVblock()
{
    void* start_addr=malloc(chunk_size_ * num_chunks_);
    
    MetaAddrInfo* meta_info = (MetaAddrInfo *)malloc(sizeof(MetaAddrInfo));
    meta_info->meta_info_type=TYPE_KVBLOCK;
    meta_info->addr=reinterpret_cast<uint64_t>(start_addr);
    meta_info_.push_back(meta_info);


    for (size_t i = bmap_block_num_; i < num_chunks_; ++i) {
        void* chunk = static_cast<char*>(start_addr) + i * chunk_size_;
        Chunk subblock;
        subblock.addr= reinterpret_cast<uint64_t>(chunk);
        subblock_free_queue_.push_back(subblock);
    }
    return;
}


// allocate subblock for new KV pair if raw subblock doesn't have enough space.
void MemoryPool::mm_alloc_subblock(size_t size, MMAllocCtx * ctx) {

    int ret = 0;
    size_t aligned_size = get_aligned_size(size);
    int num_subblocks_required = aligned_size / chunk_size_;
    assert(num_subblocks_required == 1); // only allocate one subblock

    assert(subblock_free_queue_.size() > 0);
    Chunk alloc_subblock = subblock_free_queue_.front();
    subblock_free_queue_.pop_front();

    if (subblock_free_queue_.size() == 0) {
        //Dynamically allocate new memory pool, to be done……
        return;
    }

    Chunk next_subblock = subblock_free_queue_.front();
    
    ctx->need_change_prev = false;
    ctx->num_subblocks = num_subblocks_required;
    ctx->addr = alloc_subblock.addr;
    ctx->next_addr = next_subblock.addr;
    ctx->prev_addr = last_allocated_info_.addr;
    last_allocated_info_ = alloc_subblock;
}

// release a KV pair 
void MemoryPool::mm_free(uint64_t orig_slot_val) {
    Slot slot = *(Slot *)&orig_slot_val;
    uint64_t kv_raddr = HashIndexConvert40To64Bits(slot.pointer); // get key-value pair address
    uint32_t subblock_id = (kv_raddr % (chunk_size_ * num_chunks_)) / chunk_size_;
    
    // update bitmap, set subblock to 1;
    uint64_t block_raddr = kv_raddr - (kv_raddr % mm_block_size_);
    uint32_t subblock_8byte_offset = subblock_id / 64;
    uint64_t bmap_addr = block_raddr + subblock_8byte_offset * sizeof(uint64_t); //get the conresponding address in bitmap
    uint64_t add_value = 1 << (subblock_id % 64);
    if (bmap_addr > block_raddr + chunk_size_ * bmap_block_num_) {
        printf("Error free!\n");
        exit(1);
    }

    char tmp[256] = {0};
    std::string addr_str(tmp);
    free_faa_map_[addr_str] += add_value;
}

void MemoryPool::mm_free_cur(const MMAllocCtx * ctx) {
    // 1. reconstruct the last_allocated
    Chunk last_allocated;
    memset(&last_allocated, 0, sizeof(Chunk));
    last_allocated.addr = ctx->addr;

    subblock_free_queue_.push_front(last_allocated);

    // 2. reconstruct last allocated to be last-last allocated
    Chunk last_last_allocated;
    memset(&last_last_allocated, 0, sizeof(Chunk));
    last_last_allocated.addr = ((ctx->prev_addr >> 8) << 8);
    last_allocated_info_ = last_last_allocated;
    return;
}


void MemoryPool::mm_alloc_subtable(MMAllocSubtableCtx *ctx) {
    
    // acquire the address of subtable
    ctx->addr =reinterpret_cast<uint64_t>(malloc(SUBTABLE_LEN));
    
    // initialize buckets for subtable
    for (int j = 0; j < HASH_ADDRESSABLE_BUCKET_NUM; j ++) {
        Bucket * bucket = (Bucket *)ctx->addr + j;
        bucket->h.local_depth = HASH_INIT_LOCAL_DEPTH;
        bucket->h.prefix = ctx->subtable_idx;
    }

    // record the Memory block information in meta_info
    MetaAddrInfo* meta_info = (MetaAddrInfo *)malloc(sizeof(MetaAddrInfo));
    meta_info->meta_info_type=TYPE_SUBTABLE;
    meta_info->addr=ctx->addr;
    meta_info_.push_back(meta_info);

    return;
}








