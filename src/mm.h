#include <vector>
#include <string>
#include <optional>
#include <cmath>
#include <cstdint>
#include <cassert>
#include <iostream>
#include <cassert>
#include <deque>
#include <assert.h>
#include <string.h>
#include "Myhash.h"

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
    size_t chunk_size_;  
    size_t num_chunks_; 
    uint64_t mm_block_size_; 
    uint32_t cur_mm_block_idx_;
    uint32_t bmap_block_num_;
    
    std::deque<Chunk> subblock_free_queue_;
    Chunk last_allocated_info_;

public:
    std::vector<MetaAddrInfo*> meta_info_; 

    MetaAddrInfo * init_KVblock_meta_info_;
    MetaAddrInfo * init_subtable_meta_info_;

    MemoryPool(size_t chunk_size, size_t num_chunks);

    void initial_alloc_KVblock();
    // void initial_alloc_subtable();

    void mm_alloc_subblock(size_t size, MMAllocCtx * ctx);
    void mm_free(Slot* target_slot);
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
