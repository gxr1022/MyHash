#include "mm.h"

MemoryPool::MemoryPool(size_t chunk_size, size_t num_chunks) : chunk_size_(chunk_size), num_chunks_(num_chunks) {
    
    mm_block_size_ = num_chunks_ * chunk_size_;
    cur_mm_block_idx_=0;

    // Calculate the number of chuncks occupied by bitmap
    bmap_block_num_ = num_chunks_ / 8 / chunk_size_;
    if (bmap_block_num_ * 8 * chunk_size_ < num_chunks_)
        bmap_block_num_ += 1;
    initial_alloc_KVblock(); // initialize KVblock space
    // initial_alloc_subtable(); //initialize subtables 
}

void MemoryPool::initial_alloc_KVblock()
{
    void* KVblock_init_addr=malloc(chunk_size_ * num_chunks_);
    
    MetaAddrInfo* meta_info = (MetaAddrInfo *)malloc(sizeof(MetaAddrInfo));
    meta_info->meta_info_type=TYPE_KVBLOCK;
    meta_info->addr=reinterpret_cast<uint64_t>(KVblock_init_addr);
    meta_info_.push_back(meta_info);

    init_KVblock_meta_info_ = meta_info;

    // store all KV subblock in initial KVblock.
    for (size_t i = bmap_block_num_; i < num_chunks_; ++i) {
        void* chunk = static_cast<char*>(KVblock_init_addr) + i * chunk_size_;
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
void MemoryPool::mm_free(Slot* slot) {
    // Slot slot = *(Slot *)&orig_slot_val;
    uint64_t kv_raddr = HashIndexConvert48To64Bits(slot->pointer); // get key-value pair address
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
    // free_faa_map_[addr_str] += add_value;   how to manage bitmap?
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
        memset(bucket, 0, sizeof(Bucket));
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








