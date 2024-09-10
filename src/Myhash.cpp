#include <vector>
#include <string>
#include <optional>
#include <cmath>
#include <cstdint>
#include "Myhash.h"
#include "mm.h"

Myhash::Myhash(size_t chunk_size, size_t num_chunks) {
    mm_ = new MemoryPool(chunk_size , chunk_size);
    init_hash_table();
}

void Myhash::init_hash_table() {
    init_root();
    init_subtable();
    return;
}

void Myhash::init_root()
{
    // initialize the root_ of hashtable
    root_ = (HashRoot *)malloc(sizeof(HashRoot));
    if(root_==NULL)
    {
        // to do
        return;
    }
    root_->global_depth = HASH_GLOBAL_DEPTH;
    root_->max_global_depth = HASH_MAX_GLOBAL_DEPTH;
    root_->init_local_depth = HASH_INIT_LOCAL_DEPTH;

    root_->prefix_num = 1 << HASH_MAX_GLOBAL_DEPTH;

    root_->subtable_init_num = HASH_INIT_SUBTABLE_NUM;
    root_->subtable_hash_range = HASH_ADDRESSABLE_BUCKET_NUM;
    root_->subtable_bucket_num = HASH_SUBTABLE_BUCKET_NUM;
    root_->seed = rand();

    root_->kv_offset = mm_->init_KVblock_meta_info_->addr; 

    return;
}

void Myhash::init_subtable()
{
    for (int i = 0; i < HASH_INIT_SUBTABLE_NUM; i ++) {
        for (int j = 0; j < HASH_SUBTABLE_NUM / HASH_INIT_SUBTABLE_NUM; j ++) {
            uint64_t subtable_idx = j * HASH_INIT_SUBTABLE_NUM + i;
            MMAllocSubtableCtx *subtable_info = (MMAllocSubtableCtx *)malloc(sizeof(MMAllocSubtableCtx));
            subtable_info->subtable_idx = subtable_idx;

            mm_->mm_alloc_subtable(subtable_info);

            root_->subtable_entry[subtable_idx].local_depth = HASH_INIT_LOCAL_DEPTH;
            HashIndexConvert64To48Bits(subtable_info->addr, root_->subtable_entry[subtable_idx].pointer);
        }   
    }
    return;
}

void * Myhash::kv_search(KVInfo * kv_info)
{
    KVReqCtx ctx;
    memset(&ctx, 0, sizeof(KVReqCtx));

    ctx.req_type = KV_REQ_SEARCH;
    ctx.kv_info = kv_info;

    get_kv_hash_info(&ctx.kv_info, &ctx.hash_info);
    get_kv_addr_info(&ctx.hash_info, &ctx.tbl_addr_info);
    find_kv_in_buckets(ctx);
    //need to check CRC.

    return;

}

void Myhash::find_kv_in_buckets(KVReqCtx * ctx) {
    get_com_bucket_info(ctx);

    // search all kv pair that finger print matches, myhash use two hash function to solve hash collision, use one overﬂow bucket to store overflow kv pairs.
    for (int i = 0; i < 4; i ++) {
        for (int j = 0; j < HASH_ASSOC_NUM; j ++) { // every bucket has HASH_ASSOC_NUM slot
            if (ctx->slot_arr[i][j].fp == ctx->hash_info.fp && ctx->slot_arr[i][j].len != 0) {
                 
                KVRWAddr cur_kv_addr;
                cur_kv_addr.addr = HashIndexConvert48To64Bits(ctx->slot_arr[i][j].pointer);
                cur_kv_addr.len    = ctx->slot_arr[i][j].len * mm_->subblock_sz_;
                // get KV pair   |key_len|V_len|Key|Value|
            }
        }
    }
}

void Myhash::kv_update(KVInfo * kv_info)
{
    KVReqCtx ctx;
    memset(&ctx, 0, sizeof(KVReqCtx));
    ctx.req_type = KV_REQ_UPDATE;
    ctx.kv_info = kv_info;

    // Allocate KV block memory, sizeof(k_len+v_len)=6, sizeof(crc)=1
    uint32_t kv_block_size = 6 + ctx->kv_info.key_len + ctx->kv_info.value_len + 1;
    mm_->mm_alloc_subblock(kv_block_size, &ctx->mm_alloc_ctx); 

    //acquire the address of KVblock and write key-value pair
    uint64_t KVblock_addr = ctx->mm_alloc_ctx.addr;
    memcpy(KVblock_addr, ctx->key_len, 2);
    memcpy(KVblock_addr+2, ctx->value_len, 4);
    memcpy(KVblock_addr + 6, ctx->key, ctx->key_len);
    memcpy(KVblock_addr + 6 + ctx->key_len, ctx->value,ctx->value_len);

    get_kv_hash_info(&ctx.kv_info, &ctx.hash_info);
    get_kv_addr_info(&ctx.hash_info, &ctx.tbl_addr_info);

    Slot* target_slot;
    find_slot_in_buckets(ctx,target_slot);
    if(target_slot!=NULL)
    {
        fill_slot(&ctx->mm_alloc_ctx, ctx->hash_info, target_slot);
    }
    
    return;
}

void Myhash::find_slot_in_buckets(KVReqCtx * ctx, Slot* target_slot)
{
    get_com_bucket_info(ctx);

    // search all kv pair that finger print matches, myhash use two hash function to solve hash collision, use one overﬂow bucket to store overflow kv pairs.
    for (int i = 0; i < 4; i ++) {
        for (int j = 0; j < HASH_ASSOC_NUM; j ++) { // every bucket has HASH_ASSOC_NUM slot
            if (ctx->slot_arr[i][j].fp == ctx->hash_info.fp && ctx->slot_arr[i][j].len != 0) {
                 target_slot = ctx->slot_arr[i][j];
                 return;
            }
        }
    }
}

void Myhash::kv_insert(KVInfo * kv_info)
{
    KVReqCtx ctx;
    memset(&ctx, 0, sizeof(KVReqCtx));

    ctx.req_type = KV_REQ_INSERT;
    ctx.kv_info = kv_info;

    get_kv_hash_info(&ctx.kv_info, &ctx.hash_info);
    get_kv_addr_info(&ctx.hash_info, &ctx.tbl_addr_info);

    //After get the bucket address of KV pair, we need to find an empty slot to store the KV pair.
    kv_insert_read_buckets_and_write_kv(&ctx); // BUG, there is no info about key and value .
    return;
}

void Myhash::kv_insert_read_buckets_and_write_kv(KVReqCtx * ctx) {

    // Allocate KV block memory, sizeof(k_len+v_len)=6, sizeof(crc)=1
    uint32_t kv_block_size = 6 + ctx->kv_info.key_len + ctx->kv_info.value_len + 1;
    mm_->mm_alloc_subblock(kv_block_size, &ctx->mm_alloc_ctx); 

    //acquire the address of KVblock and write key-value pair
    uint64_t KVblock_addr = ctx->mm_alloc_ctx.addr;
    memcpy(KVblock_addr, ctx->key_len, 2);
    memcpy(KVblock_addr+2, ctx->value_len, 4);
    memcpy(KVblock_addr + 6, ctx->key, ctx->key_len);
    memcpy(KVblock_addr + 6 + ctx->key_len, ctx->value,ctx->value_len);

    //find an empty slot ……
    find_empty_slot(ctx);
    if (ctx->bucket_idx == -1) {
        ctx->is_finished = true;
        mm_->mm_free_cur(&ctx->mm_alloc_ctx);
        // I think we need to resize hash table. Because all candidate buckets are filled. Or when pass the load factor?
        // ……
        return;
    }

    // Determine the target slot and fill it.
    if (ctx->bucket_idx < 2) {
        Slot * target_slot = ctx->f_com_bucket[ctx->bucket_idx].slots[ctx->slot_idx];
    } else {
        Slot * target_slot = ctx->s_com_bucket[ctx->bucket_idx - 2].slots[ctx->slot_idx];
    }
    fill_slot(&ctx->mm_alloc_ctx, ctx->hash_info, target_slot);

    return;
}

void Myhash::kv_delete(KVInfo * kv_info)
{
    KVReqCtx ctx;
    memset(&ctx, 0, sizeof(KVReqCtx));

    ctx.req_type = KV_REQ_DELETE;
    ctx.kv_info = kv_info;

    get_kv_hash_info(&ctx.kv_info, &ctx.hash_info);
    get_kv_addr_info(&ctx.hash_info, &ctx.tbl_addr_info);

    Slot* target_slot;
    find_slot_in_buckets(ctx,target_slot);

    // Release memory of KV block;
    mm_free(target_slot); 

    //Set target_slot to 0;
    memset(target_slot, 0, sizeof(Slot));

}

void Myhash::fill_slot(MMAllocCtx * mm_alloc_ctx, KVHashInfo * a_kv_hash_info, Slot * local_slot) {
    local_slot->fp = a_kv_hash_info->fp;
    local_slot->kv_len = mm_alloc_ctx->num_subblocks; // every subblock has 64 bytes, and kv_len represents the total length of KV block.
    HashIndexConvert64To48Bits(mm_alloc_ctx->addr, local_slot->pointer);
}

void Myhash::find_empty_slot(KVReqCtx * ctx) 
{
    get_local_bucket_info(ctx); // why here? 

    uint32_t f_main_idx = ctx->tbl_addr_info.f_main_idx;
    uint32_t s_main_idx = ctx->tbl_addr_info.s_main_idx;
    uint32_t f_free_num, s_free_num;
    uint32_t f_free_slot_idx, s_free_slot_idx;

    int32_t bucket_idx = -1;
    int32_t slot_idx = -1;

    for (int i = 0; i < 2; i ++) 
    {
        f_free_num = GetFreeSlotNum(ctx->f_com_bucket + f_main_idx, &f_free_slot_idx);
        s_free_num = GetFreeSlotNum(ctx->s_com_bucket + s_main_idx, &s_free_slot_idx);

        if (f_free_num > 0 || s_free_num > 0) {
            if (f_free_num >= s_free_num) {
                bucket_idx = f_main_idx;
                slot_idx = f_free_slot_idx;
            } else {
                // Help to differentiate the first com bucket and the second com bucket.
                bucket_idx = 2 + s_main_idx;
                slot_idx = s_free_slot_idx;
            }
        }
        f_main_idx = (f_main_idx + 1) % 2;
        s_main_idx = (s_main_idx + 1) % 2;
    }

    ctx->bucket_idx = bucket_idx;
    ctx->slot_idx   = slot_idx;

}

//class KVInfo represents KV information offseted by Myhash/test. 
//class KVHashInfo represents some fields which are stored in hash table.
//The function get local_depth, fp, hash_value and subtable address of a specified key.
void Myhash::get_kv_hash_info(KVInfo * a_kv_info, KVHashInfo * a_kv_hash_info) 
{
    uint64_t key_addr = (uint64_t)a_kv_info->key_addr;
    a_kv_hash_info->hash_value = VariableLengthHash((void *)key_addr, a_kv_info->key_len, 0);
    // prefix is used to find the target subtable.
    a_kv_hash_info->prefix = (a_kv_hash_info->hash_value >> SUBTABLE_USED_HASH_BIT_NUM) & HASH_MASK(race_root_->global_depth);
    a_kv_hash_info->local_depth = root_->subtable_entry[a_kv_hash_info->prefix].h.local_depth;
    a_kv_hash_info->fp = HashIndexComputeFp(a_kv_hash_info->hash_value);
}

// class KVTableAddrInfo represents detailed bucket address of KV pair in hashtable.
// The function gets bucket address of a specified key.
void Myhash::get_kv_addr_info(KVHashInfo * a_kv_hash_info, KVTableAddrInfo * a_kv_addr_info) {
    uint64_t hash_value = a_kv_hash_info->hash_value;
    uint64_t prefix     = a_kv_hash_info->prefix;

    uint64_t f_index_value = SubtableFirstIndex(hash_value, race_root_->subtable_hash_range);
    uint64_t s_index_value = SubtableSecondIndex(hash_value, f_index_value, race_root_->subtable_hash_range);
    uint64_t f_idx, s_idx;

    if (f_index_value % 2 == 0) 
        f_idx = f_index_value / 2 * 3;
    else
        f_idx = f_index_value / 2 * 3 + 1;
    
    if (s_index_value % 2 == 0)
        s_idx = s_index_value / 2 * 3;
    else 
        s_idx = s_index_value / 2 * 3 + 1;
    
    uint64_t subtable_offset;
    subtable_offset = HashIndexConvert48To64Bits(root_->subtable_entry[prefix].pointer);

    // get combined bucket offset
    a_kv_addr_info->f_main_idx = f_index_value % 2;
    a_kv_addr_info->s_main_idx = s_index_value % 2;
    a_kv_addr_info->f_idx = f_idx;
    a_kv_addr_info->s_idx = s_idx;

    a_kv_addr_info->f_bucket_addr = subtable_offset + f_idx * sizeof(Bucket);
    a_kv_addr_info->s_bucket_addr  = subtable_offset + s_idx * sizeof(Bucket);

    return;
    
}

void Myhash::get_com_bucket_info(KVReqCtx * ctx) {
    //The first combination buckets and the second combination buckets
    //Assumping that local_bucket_addr is the start address of one combination bucket. 
    //The second combination is the next to the first combination.
    ctx->f_com_bucket = reinterpret_cast<Bucket*>(ctx->tbl_addr_info.f_bucket_addr);
    ctx->s_com_bucket = reinterpret_cast<Bucket*>(ctx->tbl_addr_info.s_bucket_addr);

    ctx->slot_arr[0] = ctx->f_com_bucket[0].slots;
    ctx->slot_arr[1] = ctx->f_com_bucket[1].slots;
    ctx->slot_arr[2] = ctx->s_com_bucket[0].slots;
    ctx->slot_arr[3] = ctx->s_com_bucket[1].slots;

}
