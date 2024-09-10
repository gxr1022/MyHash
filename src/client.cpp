#include <vector>
#include <string>
#include <optional>
#include <cmath>
#include <cstdint>
#include "Myhash.h"
#include "client.h"
#include "mm.h"

Client::Client() {
    // create mm
    mm_ = new MemoryPool(1024,1024);
    init_hashtable();


}

void Client::init_root()
{
    // initialize the root of hashtable
    root_ = (HashRoot *)malloc(sizeof(HashRoot));
    if(root_==NULL)
    {
        // to do
        return;
    }
    root->global_depth = HASH_GLOBAL_DEPTH;
    root->init_local_depth = HASH_INIT_LOCAL_DEPTH;
    root->max_global_depth = HASH_MAX_GLOBAL_DEPTH;
    root->prefix_num = 1 << HASH_MAX_GLOBAL_DEPTH;
    root->subtable_init_num = HASH_INIT_SUBTABLE_NUM;
    root->subtable_hash_range = HASH_ADDRESSABLE_BUCKET_NUM;
    root->subtable_bucket_num = HASH_SUBTABLE_BUCKET_NUM;
    root->seed = rand();
    return;
}

void Client::init_subtable()
{
    for (int i = 0; i < HASH_INIT_SUBTABLE_NUM; i ++) {
        for (int j = 0; j < HASH_SUBTABLE_NUM / HASH_INIT_SUBTABLE_NUM; j ++) {
            uint64_t subtable_idx = j * HASH_INIT_SUBTABLE_NUM + i;
            MMAllocSubtableCtx *subtable_info = (MMAllocSubtableCtx *)malloc(sizeof(MMAllocSubtableCtx));
            subtable_info->subtable_idx = subtable_idx;
            mm_->mm_alloc_subtable(subtable_info);
            root_->subtable_entry[subtable_idx].local_depth = HASH_INIT_LOCAL_DEPTH;
            HashIndexConvert64To40Bits(subtable_info->addr, root_->subtable_entry[subtable_idx].pointer);
        }   
    }
    return;
}

int Client::init_hash_table() {
    init_root();
    init_subtable();
    return 0;
}

//class KVInfo represents KV information offseted by client/test. 
//class KVHashInfo represents some fields which are stored in hash table.
//The function get local_depth, fp, hash_value and subtable address of a specified key.
void Client::get_kv_hash_info(KVInfo * a_kv_info, KVHashInfo * a_kv_hash_info) 
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
void Client::get_kv_addr_info(KVHashInfo * a_kv_hash_info, KVTableAddrInfo * a_kv_addr_info) {
    uint64_t hash_value = a_kv_hash_info->hash_value;
    uint64_t prefix     = a_kv_hash_info->prefix;

    uint8_t  pr_server_id = race_root_->subtable_entry[prefix][0].server_id;
    // uint64_t r_subtable_off = HashIndexConvert40To64Bits(race_root_->subtable_entry[prefix][0].pointer);
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
    subtable_offset = HashIndexConvert40To64Bits(root_->subtable_entry[prefix].pointer);

    // get combined bucket offset
    a_kv_addr_info->f_main_idx = f_index_value % 2;
    a_kv_addr_info->s_main_idx = s_index_value % 2;
    a_kv_addr_info->f_idx = f_idx;
    a_kv_addr_info->s_idx = s_idx;

    a_kv_addr_info->f_bucket_addr = subtable_offset + f_idx * sizeof(Bucket);
    a_kv_addr_info->s_bucket_addr  = subtable_offset + s_idx * sizeof(Bucket);

    return;
    
}


// But how to find slot_array? which four buckets? I need to know

void Client::find_kv_in_buckets(KVReqCtx * ctx) {
    get_local_bucket_info(ctx);

    uint64_t local_kv_buf_addr = (uint64_t)ctx->local_kv_addr;
    ctx->kv_read_addr_list.clear();
    ctx->kv_idx_list.clear();

    // search all kv pair that finger print matches, myhash use two hash function to solve hash collision, use one overﬂow bucket to store overflow kv pairs.
    for (int i = 0; i < 4; i ++) {
        for (int j = 0; j < HASH_ASSOC_NUM; j ++) { // every bucket has HASH_ASSOC_NUM slot
            if (ctx->slot_arr[i][j].fp == ctx->hash_info.fp && ctx->slot_arr[i][j].len != 0) {
                
                
                
                // push the offset to the lists
                KVRWAddr cur_kv_addr;
                cur_kv_addr.r_kv_addr = HashIndexConvert40To64Bits(ctx->slot_arr[i][j].pointer);
                cur_kv_addr.rkey      = server_mr_info_map_[ctx->slot_arr[i][j].server_id]->rkey;
                cur_kv_addr.l_kv_addr = local_kv_buf_addr;
                cur_kv_addr.lkey      = local_buf_mr_->lkey;
                cur_kv_addr.length    = ctx->slot_arr[i][j].kv_len * mm_->subblock_sz_;
                cur_kv_addr.server_id = ctx->slot_arr[i][j].server_id;

                // print_log(DEBUG, "\t      [%s %lld]  find (%d, %d) raddr(%lx) rkey(%x)", __FUNCTION__, boost::this_fiber::get_id(),
                //     i, j, cur_kv_addr.r_kv_addr, cur_kv_addr.rkey);

                ctx->kv_read_addr_list.push_back(cur_kv_addr);
                ctx->kv_idx_list.push_back(std::make_pair(i, j));
                local_kv_buf_addr += cur_kv_addr.length;
            }
        }
    }
}

void Client::get_local_bucket_info(KVReqCtx * ctx) {
    //The first combination buckets and the second combination buckets
    //Assumping that local_bucket_addr is the start address of one combination bucket. 
    //The second combination is the next to the first combination.
    ctx->f_com_bucket = ctx->local_bucket_addr;
    ctx->s_com_bucket = ctx->local_bucket_addr + 2;

    ctx->bucket_arr[0] = ctx->f_com_bucket;
    ctx->bucket_arr[1] = ctx->f_com_bucket + 1;
    ctx->bucket_arr[2] = ctx->s_com_bucket;
    ctx->bucket_arr[3] = ctx->s_com_bucket + 1;

    ctx->slot_arr[0] = ctx->f_com_bucket[0].slots;
    ctx->slot_arr[1] = ctx->f_com_bucket[1].slots;
    ctx->slot_arr[2] = ctx->s_com_bucket[0].slots;
    ctx->slot_arr[3] = ctx->s_com_bucket[1].slots;

}

void Client::kv_insert(KVInfo * kv_info)
{
    KVReqCtx ctx;
    memset(&ctx, 0, sizeof(KVReqCtx));

    ctx.req_type = KV_REQ_INSERT;
    ctx.kv_info = kv_info;

    get_kv_hash_info(&ctx.kv_info, &ctx.hash_info);
    get_kv_addr_info(&ctx.hash_info, &ctx.tbl_addr_info);

    //After get the bucket address of KV pair, we need to find an empty slot to store the KV pair.

}

void Client::kv_insert_read_buckets_and_write_kv(KVReqCtx * ctx) {
    int ret = 0;

    update_log_tail(tail, &ctx->mm_alloc_ctx);
    prepare_log_commit_addrs(ctx);

    // Allocate KV block memory, sizeof(k_len+v_len)=6, sizeof(crc)=1
    uint32_t kv_block_size = 5 + ctx->kv_info.key_len + ctx->kv_info.value_len + 1;
    mm_->mm_alloc_subblock(kv_block_size, &ctx->mm_alloc_ctx); 

    //gen_write_kv_sr_lists This function is very important!

    //acquire the address of KVblock and write key-value pair
    uint64_t KVblock_addr = ctx->mm_alloc_ctx.addr;

    
    // memcpy(KVblock_addr, ctx->key_len, ctx->value_len);
    // memcpy(block_addr + 5, ctx->key, ctx->value);

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


void Client::fill_slot(MMAllocCtx * mm_alloc_ctx, KVHashInfo * a_kv_hash_info, Slot * local_slot) {
    local_slot->fp = a_kv_hash_info->fp;
    local_slot->kv_len = mm_alloc_ctx->num_subblocks; // every subblock has 64 bytes, and kv_len represents the total length of KV block.
    HashIndexConvert64To40Bits(mm_alloc_ctx->addr, local_slot->pointer);
}

void Client::find_empty_slot(KVReqCtx * ctx) 
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


