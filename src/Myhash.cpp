#include <vector>
#include <string>
#include <optional>
#include <cmath>
#include <cstdint>
#include "Myhash.h"
#include "mm.h"

Myhash::Myhash(size_t chunk_size, size_t num_chunks) {
    mm_ = new MemoryPool(chunk_size , num_chunks);
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
    // Reserve enongh memory space to store subtable entry.
    // Because To support concurrent access during resizing, the starting address of the directory in the memory pool cannot be changed.
    for (int i = 0; i < HASH_INIT_SUBTABLE_NUM; i ++) {
        for (int j = 0; j < HASH_SUBTABLE_NUM / HASH_INIT_SUBTABLE_NUM; j ++) {
            uint64_t subtable_idx = j * HASH_INIT_SUBTABLE_NUM + i;
            MMAllocSubtableCtx *subtable_info = (MMAllocSubtableCtx *)malloc(sizeof(MMAllocSubtableCtx));
            
            if(j==0)
            {
                subtable_info->subtable_idx = subtable_idx;
                mm_->mm_alloc_subtable(subtable_info,SUBTABLE_LEN);
                // initialize buckets for subtable
                for (int j = 0; j < HASH_ADDRESSABLE_BUCKET_NUM; j ++) {
                    Bucket * bucket = (Bucket *)subtable_info->addr + j;
                    memset(bucket, 0, sizeof(Bucket));
                    bucket->h.local_depth = HASH_INIT_LOCAL_DEPTH;                                
                    bucket->h.prefix = subtable_info->subtable_idx;
                }
                HashIndexConvert64To48Bits(subtable_info->addr, root_->subtable_entry[subtable_idx].pointer);
            }
            else
            {
                memset(root_->subtable_entry[subtable_idx].pointer, 0, sizeof(root_->subtable_entry[subtable_idx].pointer));
            }
            // only one thread initialize it, so atomic operation doesn't need.
            root_->subtable_entry[subtable_idx].local_depth = HASH_INIT_LOCAL_DEPTH;
            root_->subtable_entry[subtable_idx].lock = 0;
        }   
    }
    return;
}

void * Myhash::kv_search(KVInfo * kv_info)
{
    KVReqCtx ctx;
    memset(&ctx, 0, sizeof(KVReqCtx));
    ctx.kv_info = kv_info;

    get_kv_hash_info((uint64_t*)ctx.kv_info->key_addr,ctx.kv_info->key_len, &ctx.hash_info);
    get_kv_addr_info(&ctx.hash_info, &ctx.tbl_addr_info); // initialize ctx.hash_info
    find_kv_in_buckets(&ctx);
    
    if(ctx.ret_val.value_addr==NULL && ((ctx.hash_info.local_depth != ctx.f_com_bucket->h.local_depth && ctx.hash_info.prefix != ctx.f_com_bucket->h.prefix) || (ctx.hash_info.local_depth != ctx.f_com_bucket[1].h.local_depth && ctx.hash_info.prefix != ctx.f_com_bucket[1].h.prefix) || (ctx.hash_info.local_depth != ctx.s_com_bucket->h.local_depth && ctx.hash_info.prefix != ctx.s_com_bucket->h.prefix) || (ctx.hash_info.local_depth != ctx.s_com_bucket[1].h.local_depth && ctx.hash_info.prefix != ctx.s_com_bucket[1].h.prefix) ))
    {
        ctx.hash_info.prefix = (ctx.hash_info.hash_value >> SUBTABLE_USED_HASH_BIT_NUM) & HASH_MASK(root_->global_depth);
        ctx.hash_info.local_depth = root_->subtable_entry[ctx.hash_info.prefix].local_depth;        
        get_kv_addr_info(&ctx.hash_info, &ctx.tbl_addr_info); 
        find_kv_in_buckets(&ctx);
    }
    if(ctx.ret_val.value_addr!=NULL && !VerifyChecksum(ctx.kv_info->key_addr,ctx.kv_info->key_len,ctx.ret_val.value_addr,ctx.kv_info->value_len,ctx.checksum)){
        // maybe it doesn't found, maybe it has been released by another thread who excuted delete/update operation. Both is OK.
        ctx.ret_val.value_addr = NULL;
    }
    return ctx.ret_val.value_addr;
}

int Myhash::kv_update(KVInfo * kv_info)
{
    KVReqCtx ctx;
    memset(&ctx, 0, sizeof(KVReqCtx));

    ctx.kv_info = kv_info;

    // Allocate KV block memory, sizeof(k_len+v_len)=6, sizeof(crc)=1
    uint32_t kv_block_size = 6 + (ctx.kv_info)->key_len + (ctx.kv_info)->value_len + 8;
    mm_->mm_alloc_subblock(kv_block_size, &ctx.mm_alloc_ctx); 

    //acquire the address of KVblock and write key-value pair and checksum
    uint64_t* KVblock_addr = (uint64_t*)ctx.mm_alloc_ctx.addr;
    uint64_t checksum = SetChecksum(&(ctx.kv_info)->key_addr,(ctx.kv_info)->key_len,&(ctx.kv_info)->value_addr,(ctx.kv_info)->value_len);
    memcpy(KVblock_addr, &(ctx.kv_info)->key_len, 2);
    memcpy(KVblock_addr+2, &(ctx.kv_info)->value_len, 4);
    memcpy(KVblock_addr + 6, &(ctx.kv_info)->key_addr, (ctx.kv_info)->key_len);
    memcpy(KVblock_addr + 6 + (ctx.kv_info)->key_len, &(ctx.kv_info)->value_addr,(ctx.kv_info)->value_len);
    memcpy(KVblock_addr + 6 + (ctx.kv_info)->key_len + (ctx.kv_info)->value_len,&checksum,8);

    //If it needs memory barrier?

    get_kv_hash_info((uint64_t*)ctx.kv_info->key_addr,ctx.kv_info->key_len, &ctx.hash_info);
    get_kv_addr_info(&ctx.hash_info, &ctx.tbl_addr_info);

    Slot* target_slot;
    find_slot_in_buckets(&ctx,target_slot);

    //release the KV subblock of old key-value pair.
    uint64_t kv_raddr = HashIndexConvert48To64Bits(target_slot->pointer);
    mm_->mm_free_subblock(kv_raddr); 

    if(target_slot!=NULL)
    {
        int ret = fill_slot(&ctx.mm_alloc_ctx, &ctx.hash_info, target_slot);
        if(ret==0){
            ctx.ret_val.ret_code = KV_OPS_SUCCESS;
        }
        else{
            ctx.ret_val.ret_code = KV_OPS_FAIL_RETURN;
        }
    }
    
    return ctx.ret_val.ret_code;
}

int Myhash::find_slot_in_buckets(KVReqCtx * ctx, Slot* target_slot)
{
    get_comb_bucket_info(ctx);

    // search all kv pair that finger print matches, myhash use two hash function to solve hash collision, use one overﬂow bucket to store overflow kv pairs.
    for (int i = 0; i < 4; i ++) {
        for (int j = 0; j < HASH_ASSOC_NUM; j ++) { // every bucket has HASH_ASSOC_NUM slot
            if (ctx->slot_arr[i][j].fp == ctx->hash_info.fp && ctx->slot_arr[i][j].len != 0) {
                 target_slot = &ctx->slot_arr[i][j];
                 return 0;
            }
        }
    }
    return -1;
}

int Myhash::kv_insert(KVInfo * kv_info)
{
    KVReqCtx ctx;
    memset(&ctx, 0, sizeof(KVReqCtx));

    ctx.kv_info = kv_info;

    get_kv_hash_info((uint64_t*)ctx.kv_info->key_addr,ctx.kv_info->key_len, &ctx.hash_info);
    get_kv_addr_info(&ctx.hash_info, &ctx.tbl_addr_info);

    //After get the bucket address of KV pair, we need to find an empty slot to store the KV pair.
    kv_insert_read_buckets_and_write_kv(&ctx); 

    //Check if there are duplicate key in 4 candidate buckets.
    check_kv_in_candidate_buckets(&ctx);
    return ctx.ret_val.ret_code;
}

void Myhash::check_kv_in_candidate_buckets(KVReqCtx * ctx) {
    get_comb_bucket_info(ctx);
    bool flag = 0;
    Slot* target_slot;
    for (int i = 0; i < 4; i ++) {
        for (int j = 0; j < HASH_ASSOC_NUM; j ++) { // every bucket has HASH_ASSOC_NUM slot
            target_slot = &ctx->slot_arr[i][j];
            if (ctx->slot_arr[i][j].fp == ctx->hash_info.fp && ctx->slot_arr[i][j].len != 0) {
                //I am not sure if I need to compare two keys. Or fp is the only identifier for a key?
                if(!flag){
                    flag=1;
                }
                else{
                    //release the kv subblock of duplicated keys.
                    uint64_t kv_raddr = HashIndexConvert48To64Bits(target_slot->pointer);
                    mm_->mm_free_subblock(kv_raddr); 

                    //Set target_slot of duplicated keys to 0;
                    uint64_t atomic_slot_val = 0;
                    uint64_t expected = *reinterpret_cast<uint64_t*>(target_slot);
                    atomic_write_to_slot(&ctx->slot_arr[i][j], expected, atomic_slot_val);
                }
            }
        }
    }
    return;

}

void Myhash::find_kv_in_buckets(KVReqCtx * ctx) {
    get_comb_bucket_info(ctx);
    // search all kv pair that finger print matches, myhash use two hash function to solve hash collision, use one overﬂow bucket to store overflow kv pairs.
    for (int i = 0; i < 4; i ++) {
        for (int j = 0; j < HASH_ASSOC_NUM; j ++) { // every bucket has HASH_ASSOC_NUM slot
            if (ctx->slot_arr[i][j].fp == ctx->hash_info.fp && ctx->slot_arr[i][j].len != 0) {
                //I am not sure if I need to compare two keys. Or fp is the only identifier for a key?

                KVRWAddr cur_kv_addr;
                cur_kv_addr.addr = HashIndexConvert48To64Bits(ctx->slot_arr[i][j].pointer);
                // cur_kv_addr.len    = ctx->slot_arr[i][j].len * mm_->chunk_size_;
                // get KV pair   |key_len|V_len|Key|Value|
                ctx->ret_val.value_addr = (void*)(cur_kv_addr.addr + 6 + ctx->kv_info->key_len); 
                memcpy(&ctx->checksum, ctx->ret_val.value_addr + ctx->kv_info->value_len, sizeof(uint64_t)); 
                return;
            }
        }
    }

    ctx->ret_val.value_addr = NULL;
    return;

}

// In paper, read bukets and write KV subblock execute in parallel. I think this is different from mine.
void Myhash::kv_insert_read_buckets_and_write_kv(KVReqCtx * ctx) {

    // Allocate KV block memory, sizeof(k_len+v_len)=6, sizeof(crc)=1
    uint32_t kv_block_size = 6 + (ctx->kv_info)->key_len + (ctx->kv_info)->value_len+8;
    
    //acquire the address of KVblock and write key-value pair
    mm_->mm_alloc_subblock(kv_block_size, &ctx->mm_alloc_ctx); 
    void* KVblock_addr = (void*)ctx->mm_alloc_ctx.addr;
    if(KVblock_addr == NULL){
        printf("Error: Failed to allocate memory for KVblock_addr\n");
        return;
    }
    uint64_t checksum = SetChecksum(&(ctx->kv_info)->key_addr,(ctx->kv_info)->key_len,&(ctx->kv_info)->value_addr,(ctx->kv_info)->value_len);
    memcpy(KVblock_addr, &(ctx->kv_info)->key_len, sizeof(uint16_t));
    memcpy(KVblock_addr + 2, &(ctx->kv_info)->value_len, sizeof(uint32_t));
    memcpy(KVblock_addr + 6, &(ctx->kv_info)->key_addr, (ctx->kv_info)->key_len);
    memcpy(KVblock_addr + 6 + (ctx->kv_info)->key_len, &(ctx->kv_info)->value_addr,(ctx->kv_info)->value_len);
    memcpy(KVblock_addr + 6 + (ctx->kv_info)->key_len + (ctx->kv_info)->value_len,&checksum,8);
    
    //find an empty slot ……
    find_empty_slot(ctx);
    if (ctx->bucket_idx == -1) {
        mm_->mm_free_cur(&ctx->mm_alloc_ctx); 
        // I think we need to resize hash table. Because all candidate buckets are filled. Or when pass the load factor?
        // ……
        // trigger resize operation

        return;
    }
    // Determine the target slot and fill it.
    Slot* target_slot;
    if (ctx->bucket_idx < 2) {
         target_slot = &ctx->f_com_bucket[ctx->bucket_idx].slots[ctx->slot_idx];
    } else {
        target_slot = &ctx->s_com_bucket[ctx->bucket_idx - 2].slots[ctx->slot_idx];
    }

    int ret = fill_slot(&ctx->mm_alloc_ctx, &ctx->hash_info, target_slot);
    if(ret==0){
        ctx->ret_val.ret_code = KV_OPS_SUCCESS;
    }
    else{
        ctx->ret_val.ret_code = KV_OPS_FAIL_RETURN;
    }
    return;
}

// Seperate one independent thread to run kv resize operation
void Myhash::kv_resize(uint64_t cur_prefix, uint8_t cur_local_depth){

    // Before execute resize operation, check if current subtable is locked. 
    if(root_->subtable_entry[cur_prefix].lock != 0){
        return;
    }
    uint64_t new_prefix;
    uint8_t new_local_depth;
    uint8_t p[6];
    uint64_t expected_new,atomic_subtable_val_new, expected_cur, atomic_subtable_val_cur;
    Subtable *target_subtable;
    new_prefix = cur_prefix | (1 << cur_local_depth);
    new_local_depth = cur_local_depth + 1;
    
    // 1.atomiclly modify and lock current subtable entry.
    target_subtable = &root_->subtable_entry[cur_prefix];
    expected_cur = pack_subtable(root_->subtable_entry[cur_prefix]);
    atomic_subtable_val_cur = pack_subtable_modified_fields(1,new_local_depth,root_->subtable_entry[cur_prefix].pointer);    
    atomic_write_to_subtable(target_subtable, expected_cur, atomic_subtable_val_cur);

    // 2.increse the global_depth by 1
    // global_depth += 1;
    root_ -> global_depth.fetch_add(1, std::memory_order_relaxed);
    
    // 3.update subtable entries to new directory
    int new_directory_size = 1 << root_ -> global_depth;
    uint64_t expected_header, atomic_header_val;
    for (int i = 0; i < (new_directory_size / 2); ++i) {
        int new_id = i + new_directory_size / 2 ;
        //allocate new subtable for resize.
        if(new_id == new_prefix && root_ -> subtable_entry[new_id].lock == 0) // no other thread is holding it
        {
            // allocate new subtable
            MMAllocSubtableCtx *subtable_info = (MMAllocSubtableCtx *)malloc(sizeof(MMAllocSubtableCtx));
            subtable_info->subtable_idx = new_prefix;
            mm_->mm_alloc_subtable(subtable_info,SUBTABLE_LEN);
            // initialize buckets for subtable
            for (int j = 0; j < HASH_ADDRESSABLE_BUCKET_NUM; j ++) {
                Bucket * bucket = (Bucket *)subtable_info->addr + j;
                
                // atomically update new subtable
                atomic_header_val = pack_header_fields(new_local_depth,new_prefix);
                atomic_write_to_bucket_header(&bucket->h,atomic_header_val);
                memset(bucket->slots, 0, 7*sizeof(Slot));

            }
            HashIndexConvert64To48Bits(subtable_info->addr, p);
            atomic_subtable_val_new = pack_subtable_modified_fields(1,new_local_depth,p);
        }
        else
        {
            atomic_subtable_val_new = pack_subtable_modified_fields(0,cur_local_depth,root_->subtable_entry[i].pointer);
        }  
        // atomically lock new subtable entry and update the local_depth and pointer of new subtable.
        expected_new = pack_subtable(root_->subtable_entry[new_id]); 
        target_subtable = &root_->subtable_entry[new_prefix];
        atomic_write_to_subtable(target_subtable, expected_new, atomic_subtable_val_new);          
    }

    // 5.modify buckets for new subtable
    // (1) get the init address of new_subtable
    uint64_t cur_subtable_addr = HashIndexConvert48To64Bits(root_->subtable_entry[cur_prefix].pointer);
    uint64_t new_subtable_addr = HashIndexConvert48To64Bits(root_->subtable_entry[new_prefix].pointer);

    // (2) iterate current subtable, recalculate the hash value of each key. 
    uint64_t iter_kv_subblock_addr,iter_key_addr,iter_key_len;
    uint8_t iter_num_of_subblocks;
    for (int i = 0; i < HASH_ADDRESSABLE_BUCKET_NUM; i ++) 
    {
        Bucket * bucket_in_cur_subtbl = (Bucket *)cur_subtable_addr + i;
        // atomically update the header of current bucket 
        expected_header = pack_header_fields(cur_local_depth,cur_prefix);
        atomic_header_val = pack_header_fields(new_local_depth,cur_prefix);
        atomic_cas_to_bucket_header(&bucket_in_cur_subtbl->h,expected_header, atomic_header_val);

        for (int j = 0; j < HASH_ASSOC_NUM; j ++)
        { 
            Slot* target_slot = &bucket_in_cur_subtbl->slots[j];
            iter_kv_subblock_addr = HashIndexConvert48To64Bits(target_slot -> pointer);
            iter_num_of_subblocks = target_slot -> len;

            // get hash_value and prefix of iter_key
            iter_key_addr = iter_kv_subblock_addr + 6 ;
            uint16_t iter_key_len = *reinterpret_cast<uint16_t*>(iter_kv_subblock_addr);
            uint64_t iter_hash_value = VariableLengthHash((void *)iter_key_addr, iter_key_len, 0);
            uint64_t iter_key_prefix = (iter_hash_value >> SUBTABLE_USED_HASH_BIT_NUM) & HASH_MASK(root_->global_depth);
            
            if (iter_key_prefix == new_prefix){
                KVHashInfo iter_kv_hash_info;
                KVTableAddrInfo tbl_addr_info;
                iter_kv_hash_info.hash_value = iter_hash_value;
                get_kv_hash_info(&iter_key_addr,iter_key_len,&iter_kv_hash_info); // prefix, fp, hash_value,local_depth;
                get_kv_addr_info(&iter_kv_hash_info, &tbl_addr_info);

                // move KV pair to new subtable.
                kv_resize_update_slot(iter_kv_subblock_addr,iter_num_of_subblocks,&iter_kv_hash_info,&tbl_addr_info); 
                
                // don't forget to reset current slot, note that we don't need to move KVsubblock.
                uint64_t expected = *reinterpret_cast<uint64_t*>(target_slot);
                atomic_write_to_slot(target_slot,expected,0);
            }
            

        }
    }

    // unlock current and new subtable entry atomiclly.
    target_subtable = &root_->subtable_entry[cur_prefix];
    expected_cur = atomic_subtable_val_cur;
    atomic_subtable_val_cur = pack_subtable_modified_fields(0,new_local_depth,root_->subtable_entry[cur_prefix].pointer);    
    atomic_write_to_subtable(target_subtable, expected_cur, atomic_subtable_val_cur);

    target_subtable = &root_->subtable_entry[new_prefix];
    expected_new = pack_subtable(root_->subtable_entry[new_prefix]);
    atomic_subtable_val_new = pack_subtable_modified_fields(0,new_local_depth,root_->subtable_entry[new_prefix].pointer);    
    atomic_write_to_subtable(target_subtable, expected_new, atomic_subtable_val_new);

    return;

}

void Myhash::kv_resize_update_slot(uint64_t iter_kv_subblock_addr, uint8_t iter_kv_subblock_num, KVHashInfo *kv_hash_info, KVTableAddrInfo *tbl_addr_info ) {

    Bucket* f_com_bucket, * s_com_bucket, *new_bucket;
    int32_t bucket_idx, slot_idx;

    //find an empty slot ……
    find_empty_slot(tbl_addr_info, f_com_bucket, s_com_bucket, bucket_idx, slot_idx);
    
    // Determine the target slot and fill it.
    Slot* target_slot;
    if (bucket_idx < 2) {
         target_slot = &f_com_bucket[bucket_idx].slots[slot_idx];
         new_bucket = &f_com_bucket[bucket_idx];
    } else {
        target_slot = &s_com_bucket[bucket_idx - 2].slots[slot_idx];
        new_bucket = &s_com_bucket[bucket_idx - 2];
    }
    // fill slot in new subtable
    int ret = fill_slot(iter_kv_subblock_addr, iter_kv_subblock_num, kv_hash_info, target_slot);
    
    //update the header of new bucket
    // new_bucket->h.local_depth = kv_hash_info->local_depth;
    // new_bucket->h.prefix = kv_hash_info->prefix;
    return;
}

void Myhash::find_empty_slot(KVTableAddrInfo *tbl_addr_info, Bucket* f_com_bucket,Bucket* s_com_bucket,int32_t &bucket_idx,int32_t &slot_idx) 
{

    get_comb_bucket_info(tbl_addr_info,f_com_bucket, s_com_bucket);

    uint32_t f_main_idx = tbl_addr_info -> f_main_idx;
    uint32_t s_main_idx = tbl_addr_info -> s_main_idx;
    uint32_t f_free_num, s_free_num;
    uint32_t f_free_slot_idx, s_free_slot_idx;

    int32_t local_bucket_idx = -1;
    int32_t local_slot_idx = -1;

    for (int i = 0; i < 2; i ++) 
    {
        f_free_num = GetFreeSlotNum(f_com_bucket + f_main_idx, &f_free_slot_idx);
        s_free_num = GetFreeSlotNum(s_com_bucket + s_main_idx, &s_free_slot_idx);

        if (f_free_num > 0 || s_free_num > 0) {
            if (f_free_num >= s_free_num) {
                local_bucket_idx = f_main_idx;
                local_slot_idx = f_free_slot_idx;
            } else {
                // Help to differentiate the first com bucket and the second com bucket.
                local_bucket_idx = 2 + s_main_idx;
                local_slot_idx = s_free_slot_idx;
            }
        }
        f_main_idx = (f_main_idx + 1) % 2;
        s_main_idx = (s_main_idx + 1) % 2;
    }

    bucket_idx = local_bucket_idx;
    slot_idx   = local_slot_idx;
    return;
}

int Myhash::fill_slot(uint64_t iter_kv_subblock_addr, uint8_t iter_kv_subblock_num,  KVHashInfo * a_kv_hash_info, Slot* target_slot) {
    
    uint64_t atomic_slot_val;
    uint8_t * p_48 = new uint8_t[6];
    HashIndexConvert64To48Bits(iter_kv_subblock_addr, p_48);

    atomic_slot_val |= (static_cast<uint64_t>(a_kv_hash_info->fp) << 56);
    atomic_slot_val |= (static_cast<uint64_t>(iter_kv_subblock_num) << 48);
    atomic_slot_val |= (static_cast<uint64_t>(p_48[0]) << 40);
    atomic_slot_val |= (static_cast<uint64_t>(p_48[1]) << 32);
    atomic_slot_val |= (static_cast<uint64_t>(p_48[2]) << 24);
    atomic_slot_val |= (static_cast<uint64_t>(p_48[3]) << 16);
    atomic_slot_val |= (static_cast<uint64_t>(p_48[4]) << 8);
    atomic_slot_val |= (static_cast<uint64_t>(p_48[5]));

    //write to slot atomiclly. So don't need lock.
    int ret = atomic_write_to_slot(target_slot, 0, atomic_slot_val);
    return ret;

}



int Myhash::kv_delete(KVInfo * kv_info)
{
    KVReqCtx ctx;
    memset(&ctx, 0, sizeof(KVReqCtx));

    ctx.kv_info = kv_info;

    get_kv_hash_info((uint64_t*)ctx.kv_info->key_addr,ctx.kv_info->key_len, &ctx.hash_info);
    get_kv_addr_info(&ctx.hash_info, &ctx.tbl_addr_info);

    Slot* target_slot;
    int ret = find_slot_in_buckets(&ctx,target_slot);
    if (ret == 0)
    {
        // Release memory of KV block;
        uint64_t kv_raddr = HashIndexConvert48To64Bits(target_slot->pointer);
        mm_->mm_free_subblock(kv_raddr); 

        //Set target_slot to 0;
        uint64_t atomic_slot_val = 0;
        uint64_t expected = *reinterpret_cast<uint64_t*>(target_slot);
        atomic_write_to_slot(target_slot, expected, atomic_slot_val);
        ctx.ret_val.ret_code = KV_OPS_SUCCESS;
    }
    else 
    {
        
    }
    
    
    if(ret==0){
        
    }
    else{
        ctx.ret_val.ret_code = KV_OPS_FAIL_RETURN;
    }
    return ctx.ret_val.ret_code;

}

int Myhash::fill_slot(MMAllocCtx * mm_alloc_ctx, KVHashInfo * a_kv_hash_info, Slot* target_slot) {
    
    // target_slot->fp = a_kv_hash_info->fp;
    // target_slot->len = mm_alloc_ctx->num_subblocks; // every subblock has 64 bytes, and kv_len represents the total length of KV block.
    // HashIndexConvert64To48Bits(mm_alloc_ctx->addr, target_slot->pointer);
    uint64_t atomic_slot_val;
    uint8_t * p_48 = new uint8_t(6);
    HashIndexConvert64To48Bits(mm_alloc_ctx->addr, p_48);

    atomic_slot_val |= (static_cast<uint64_t>(a_kv_hash_info->fp) << 56);
    atomic_slot_val |= (static_cast<uint64_t>(mm_alloc_ctx->num_subblocks) << 48);
    atomic_slot_val |= (static_cast<uint64_t>(p_48[0]) << 40);
    atomic_slot_val |= (static_cast<uint64_t>(p_48[1]) << 32);
    atomic_slot_val |= (static_cast<uint64_t>(p_48[2]) << 24);
    atomic_slot_val |= (static_cast<uint64_t>(p_48[3]) << 16);
    atomic_slot_val |= (static_cast<uint64_t>(p_48[4]) << 8);
    atomic_slot_val |= (static_cast<uint64_t>(p_48[5]));

    //write to slot atomiclly. So don't need lock.
    int ret = atomic_write_to_slot(target_slot,0, atomic_slot_val);
    return ret;

}

int Myhash::atomic_cas_to_bucket_header(Header* target_header,uint64_t expected, uint64_t new_value) {
    std::atomic<uint64_t>* atomic_header = reinterpret_cast<std::atomic<uint64_t>*>(target_header);
    if (atomic_header->compare_exchange_strong(expected, new_value, std::memory_order_relaxed)) {
        return 0;
    } else {
       return -1;
    }
}

int Myhash::atomic_write_to_bucket_header(Header* target_header, uint64_t new_value) {
    std::atomic<uint64_t>* atomic_header = reinterpret_cast<std::atomic<uint64_t>*>(target_header);
    atomic_header->store(new_value, std::memory_order_relaxed);
    return 0;
}

int Myhash::atomic_write_to_subtable(Subtable* target_subtable,uint64_t expected, uint64_t new_value) {
    std::atomic<uint64_t>* atomic_subtable = reinterpret_cast<std::atomic<uint64_t>*>(target_subtable);
    if (atomic_subtable->compare_exchange_strong(expected, new_value, std::memory_order_relaxed)) {
        return 0;
    } else {
       return -1;
    }
}

int Myhash::atomic_write_to_slot(Slot* target_slot,uint64_t expected, uint64_t new_value) {
    std::atomic<uint64_t>* atomic_slot = reinterpret_cast<std::atomic<uint64_t>*>(target_slot);
    if (atomic_slot->compare_exchange_strong(expected, new_value, std::memory_order_relaxed)) {
        return 0;
    } else {
       return -1;
    }
}

void Myhash::find_empty_slot(KVReqCtx * ctx) 
{
    get_comb_bucket_info(ctx); // why here? 

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
void Myhash::get_kv_hash_info( uint64_t* key_addr, uint64_t key_len, KVHashInfo * a_kv_hash_info) 
{
    a_kv_hash_info->hash_value = VariableLengthHash((void *)key_addr, key_len, 0);
    // prefix is used to find the target subtable.
    a_kv_hash_info->prefix = (a_kv_hash_info->hash_value >> SUBTABLE_USED_HASH_BIT_NUM) & HASH_MASK(root_->global_depth);

    a_kv_hash_info->local_depth = root_->subtable_entry[a_kv_hash_info->prefix].local_depth;
    a_kv_hash_info->fp = HashIndexComputeFp(a_kv_hash_info->hash_value);
}

// class KVTableAddrInfo represents detailed bucket address of KV pair in hashtable.
// The function gets bucket address of a specified key.
void Myhash::get_kv_addr_info(KVHashInfo * a_kv_hash_info, KVTableAddrInfo * a_kv_addr_info) {
    uint64_t hash_value = a_kv_hash_info->hash_value;
    uint64_t prefix     = a_kv_hash_info->prefix;

    uint64_t f_index_value = SubtableFirstIndex(hash_value, root_->subtable_hash_range); // get different bucket according to hash_value
    uint64_t s_index_value = SubtableSecondIndex(hash_value, f_index_value, root_->subtable_hash_range);
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

void Myhash::get_comb_bucket_info(KVReqCtx * ctx) {
    //Initialize the first combination buckets and the second combination buckets, ensure that the firt bucket has lower address than the second one.
    if(ctx->tbl_addr_info.f_bucket_addr > ctx->tbl_addr_info.s_bucket_addr)
    {
        ctx->f_com_bucket = reinterpret_cast<Bucket*>(ctx->tbl_addr_info.s_bucket_addr);
        ctx->s_com_bucket = reinterpret_cast<Bucket*>(ctx->tbl_addr_info.f_bucket_addr);
    }
    else
    {
        ctx->f_com_bucket = reinterpret_cast<Bucket*>(ctx->tbl_addr_info.f_bucket_addr);
        ctx->s_com_bucket = reinterpret_cast<Bucket*>(ctx->tbl_addr_info.s_bucket_addr);
    }
    
    ctx->slot_arr[0] = ctx->f_com_bucket[0].slots;
    ctx->slot_arr[1] = ctx->f_com_bucket[1].slots;
    ctx->slot_arr[2] = ctx->s_com_bucket[0].slots;
    ctx->slot_arr[3] = ctx->s_com_bucket[1].slots;

}

void Myhash::get_comb_bucket_info(KVTableAddrInfo *tbl_addr_info,Bucket* f_com_bucket, Bucket* s_com_bucket ) {
    //Initialize the first combination buckets and the second combination buckets, ensure that the firt bucket has lower address than the second one.
    if(tbl_addr_info->f_bucket_addr > tbl_addr_info->s_bucket_addr)
    {
        f_com_bucket = reinterpret_cast<Bucket*>(tbl_addr_info -> s_bucket_addr);
        s_com_bucket = reinterpret_cast<Bucket*>(tbl_addr_info -> f_bucket_addr);
    }
    else
    {
        f_com_bucket = reinterpret_cast<Bucket*>(tbl_addr_info -> f_bucket_addr);
        s_com_bucket = reinterpret_cast<Bucket*>(tbl_addr_info -> s_bucket_addr);
    }
    return;
}



// we should only set one thread to GC. so in normal case, there is no racing between client threads.
void Myhash::free_batch() {
    std::unordered_map<std::string, uint64_t> faa_map(mm_->free_kv_subblock_map_);
    mm_->free_kv_subblock_map_.clear();
    for (auto it = faa_map.begin(); it != faa_map.end(); it ++) {
        uint64_t bmap_addr = std::stoull(it->first);  // the base address of bitmap block, note that every bitmap block has 8 bytes.
        uint64_t corr_bit = it -> second;
        
        // Calculate the start address of KV memory block and offset address. recollect the subblock.
        uint64_t block_addr = bmap_addr - ((bmap_addr % sizeof(uint64_t)) * sizeof(uint64_t));
        int subblock_offset = 0;
        while (corr_bit != 1) {
            corr_bit >>= 1;
            subblock_offset++;
        }
        uint64_t kv_subblock_address = block_addr + (subblock_offset * mm_ -> chunk_size_);
        Chunk subblock;
        subblock.addr= kv_subblock_address;
        mm_ -> subblock_free_queue_.push_back(subblock);

        // set the correspinding bit of bitmap to 0;
        reset_bit_atomic(&bmap_addr,corr_bit); //atomic set operation
    }
    return;
}

uint32_t Myhash::GetFreeSlotNum(Bucket * bucket, uint32_t * free_idx) {
    *free_idx = HASH_ASSOC_NUM;
    uint32_t free_num = 0;
    for (int i = 0; i < HASH_ASSOC_NUM; i++) {
      // when initialize the subtable, set all bucket to 0.
        if (bucket->slots[i].fp == 0 && bucket->slots[i].len == 0 && IsEmptyPointer(bucket->slots[i].pointer, 5)) {
            // free_idx_list[free_num] = i;
            free_num ++;
            *free_idx = i;
        }
    }
    return free_num;
}


