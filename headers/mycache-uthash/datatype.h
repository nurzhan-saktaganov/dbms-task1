#ifndef DATATYPE_H
#define DATATYPE_H
#include "uthash.h"

struct my_struct {
    int id;                    /* key */
    cache_block *cache_block_ptr;
    UT_hash_handle hh;         /* makes this structure hashable */
};

struct my_struct *global_hash = NULL;

#endif
