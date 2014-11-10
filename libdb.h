#ifndef LIBDB_H
#define LIBDB_H

#include <string.h>
/* */
/* */

#define MIN_BLOCK_SIZE 1024

/* check `man dbopen` */
struct DBT {
	void  *data;
	size_t size;
};

struct DBC {
	/* Maximum on-disk file size */
	/* 512MB by default */
	size_t db_size;
	/* Maximum chunk (node/data chunk) size */
	/* 4KB by default */
	size_t chunk_size; // block size
	/* Maximum memory size */
	/* 16MB by default */
    size_t mem_size; 
};

typedef struct _cache_block {
	int block_id;
	int pos_in_cache;
	struct _cache_block *prev;
	struct _cache_block *next;
} cache_block;

typedef struct _db_cache {
	cache_block *first;
	cache_block *last;
	int total_blocks;
	int occuped_blocks;
	void *cache_memory;
} db_cache;

typedef struct {
	size_t db_size;
	size_t chunk_size;
	size_t mem_size;
	int root_id;
	int bitmap_size; /*in bytes*/
	int bitmap_start_index;
	int free_block_count;
	int tree_depth;
} db_header;

typedef struct {
	int fd;
	size_t db_size;
	size_t chunk_size;
	size_t mem_size;
	void * root_node;
	int root_id;
	/*bit map*/
	char * bitmap;
	/*bitmap size in bytes*/
	size_t bitmap_size;
	/*first iterating byte in bitmap*/
	int bitmap_start_index;
	int free_block_count;
	int tree_depth;
} db_info_in_DB;


struct DB {
    /* Public API */
    int (*close)(struct DB *db);
    int (*del)(struct DB *db, struct DBT *key);
    int (*get)(struct DB *db, struct DBT *key, struct DBT *data);
    int (*put)(struct DB *db, struct DBT *key, struct DBT *data);
    int (*sync)(struct DB *db);
    /* Private API */
	//db_info_in_DB db_info;
    /* ... */
}; /* Need for supporting multiple backends (HASH/BTREE) */

struct MY_DB {
	 /* Public API */
    int (*close)(struct DB *db);
    int (*del)(struct DB *db, struct DBT *key);
    int (*get)(struct DB *db, struct DBT *key, struct DBT *data);
    int (*put)(struct DB *db, struct DBT *key, struct DBT *data);
    int (*sync)(struct DB *db);
    /* Private API */
	db_info_in_DB db_info;
	db_cache cache;
    /* ... */
}; /* Need for supporting multiple backends (HASH/BTREE) */


struct DB *dbcreate(const char *file, struct DBC conf);
struct DB *dbopen  (const char *file); /* Metadata in file */

int db_close(struct DB *db);
int del(struct DB *db, struct DBT *key);
int get(struct DB *db, struct DBT *key, struct DBT *data);
int put(struct DB *db, struct DBT *key, struct DBT *data);
	
int db_del(struct DB *db, void *k, size_t size)
{
	struct DBT key;
	key.data = k;
	key.size = size;
	return del(db, &key);
}

int db_get(struct DB *db, void *k, size_t k_s, void **v, size_t *v_s)
{
	struct DBT key;
	struct DBT value;
	int res;
	key.data = k;
	key.size = k_s;
	res = get(db, &key, &value);
	*v = value.data;
	*v_s = value.size;
	return  res;
}	
	
int db_put(struct DB *db, void *k, size_t k_s, void * v, size_t v_s )
{
	struct DBT key;
	struct DBT value;
	key.data = k;
	key.size = k_s;
	value.data = v;
	value.size = v_s;
	return put(db, &key, &value);
}

#endif


