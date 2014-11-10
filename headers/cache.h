#ifndef CACHE_H
#define CACHE_H

#include <stdlib.h>
#include <string.h>

#ifdef WITH_AVL
#include "mycache-avl/avl_tree.h"
#include "mycache-avl/read_through_cache.h"
#include "mycache-avl/write_through_cache.h"
#else
#include "mycache/read_through_cache.h"
#include "mycache/write_through_cache.h"
#endif

void* address_in_cache(struct MY_DB *db, cache_block *c_b)
{
	void *address;
	int offset_in_cache;
	offset_in_cache = c_b->pos_in_cache * db->db_info.chunk_size;
	address = db->cache.cache_memory + offset_in_cache;
	return address;
}

void flush_cache(struct MY_DB *db)
{
	cache_block *current;
	long int offset;
	void *address;
	
	current = db->cache.first;
	while(current != NULL) {
		address = address_in_cache(db, current);
		offset = block_offset_in_file(db, current->block_id);
		lseek(db->db_info.fd, offset, 0);
		write(db->db_info.fd, address, db->db_info.chunk_size);
		current = current->next;
	}
		
	return;
}

void free_cache(struct MY_DB *db)
{
	cache_block *current, *next;
	
	current = db->cache.first;
	while(current != NULL){
		next = current->next;
		free(current);
		current = next;
	}
	
	db->cache.occuped_blocks = 0;
	free(db->cache.cache_memory);
	db->cache.cache_memory = NULL;
#ifdef WITH_AVL
	bin_tree_clear(db->cache.bin_tree);
#endif
	
	return;
}


#endif
