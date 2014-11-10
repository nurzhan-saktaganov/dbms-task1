#ifndef READ_THROUGH_CACHE_H
#define READ_THROUGH_CACHE_H

#include <stdlib.h>
#include <string.h>

void read_through_cache(struct MY_DB *db, void *block, int block_id) {
	
	cache_block *current;
	long int offset;
	void *address;
	current = db->cache.first;
	while(current != NULL) {
		if(current->block_id == block_id) {
			address = address_in_cache(db, current);
			memcpy(block, address, db->db_info.chunk_size);
			//success++;
			
			if(current == db->cache.first) {
				return;
			} else if (current == db->cache.last){
				db->cache.last = db->cache.last->prev;
			}
			current->prev->next = current->next;
			if(current->next != NULL)
				current->next->prev = current->prev;
			
			current->prev = NULL;
			
			current->next = db->cache.first;
			db->cache.first->prev = current;
			db->cache.first = current;
			return;
		}
		//search++;
		current = current->next;
	}
	
	if(db->cache.occuped_blocks == db->cache.total_blocks) {
		/* no free space in cache, 'flush' last */
		address = address_in_cache(db, db->cache.last);
		
		current = db->cache.last;
		db->cache.last = db->cache.last->prev;
		db->cache.last->next = NULL;
		// = db->cache.last->block_id;
		current->prev = NULL;
		current->next = db->cache.first;
		db->cache.first->prev = current;
		db->cache.first = current;
		db->cache.first->block_id = block_id;
		offset = block_offset_in_file(db, block_id);
		lseek(db->db_info.fd, offset, 0);
		read(db->db_info.fd, address, db->db_info.chunk_size);
		memcpy(block, address, db->db_info.chunk_size);
	} else {
		//TODO;
		current = (cache_block *) malloc(sizeof(cache_block));
		current->block_id = block_id;
		current->pos_in_cache = db->cache.occuped_blocks;
		current->prev = NULL;
		current->next = db->cache.first;
		if(db->cache.first != NULL) {
			db->cache.first->prev = current;
			if(db->cache.first->next == NULL)
				db->cache.last = db->cache.first;
		} 
		
		db->cache.first = current;
		
		offset = block_offset_in_file(db, block_id);
		address = address_in_cache(db, db->cache.first);
		lseek(db->db_info.fd, offset, 0);
		read(db->db_info.fd, address, db->db_info.chunk_size);
		memcpy(block, address, db->db_info.chunk_size);
		db->cache.occuped_blocks++;
	}
	//missmatch++;
	return;
}

#endif
