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
			write(1, "1\n", 2);
			address = address_in_cache(db, current);
			memcpy(block, address, db->db_info.chunk_size);
			
			
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
		current = current->next;
	}

	if(db->cache.occuped_blocks == db->cache.total_blocks) {
		/* no free space in cache, flush last */
		write(1, "2-1\n", 4);
		offset = block_offset_in_file(db, db->cache.last->block_id);
		address = address_in_cache(db, db->cache.last);
		lseek(db->db_info.fd, offset, 0);
		write(db->db_info.fd, address, db->db_info.chunk_size);
		write(1, "2-2\n", 4);
		current = db->cache.last;
		if(db->cache.last->prev == NULL)
			exit(1);
		db->cache.last = db->cache.last->prev;
		if(db->cache.last->next  == NULL)
			exit(2);
		db->cache.last->next = NULL;
		// = db->cache.last->block_id;
		current->prev = NULL;
		current->next = db->cache.first;
		db->cache.first->prev = current;
		db->cache.first = current;
		db->cache.first->block_id = block_id;
		write(1, "2-3\n", 4);
		offset = block_offset_in_file(db, block_id);
		lseek(db->db_info.fd, offset, 0);
		read(db->db_info.fd, address, db->db_info.chunk_size);
		memcpy(block, address, db->db_info.chunk_size);
		if(db->cache.last->prev == NULL)
			exit(3);
	} else {
		//TODO;
		write(1, "3\n", 2);
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
		if(db->cache.last->prev == NULL)
			exit(5);
	}
	
	return;
}

#endif
