#ifndef WRITE_THROUGH_CACHE_H
#define WRITE_THROUGH_CACHE_H

#include <stdlib.h>
#include <string.h>

void write_through_cache(struct MY_DB *db, void *block, int block_id) {
	
	cache_block *current;
	long int offset;
	void *address;
		
	current = bin_tree_search(db->cache.bin_tree, block_id);
	if(current != NULL) {
		if(current->block_id == block_id) {
			address = address_in_cache(db, current);
			memcpy(address, block, db->db_info.chunk_size);
			//success++;
			/* write to disk */
			offset = block_offset_in_file(db, block_id);
			lseek(db->db_info.fd, offset, 0);
			write(db->db_info.fd, block, db->db_info.chunk_size);
						
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
	}

	if(db->cache.occuped_blocks == db->cache.total_blocks) {
		/* no free space in cache, flush last */
		db->cache.bin_tree = bin_tree_remove(db->cache.bin_tree,
												db->cache.last->block_id);
		
		offset = block_offset_in_file(db, db->cache.last->block_id);
		address = address_in_cache(db, db->cache.last);
		lseek(db->db_info.fd, offset, 0);
		write(db->db_info.fd, address, db->db_info.chunk_size);
		
		current = db->cache.last;
		db->cache.last = db->cache.last->prev;
		db->cache.last->next = NULL;
		
		current->prev = NULL;
		current->next = db->cache.first;
		db->cache.first->prev = current;
		db->cache.first = current;
		db->cache.first->block_id = block_id;
		
		db->cache.bin_tree = bin_tree_insert(db->cache.bin_tree,
													block_id, current);		
		memcpy(address, block, db->db_info.chunk_size);
		/* write to disk */
		offset = block_offset_in_file(db, block_id);
		lseek(db->db_info.fd, offset, 0);
		write(db->db_info.fd, block, db->db_info.chunk_size);
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
		db->cache.bin_tree = bin_tree_insert(db->cache.bin_tree,
													block_id, current);
		
		address = address_in_cache(db, db->cache.first);
		memcpy(address, block, db->db_info.chunk_size);
		/* write to disk */
		offset = block_offset_in_file(db, block_id);
		lseek(db->db_info.fd, offset, 0);
		write(db->db_info.fd, block, db->db_info.chunk_size);
		
		db->cache.occuped_blocks++;
	}
	//mismatch++;
	
	return;
}

#endif
