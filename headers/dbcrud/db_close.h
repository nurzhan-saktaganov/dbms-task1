#ifndef DB_CLOSE_H
#define DB_CLOSE_H

#include <stdlib.h>
#include <string.h>
#include <fcntl.h>
#include <unistd.h>

int db_close(struct DB *db_in){
	
	db_header hdr;
	struct MY_DB *db = (struct MY_DB *) db_in;
		
	hdr.db_size = db->db_info.db_size;
	hdr.chunk_size = db->db_info.chunk_size;
	hdr.mem_size = db->db_info.mem_size;
	hdr.root_id = db->db_info.root_id;
	hdr.bitmap_size = db->db_info.bitmap_size;
	hdr.bitmap_start_index = db->db_info.bitmap_start_index;
	hdr.free_block_count = db->db_info.free_block_count;
	hdr.tree_depth = db->db_info.tree_depth;
	 
	 /* write meta-info to file */
	lseek(db->db_info.fd, 0, 0);
	write(db->db_info.fd, &hdr, sizeof(hdr));
	write(db->db_info.fd, db->db_info.bitmap, hdr.bitmap_size);
	
	//flush_cache(db);
	/* write root block to file*/
	write_block_to_file(db, db->db_info.root_node, db->db_info.root_id);
#ifdef WITH_CACHE
	free_cache(db);
#endif
	free(db->db_info.root_node);
	free(db->db_info.bitmap);
	free((void *)db);
	
	return close(db->db_info.fd);
}

#endif
