#ifndef DB_OPEN_H
#define DB_OPEN_H

#include <stdlib.h>
#include <string.h>
#include <fcntl.h>
#include <unistd.h>

/* We can not open db, if there no db with that name */
struct DB *dbopen (const char *file)
{
	struct MY_DB *res;
	db_header hdr;
	char *bitmap;
	
	res = (struct MY_DB *) malloc(sizeof(struct MY_DB));
	memset( res, 0, sizeof(struct MY_DB));
	
	res->db_info.fd = open(file, O_RDWR, 0777);
	if( res->db_info.fd == -1) {
		free(res);
		return NULL;
	}
	
	read(res->db_info.fd, &hdr, sizeof(hdr));
	bitmap = (char *)malloc(hdr.bitmap_size);
	read(res->db_info.fd, bitmap, hdr.bitmap_size);
	
	res->db_info.db_size = hdr.db_size;
	res->db_info.chunk_size = hdr.chunk_size;
	res->db_info.mem_size = hdr.mem_size;
	res->db_info.root_id = hdr.root_id;
	res->db_info.bitmap = bitmap;
	res->db_info.bitmap_size = hdr.bitmap_size;
	res->db_info.bitmap_start_index = hdr.bitmap_start_index;
	res->db_info.free_block_count = hdr.free_block_count;
	res->db_info.tree_depth = hdr.tree_depth;
	/* */
	res->db_info.root_node = (void *) malloc(hdr.chunk_size);
	read_block_from_file(res, res->db_info.root_node, hdr.root_id);
	
	/* init functions */
	res->close = db_close;
	res->del = del;
	res->get = get;
	res->put = put;
	
	return (struct DB *)res;
}

#endif 
