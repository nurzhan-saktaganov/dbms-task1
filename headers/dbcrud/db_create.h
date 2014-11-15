#ifndef DB_CREATE_H
#define DB_CREATE_H

#include <stdlib.h>
#include <string.h>
#include <fcntl.h>
#include <unistd.h>

/* We can not create db, if db with same name already exists */
struct DB *dbcreate(const char *file, struct DBC conf)
{
	struct MY_DB *res;
	char buf;
	db_header hdr;
	int tmp1;
	int tmp2;
	void *root_block;
	int *int_ptr;
	/* limit for min chunk_size */
	if(conf.chunk_size < MIN_BLOCK_SIZE) {
		conf.chunk_size = MIN_BLOCK_SIZE;
	}
	
	res = (struct MY_DB *) malloc(sizeof(struct MY_DB));
	memset( res, 0, sizeof(struct MY_DB));
		
	res->db_info.fd = open(file, O_RDONLY, 0);
	if(res->db_info.fd != -1) {
		/* if file|db already exists - error */
		free(res);
		return NULL;
	} else {
		/* create new db size of $db_size bytes */
		res->db_info.fd = open(file, O_CREAT | O_RDWR, 0777);
		//res->db_info.fd = creat(file, 0777);
		lseek(res->db_info.fd, conf.db_size - 1, 0);
		write(res->db_info.fd, &buf, sizeof(buf));
	}
#ifdef WITH_CACHE	
	conf.mem_size = (conf.mem_size / conf.chunk_size) * conf.chunk_size;
#endif
	/* prepare a db's header */
	hdr.db_size = conf.db_size;
	hdr.chunk_size = conf.chunk_size;
#ifdef WITH_CACHE
	hdr.mem_size = conf.mem_size;
#endif
	/* bitmap - one bit for one chunk */
	hdr.bitmap_size = conf.db_size / conf.chunk_size / 8;
	/* tmp1 is size of all meta-info in bytes */
	tmp1 = (sizeof(db_header) + hdr.bitmap_size);
	/* tmp2 is size of all meta-info in chunks */
	tmp2 = tmp1 / conf.chunk_size;
	if (tmp2 * conf.chunk_size != tmp1) {
		tmp2++;
	}
	/* chunks union(CU) consists of 8 x  of chunks */
	/* now tmp1 is size off all meta-info in CU*/
	tmp1 = tmp2 / 8;
	if (tmp1 * 8 != tmp2) {
		tmp1++;
	}
	hdr.bitmap_start_index = tmp1;
	hdr.free_block_count = (hdr.bitmap_size - hdr.bitmap_start_index) * 8;
	
	res->db_info.db_size = conf.db_size;
	res->db_info.chunk_size = conf.chunk_size;
#ifdef WITH_CACHE
	res->db_info.mem_size = conf.mem_size;
#endif
	
	res->db_info.bitmap_size = hdr.bitmap_size;
	res->db_info.bitmap_start_index = hdr.bitmap_start_index;
	res->db_info.free_block_count = hdr.free_block_count;
	res->db_info.tree_depth = 0;
	res->db_info.bitmap = (char *) malloc(hdr.bitmap_size);
	/* all chuncks are free */
	memset(res->db_info.bitmap, 0, hdr.bitmap_size);
	
	/* reserve block for root_node */
	hdr.root_id = block_allocate(res);
	res->db_info.root_id = hdr.root_id;
	
	/* */
	root_block = (void *) malloc(conf.chunk_size);
	res->db_info.root_node = root_block;
	
	/* 0 key/value pairs in root*/
	int_ptr = (int *)root_block;
	*int_ptr = 0;
	/* sign of leaf, i.e. root node is leaf*/
	*(char *)(int_ptr + 1 ) = (char) 1;
	
	/* init cache */
#ifdef WITH_CACHE
	res->cache.total_blocks = conf.mem_size / conf.chunk_size;
	res->cache.occuped_blocks = 0;
	res->cache.first = NULL;
	res->cache.last = NULL;
	res->cache.cache_memory = malloc(conf.mem_size);
	res->cache.bin_tree = NULL;
#endif
		
	/* write to file meta-info */
	lseek(res->db_info.fd, 0, 0);
	write(res->db_info.fd, &hdr, sizeof(hdr));
	write(res->db_info.fd, res->db_info.bitmap, hdr.bitmap_size);
	/*fwrite(res->db_info.bitmap, sizeof(char), hdr.bitmap_size, res->db_info.fp);*/
	
	/* write to file root node */
	write_block_to_file(res, root_block, hdr.root_id);
	
	/* init functions */
	res->close = db_close;
	res->del = del;
	res->get = get;
	res->put = put;
	
	return (struct DB *)res;
}

#endif
