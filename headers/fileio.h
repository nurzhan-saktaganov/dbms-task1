#ifndef FILEIO_H
#define FILEIO_H

#define WITH_CACHE

#include <unistd.h>
#include "../libdb.h"

void write_block_to_file(struct MY_DB *db, void *block, int block_id)
{
#ifdef WITH_CACHE
	write(1,"writebegin", 10);
	write_through_cache(db, block, block_id);
	write(1,"writeend\n", 9);
#else
	
	long int offset = block_offset_in_file(db, block_id);
	lseek(db->db_info.fd, offset, 0 );
	write(db->db_info.fd, block, db->db_info.chunk_size);
#endif
	return;
}

void read_block_from_file(struct MY_DB *db, void *block, int block_id)
{
#ifdef WITH_CACHE
	write(1,"readbegin",9);
	read_through_cache(db, block, block_id);
	write(1,"readend\n", 8);
#else	
	long int offset = block_offset_in_file(db, block_id);
	lseek(db->db_info.fd, offset, 0);
	read(db->db_info.fd, block, db->db_info.chunk_size);
#endif
	return;
}

#endif
