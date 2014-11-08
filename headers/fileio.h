#ifndef FILEIO_H
#define FILEIO_H

#include <unistd.h>
#include "../libdb.h"

void write_block_to_file(struct MY_DB *db, void *block, int block_id)
{
	long int offset = block_offset_in_file(db, block_id);
	lseek(db->db_info.fd, offset, 0 /* from begin */);
	write(db->db_info.fd, block, db->db_info.chunk_size);
	return;
}

void read_block_from_file(struct MY_DB *db, void *block, int block_id)
{
	long int offset = block_offset_in_file(db, block_id);
	lseek(db->db_info.fd, offset, 0 /* from begin */);
	read(db->db_info.fd, block, db->db_info.chunk_size);
	return;
}

#endif