#include "libdb.h"
#include <stdio.h>
#include <stdlib.h>
#include <string.h>

#define b0000_0000 (char) 0
#define b0000_0001 (char) 1
#define b0000_0010 (char) 2
#define b0000_0100 (char) 4
#define b0000_1000 (char) 8
#define b0001_0000 (char) 16
#define b0010_0000 (char) 32
#define b0100_0000 (char) 64
#define b1000_0000 (char) 128

/* compare two keys */
int cmpkeys(void *key1, void *key2, int size1, int size2)
{
	int minsize;
	int res;
	
	minsize = (size1 < size2 ? size1 : size2);
	
	res = memcmp(key1, key2, minsize);
	if (res == 0 && size1 != size2) {
		if(size1 > size2)
			res = 1;
		else
			res = -1;
	}
	
	return res;
}

/* returns block offset in file by id */
long int block_offset_in_file(const struct DB *db, int block_id)
{
	long int offset = block_id;
	return offset *= db->db_info.chunk_size;
}

/* returns id of allocated block */
/* numeration from 0 */
int block_allocate(struct DB *db)
{
	int i;
	int j = 0;
	int i0 = db->db_info.bitmap_start_index;
	int size = db->db_info.bitmap_size;
	char *bitmap = db->db_info.bitmap;
	
	/* find first free chunk and mark it as not free */
	for(i = i0; i < size; i++)
	{
		if(bitmap[i] == ~b0000_0000) {
			continue;
		} else if (!(b1000_0000 & bitmap[i])){
			j = 1;
			bitmap[i] = b1000_0000 | bitmap[i];
			break;
		} else if (!(b0100_0000 & bitmap[i])){
			j = 2;
			bitmap[i] = b0100_0000 | bitmap[i];
			break;
		} else if (!(b0010_0000 & bitmap[i])){
			j = 3;
			bitmap[i] = b0010_0000 | bitmap[i];
			break;
		} else if (!(b0001_0000 & bitmap[i])){
			j = 4;
			bitmap[i] = b0001_0000 | bitmap[i];
			break;
		} else if (!(b0000_1000 & bitmap[i])){
			j = 5;
			bitmap[i] = b0000_1000 | bitmap[i];
			break;
		} else if (!(b0000_0100 & bitmap[i])){
			j = 6;
			bitmap[i] = b0000_0100 | bitmap[i];
			break;
		} else if (!(b0000_0010 & bitmap[i])){
			j = 7;
			bitmap[i] = b0000_0010 | bitmap[i];
			break;
		} else if (!(b0000_0001 & bitmap[i])){
			j = 8;
			bitmap[i] = b0000_0001 | bitmap[i];
			break;
		}
	}
	
	if (j){
		/* return block/chunk id*/
		return i*8 + j - 1;
	}

	return 0;
}

/* numeration from 0 */
void block_free(struct DB *db, int block_id)
{
	int i = block_id / 8; /* byte position in bitmap */
	int j = block_id % 8 + 1; /* bit position in byte */
	char *bitmap = db->db_info.bitmap;
	
	/* boundaries check */
	if(i >= db->db_info.bitmap_size || block_id < 0)
		return;
		
	/* mark block as free */
	if (j == 1) {
		bitmap[i] = ~ b1000_0000 & bitmap[i];
	} else if (j == 2) {
		bitmap[i] = ~ b0100_0000 & bitmap[i];
	} else if (j == 3) {
		bitmap[i] = ~ b0010_0000 & bitmap[i];
	} else if (j == 4) {
		bitmap[i] = ~ b0001_0000 & bitmap[i];
	} else if (j == 5) {
		bitmap[i] = ~ b0000_1000 & bitmap[i];
	} else if (j == 6) {
		bitmap[i] = ~ b0000_0100 & bitmap[i];
	} else if (j == 7) {
		bitmap[i] = ~ b0000_0010 & bitmap[i];
	} else if (j == 8) {
		bitmap[i] = ~ b0000_0001 & bitmap[i];
	} 
		
	return;
}

void write_block_to_file(const struct DB *db, void *block, int block_id)
{
	long int offset = block_offset_in_file(db, block_id);
	fseek(db->db_info.fp, offset, SEEK_SET);
	fwrite(block, db->db_info.chunk_size, 1, db->db_info.fp);
	return;
}

void read_block_from_file(struct DB *db, void *block, int block_id)
{
	long int offset = block_offset_in_file(db, block_id);
	fseek(db->db_info.fp, offset, SEEK_SET);
	fread(block, db->db_info.chunk_size, 1, db->db_info.fp);
	return;
}


/* We can not create db, if db with same name already exists */
struct DB *dbcreate(const char *file, const struct DBC confg)
{
	struct DB *res;
	char buf;
	db_header hdr;
	int tmp1;
	int tmp2;
	struct DBC conf = confg;
	char *root_block;
	int *int_ptr;
	
	/* limit for min chunk_size */
	if(conf.chunk_size < 1024) {
		conf.chunk_size = 1024;
	}
	
	res = (struct DB *) malloc(sizeof(struct DB));
	/* just for good form :) */
	memset( res, 0, sizeof(struct DB));
		
	res->db_info.fp = fopen(file, "r+");
	if (res->db_info.fp != NULL) {
		/* if file|db already exists - error */
		free(res);
		return NULL;
	} else {
		/* create new db size of $db_size bytes */
		res->db_info.fp = fopen(file, "w+");
		fseek(res->db_info.fp, conf.db_size - 1, SEEK_SET);
		fwrite(&buf, sizeof(buf), 1, res->db_info.fp);
		/*fseek(res->db_info.fp, 0, SEEK_SET);*/
	}
	
	/* prepare a db's header */
	hdr.db_size = conf.db_size;
	hdr.chunk_size = conf.chunk_size;
	hdr.mem_size = conf.mem_size;
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
	/* now tmp1 is size off all meta-ifno in CU*/
	tmp1 = tmp2 / 8;
	if (tmp1 * 8 != tmp2) {
		tmp1++;
	}
	hdr.bitmap_start_index = tmp1;
	
	res->db_info.db_size = conf.db_size;
	res->db_info.chunk_size = conf.chunk_size;
	res->db_info.mem_size = conf.mem_size;
	
	res->db_info.bitmap_size = hdr.bitmap_size;
	res->db_info.bitmap_start_index = hdr.bitmap_start_index;
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
	*(char *)(int_ptr + 1 ) = 1;
	
	/* write to file meta-info */
	fseek(res->db_info.fp, 0, SEEK_SET);
	fwrite(&hdr, sizeof(hdr), 1, res->db_info.fp);
	fwrite(res->db_info.bitmap, sizeof(char), hdr.bitmap_size, res->db_info.fp);
	
	/* write to file root node */
	write_block_to_file(res, root_block, hdr.root_id);
	
	return res;
}

/* We can not open db, if there no db with that name */
struct DB *dbopen (const char *file)
{
	struct DB *res;
	db_header hdr;
	char *bitmap;
	
	res = (struct DB *) malloc(sizeof(struct DB));
	
	res->db_info.fp = fopen(file, "r+");
	if (res->db_info.fp == NULL) {
		// file|db does not exist
		return NULL;
	}
	
	fread(&hdr, sizeof(hdr), 1, res->db_info.fp);
	bitmap = (char *)malloc(hdr.bitmap_size);
	fread(bitmap, sizeof(char), hdr.bitmap_size, res->db_info.fp);
	
	res->db_info.db_size = hdr.db_size;
	res->db_info.chunk_size = hdr.chunk_size;
	res->db_info.mem_size = hdr.mem_size;
	res->db_info.root_id = hdr.root_id;
	res->db_info.bitmap = bitmap;
	res->db_info.bitmap_size = hdr.bitmap_size;
	res->db_info.bitmap_start_index = hdr.bitmap_start_index;
	/* */
	res->db_info.root_node = (void *) malloc(hdr.chunk_size);
	read_block_from_file(res, res->db_info.root_node, hdr.root_id);
	
	return res;
}

int close(const struct DB *db){
	
	db_header hdr;
	
	hdr.db_size = db->db_info.db_size;
	hdr.chunk_size = db->db_info.chunk_size;
	hdr.mem_size = db->db_info.mem_size;
	hdr.root_id = db->db_info.root_id;
	hdr.bitmap_size = db->db_info.bitmap_size;
	hdr.bitmap_start_index = db->db_info.bitmap_start_index;
	 
	 /* write meta-info to file */
	fseek(db->db_info.fp, 0, SEEK_SET);
	fwrite(&hdr, sizeof(hdr), 1, db->db_info.fp);
	fwrite(db->db_info.bitmap, sizeof(char), hdr.bitmap_size, db->db_info.fp);
	
	/* write root block to file*/
	write_block_to_file(db, db->db_info.root_node, db->db_info.root_id);
	
	free(db->db_info.root_node);
	free(db->db_info.bitmap);
	free((void *)db);
	
	return fclose(db->db_info.fp);
}

/*
 *B-Tree-Search(x,k)
 * i = 1
 * while i <= x.n && k > x.key[i]
 * 	i = i + 1
 * if i <= x.n && k == x.key[i]
 * 	return (x,i)
 * elseif x.leaf
 * 	return NIL
 * else DISK-READ(x.c[i])
 * 	return B-Tree-Search(x.c[i],k) 
*/

int b_tree_search(const struct DB *db, void *node, struct DBT *key, struct DBT *data)
{
	
	int key_number;
	char is_leaf;
	int *key_lens;/* array of keys' length in bytes */
	int *value_lens;
	int *block_ids;
	void *key_i_ptr; /* pointer to current key */
	void *value_i_ptr;
	
	int i;
	int res;
	int cmp;
	void *child_node;
	
	key_number = *((int *) node);
	is_leaf = *(char *)(node + sizeof(int));
	key_lens = (int *)(node + sizeof(int) + sizeof(char));
	value_lens = key_lens + key_number;
	block_ids = value_lens + key_number;
	/* pointer to first key */
	key_i_ptr = (void *)(block_ids + key_number + 1);
	value_i_ptr = key_i_ptr;
	for(i = 0; i < key_number; i++) {
		value_i_ptr += key_lens[i];
	}
	
	/* algorithm of b-tree-search from Corman*/
	i = 0;
	while( i < key_number 
			&& (cmp = cmpkeys(key->data, key_i_ptr, key->size, key_lens[i])) > 0){
		key_i_ptr += key_lens[i];
		value_i_ptr += value_i_ptr[i];
		i++;
	}
	
	if (i < key_number && cmp == 0) {
		data->data = malloc(value_lens[i]);
		memcpy(data->data, key_i_ptr, key_lens[i]);		
	} else if (is_leaf) {
		return -1;
	} else {
		child_node = malloc(db->db_info.chunk_size);
		read_block_from_file(db, child_node, block_id[i]);
		res = b_tree_search(db, child_node, key, data);
		free(child_node);
		return res;
	}
	
	return 0;
}

int get(const struct DB *db, struct DBT *key, struct DBT *data)
{
	 return b_tree_search(db, db->db_info.root_node, key, data);
}



 
int main(int argc, char **argv) {
#define N 15
#define Kb *1000
#define Mb *1000000

	struct DBC dbc;
	struct DB *db;
	int i;//, j;
	int id[N];
	dbc.mem_size = 0;
	dbc.db_size = 1 Mb;
	dbc.chunk_size = 4 Kb;
	
	for (i = 0; i < N; i++) {
		if (!(db = dbcreate("testdb.db", dbc))) {
			db = dbopen("testdb.db");
		}
		printf("\nit = %d, root_id  = %d", i, db->db_info.root_id);
		close(db);
	}
	 
	if (!(db = dbcreate("testdb.db", dbc))) {
			db = dbopen("testdb.db");
	}
	
	for(i = 0; i < N; i++) {
		id[i] = block_allocate(db);
		printf("\nid = %d", id[i]);
	}
	
	for(i = 0; i < N; i++) {
		block_free(db, id[i]);
	}
	
	printf("\n------------------------");
	
	for(i = 0; i < N; i++) {
		id[i] = block_allocate(db);
		printf("\nid = %d", id[i]);
	}
	
	for(i = 0; i < N; i++) {
		block_free(db, id[i]);
	}
	
	close(db);
	 
	return 0;
}
