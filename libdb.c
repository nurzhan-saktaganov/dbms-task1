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
int block_allocate(const struct DB *db)
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

void read_block_from_file(const struct DB *db, void *block, int block_id)
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
	void *child_node;
	
	key_number = *((int *) node);
	is_leaf = *(char *)(node + sizeof(int));
	key_lens = (int *)(node + sizeof(int) + sizeof(char));
	value_lens = key_lens + key_number;
	block_ids = value_lens + key_number;
	/* pointer to first key */
	key_i_ptr = (void *)(block_ids + key_number + 1);
	/* pointer to first value */
	value_i_ptr = key_i_ptr;
	for(i = 0; i < key_number; i++) {
		value_i_ptr += key_lens[i];
	}
	
	/* algorithm of b-tree-search from Cormen*/
	i = 0;
	while( i < key_number 
			&& cmpkeys(key->data, key_i_ptr, key->size, key_lens[i]) > 0){
		key_i_ptr += key_lens[i];
		value_i_ptr += value_lens[i];
		i++;
	}
	
	if (i < key_number 
			&& cmpkeys(key->data, key_i_ptr, key->size, key_lens[i]) == 0){
		data->data = malloc(value_lens[i]);
		data->size = value_lens[i];
		memcpy(data->data, value_i_ptr, value_lens[i]);
		res = 0;
	} else if (is_leaf) {
		res = -1;
	} else {
		child_node = malloc(db->db_info.chunk_size);
		read_block_from_file(db, child_node, block_ids[i]);
		res = b_tree_search(db, child_node, key, data);
		free(child_node);
	}
	
	return res;
}

int get(const struct DB *db, struct DBT *key, struct DBT *data)
{
	 return b_tree_search(db, db->db_info.root_node, key, data);
}

int get_block_size(void *block)
{
	int block_size;
	int key_count;
	int *keys_size;
	int *values_size;
	int i;
	
	key_count = *(int *)(block);
	keys_size = (int *)(block + sizeof(int) + sizeof(char));
	values_size = keys_size + key_count;
	
	block_size = sizeof(char) + (2 + 3 * key_count) * sizeof(int);
	
	for(i = 0; i < key_count; i++)
	{
		block_size += keys_size[i];
		block_size += values_size[i];
	}
	
	return block_size;
}

/* m_leaf - modified leaf */
void add_to_leaf(const struct DB *db, void *leaf, struct DBT *key,
									struct DBT *data, void *m_leaf)
{
	int *leaf_key_count;
	int *m_leaf_key_count;
	/* */
	int *leaf_keys_size;
	int *m_leaf_keys_size;
	/* */
	int *leaf_values_size;
	int *m_leaf_values_size;
	/* */
	void *leaf_keys;
	void *m_leaf_keys;
	/* */
	void *leaf_values;
	void *m_leaf_values;
	
	int i;
	
	leaf_key_count = (int *)leaf;
	m_leaf_key_count = (int *)m_leaf;
	/* key number and sign of leaf */
	*m_leaf_key_count = *leaf_key_count + 1;
	*(char *)(m_leaf + sizeof(int)) = (char) 1;
	leaf_keys_size = (int *)(leaf + sizeof(int) + sizeof(char));
	m_leaf_keys_size = (int *)(m_leaf + sizeof(int) + sizeof(char));
	/* */
	leaf_values_size = leaf_keys_size + *leaf_key_count;
	m_leaf_values_size = m_leaf_keys_size + *m_leaf_key_count;	
	/* setting leaf_keys and m_leaf_keys pointers */
	leaf_keys = (leaf + sizeof(char) + (2 + 3 * (*leaf_key_count)) * sizeof(int));
	m_leaf_keys = (m_leaf + sizeof(char) + (2 + 3 * (*m_leaf_key_count)) * sizeof(int));
	
	/* setting leaf_values and m_leaf_values pointers */
	leaf_values = leaf_keys;
	m_leaf_values = m_leaf_keys;
	
	for(i = 0; i < *leaf_key_count; i++)
	{
		leaf_values += leaf_keys_size[i];
		m_leaf_values += leaf_keys_size[i];
	}
	
	m_leaf_values += key->size;
	
	/* */
	i = 0;
	while(i < *leaf_key_count && cmpkeys(key->data, leaf_keys, key->size, leaf_keys_size[i]) > 0)
	{
		memcpy(m_leaf_keys, leaf_keys, leaf_keys_size[i]);
		memcpy(m_leaf_values, leaf_values, leaf_values_size[i]);
		m_leaf_keys_size[i] = leaf_keys_size[i];
		m_leaf_values_size[i] = leaf_values_size[i];
		leaf_keys += leaf_keys_size[i];
		m_leaf_keys += leaf_keys_size[i];
		leaf_values += leaf_values_size[i];
		m_leaf_values += leaf_values_size[i];
		i++;
	}
	
	memcpy(m_leaf_keys, key->data, key->size);
	memcpy(m_leaf_values, data->data, data->size);
	m_leaf_keys_size[i] = key->size;
	m_leaf_keys += key->size;
	m_leaf_values_size[i] = data->size;
	m_leaf_values += data->size;
	
	while(i < *leaf_key_count)
	{
		memcpy(m_leaf_keys, leaf_keys, leaf_keys_size[i]);
		memcpy(m_leaf_values, leaf_values, leaf_values_size[i]);
		m_leaf_keys_size[i + 1] = leaf_keys_size[i];
		m_leaf_values_size[i + 1] = leaf_values_size[i];
		leaf_keys += leaf_keys_size[i];
		m_leaf_keys += leaf_keys_size[i];
		leaf_values += leaf_values_size[i];
		m_leaf_values += leaf_values_size[i];
		i++;
	}

	return;
}

int get_child_block_id_to_insert(const struct DB *db, void *node, struct DBT *key)
{
	int child_block_id;
	int key_count;
	int *keys_size;
	int *blocks_id;
	void *keys;
	int i;
	
	key_count = *(int *)(node);
	keys_size = (int *)(node + sizeof(int) + sizeof(char));
	blocks_id = keys_size + 2 * key_count;
	keys = (void *)(blocks_id + key_count + 1);
	
	i = 0;
	while( i < key_count && cmpkeys(key->data, keys, key->size, keys_size[i]) > 0)
	{
		keys += keys_size[i];
		i++;
	}
	
	child_block_id = blocks_id[i];
	
	return child_block_id;
}

int get_split_point(const struct DB *db, void *node)
{
	int split_point;
	int key_count;
	int *keys_size;
	int *values_size;
	int left_size;
	int i;
		
	key_count = *(int *)(node);
	keys_size = (int *)(node + sizeof(int) + sizeof(char));
	values_size = keys_size + key_count;
	
	left_size = 2 * sizeof(int) + sizeof(char);
	
	i = 0;
	while( left_size < (db->db_info.chunk_size / 2) )
	{
		left_size += 3 * sizeof(int);
		left_size += keys_size[i];
		left_size += values_size[i];
		i++;
	}
	
	split_point = i;
	return split_point;
}

void split_child(const struct DB *db, void *parent, void *child, int child_block_id)
{
	/*t_parent - tmp parent*/
	void *t_parent;
	//int child_block_index;
	int t_parent_size;
	/* key count */
	int *parent_key_count;
	int *t_parent_key_count;
	/* keys size */
	int *parent_keys_size;
	int *t_parent_keys_size;
	/* values size */
	int *parent_values_size;
	int *t_parent_values_size;
	/* blocks id */
	int *parent_blocks_id;
	int *t_parent_blocks_id;
	/* keys */
	void *parent_keys;
	void *t_parent_keys;
	/* values */
	void *parent_values;
	void *t_parent_values;
	int i;
	int split_point;
	/* child - child1, child2 */
	void *child1;
	void *child2;
	int child2_block_id;
	/* key count */
	int *child_key_count;
	int *child1_key_count;
	int *child2_key_count;
	/* keys size */
	int *child_keys_size;
	int *child1_keys_size;
	int *child2_keys_size;
	/* values size */
	int *child_values_size;
	int *child1_values_size;
	int *child2_values_size;
	/* blocks id */
	int *child_blocks_id;
	int *child1_blocks_id;
	int *child2_blocks_id;
	/* keys */
	void *child_keys;
	void *child1_keys;
	void *child2_keys;
	/* values */
	void *child_values;
	void *child1_values;
	void *child2_values;
	/* */
	void *keys_from;
	void *values_from;
	
	/* split child into child1 and child2*/
	{
		split_point = get_split_point(db, child);
		child1 = malloc(db->db_info.chunk_size);
		child2 = malloc(db->db_info.chunk_size);
		
		child_key_count = (int *) child;
		child1_key_count = (int *) child1;
		child2_key_count = (int *) child2;
		
		*child1_key_count = split_point;
		*child2_key_count = *child_key_count - split_point - 1;
		
		/* sign of leaf */
		*(char *)(child1 + sizeof(int)) = *(char *)(child + sizeof(int));
		*(char *)(child2 + sizeof(int)) = *(char *)(child + sizeof(int));
		/* keys size */
		child_keys_size = (int *)(child + sizeof(int) + sizeof(char));
		child1_keys_size = (int *)(child1 + sizeof(int) + sizeof(char));
		child2_keys_size = (int *)(child2 + sizeof(int) + sizeof(char));
		/* values size */
		child_values_size = child_keys_size + *child_key_count;
		child1_values_size = child1_keys_size + *child1_key_count;
		child2_values_size = child2_keys_size + *child2_key_count;
		/* blocks id */
		child_blocks_id = child_values_size + *child_key_count;
		child1_blocks_id = child1_values_size + *child1_key_count;
		child2_blocks_id = child2_values_size + *child2_key_count;
		/* keys */
		child_keys = (void *)(child_blocks_id + *child_key_count + 1);
		child1_keys = (void *)(child1_blocks_id + *child1_key_count + 1);
		child2_keys = (void *)(child2_blocks_id + *child2_key_count + 1);
		/* values */
		child_values = child_keys;
		child1_values = child1_keys;
		child2_values = child2_keys;
		
		for (i = 0; i < split_point; i++)
		{
			child1_keys_size[i] = child_keys_size[i];
			child1_values_size[i] = child_values_size[i];
			child1_blocks_id[i] = child_blocks_id[i];
			child_values += child_keys_size[i];
			child1_values += child_keys_size[i];
		}
		
		child_blocks_id[split_point] = child_blocks_id[split_point];
		child_values += child_keys_size[i];
		//child_values += child_keys_size[split_child];
		
		for(i = split_point + 1; i < *child_key_count; i++)
		{
			child2_keys_size[i - split_point - 1] = child_keys_size[i];
			child2_values_size[i - split_point - 1] = child_values_size[i];
			child2_blocks_id[i - split_point - 1] = child_blocks_id[i];
			child_values += child_keys_size[i];
			child2_values += child_keys_size[i];
		}
		
		child2_blocks_id[*child_key_count - split_point - 1] = child_blocks_id[*child_key_count];
		
		/* copy keys and values */
		for(i = 0; i < split_point; i++)
		{
			memcpy(child1_keys, child_keys, child_keys_size[i]);
			memcpy(child1_values, child_values, child_values_size[i]);
			child_keys += child_keys_size[i];
			child1_keys += child_keys_size[i];
			child_values += child_values_size[i];
			child1_values += child_values_size[i];
		}
	
		keys_from = child_keys;
		values_from = child_values;
		
		child_keys += child_keys_size[split_point];
		child_values += child_values_size[split_point];
		
		for(i = split_point + 1; i < *child_key_count; i++)
		{
			memcpy(child2_keys, child_keys, child_keys_size[i]);
			memcpy(child2_values, child_values, child_values_size[i]);
			child_keys += child_keys_size[i];
			child2_keys += child_keys_size[i];
			child_values += child_values_size[i];
			child2_values += child_values_size[i];
		}
		
		write_block_to_file(db, child1, child_block_id);
		child2_block_id = block_allocate(db);
		write_block_to_file(db, child2, child2_block_id);
		free(child1);
		free(child2);
	}
	
	
	/* modify parent */
	{
		t_parent_size = 2 * db->db_info.chunk_size;
		t_parent = malloc(t_parent_size);	
		parent_key_count = (int *) parent;
		t_parent_key_count = (int *) t_parent;
		*t_parent_key_count = *parent_key_count + 1;
		*(char *)(t_parent + sizeof(int)) = *(char *)(parent + sizeof(int));
		/* keys size */
		parent_keys_size = (int *)(parent + sizeof(int) + sizeof(char));
		t_parent_keys_size = (int *)(t_parent + sizeof(int) + sizeof(char));
		/* values size */
		parent_values_size = parent_keys_size + *parent_key_count;
		t_parent_values_size = t_parent_keys_size + *t_parent_key_count;
		/* blocks id */
		parent_blocks_id = parent_values_size + *parent_key_count;
		t_parent_blocks_id = t_parent_values_size + *t_parent_key_count;
		/* keys */
		parent_keys = (void *)(parent_blocks_id + *parent_key_count + 1);
		t_parent_keys = (void *)(t_parent_blocks_id + *t_parent_key_count + 1);
		/* values */
		parent_values = parent_keys;
		t_parent_values = t_parent_keys;
	
		for(i = 0; i < *parent_key_count; i++) 
		{
			parent_values += parent_keys_size[i];
			t_parent_values += parent_keys_size[i];
		}
	
		t_parent_values += child_keys_size[split_point];
		
		/* */
		i = 0;
		while( i < *parent_key_count
					&& cmpkeys(keys_from, parent_keys, child_keys_size[split_point], parent_keys_size[i]) > 0)
		{
			memcpy(t_parent_keys, parent_keys, parent_keys_size[i]);
			memcpy(t_parent_values, parent_values, parent_values_size[i]);
			parent_keys += parent_keys_size[i];
			t_parent_keys += parent_keys_size[i];
			parent_values += parent_values_size[i];
			t_parent_values += parent_values_size[i];
			t_parent_keys_size[i] = parent_keys_size[i];
			t_parent_values_size[i] = parent_values_size[i];
			t_parent_blocks_id[i] = parent_blocks_id[i];
			i++;
		}
		
		memcpy(t_parent_keys, keys_from, child_keys_size[split_point]);
		memcpy(t_parent_values, values_from, child_values_size[split_point]);
		t_parent_keys_size[i] = child_keys_size[split_point];
		t_parent_values_size[i] = child_values_size[split_point];
		t_parent_blocks_id[i] = child_block_id;
		printf("child_block_id = %d, parent_blocks_id[i] = %d, should be equal\n", child_block_id, parent_blocks_id[i]);
		t_parent_blocks_id[i + 1] = child2_block_id;
		t_parent_keys += child_keys_size[split_point];
		t_parent_values += child_values_size[split_point];
		
		while( i < *parent_key_count )
		{
			memcpy(t_parent_keys, parent_keys, parent_keys_size[i]);
			memcpy(t_parent_values, parent_values, parent_values_size[i]);
			parent_keys += parent_keys_size[i];
			t_parent_keys += parent_keys_size[i];
			parent_values += parent_values_size[i];
			t_parent_values += parent_values_size[i];
			t_parent_keys_size[i + 1] = parent_keys_size[i];
			t_parent_values_size[i + 1] = parent_values_size[i];
			t_parent_blocks_id[i + 2] = parent_blocks_id[i + 1];
			i++;
		}

		memcpy(parent, t_parent, t_parent_size);
		free(t_parent);
	}
	
	return;
}




int b_tree_insert(const struct DB *db, void *node, struct DBT *key,
									struct DBT *data, void *modified_parent, int block_id)
{	
	char is_leaf;
	void *modified_me;
	void *child_block;
	int child_block_id;
	int i_am_modified = 0;
	int modified_me_size;	
	
	modified_me_size = 2 * db->db_info.chunk_size;
	
	is_leaf = *(char *)(node + sizeof(int));
	modified_me = malloc(modified_me_size);
	memcpy(modified_me, node, db->db_info.chunk_size);
	
	if(is_leaf) {
		add_to_leaf(db, node, key, data, modified_me);
		i_am_modified = 1;
	} else {
		child_block = malloc(db->db_info.chunk_size);
		child_block_id = get_child_block_id_to_insert(db, node, key);
		read_block_from_file(db, child_block, child_block_id);
		i_am_modified = b_tree_insert(db, child_block, key, data, modified_me, child_block_id);
		free(child_block);
	}
	
	if(! i_am_modified){
		free(modified_me);
		return 0;
	}
	
	
	if( get_block_size(modified_me) <= db->db_info.chunk_size) {
		memcpy(node, modified_me, db->db_info.chunk_size);
		write_block_to_file(db, node, block_id);
		free(modified_me);
		return 0;
	}

	split_child(db, modified_parent, modified_me, block_id);
	
	free(modified_me);
	return 1;
}

int put(const struct DB *db_in, struct DBT *key, struct DBT *data)
{
	void *pseudo_root;
	int i_am_modified;
	int pseudo_root_size;
	int *key_count;
	char *is_leaf;
	int *child_id;
	int new_block_id;
	struct DB *db = db_in;
	
	pseudo_root_size = 2 * db->db_info.chunk_size;
	pseudo_root = malloc(pseudo_root_size);
	key_count = (int *)pseudo_root;
	is_leaf = (char *)(pseudo_root + sizeof(int));
	child_id = (int *)(pseudo_root + sizeof(int) + sizeof(char));
	
	*key_count = 0;
	*is_leaf = 0;
	*child_id = db->db_info.root_id;
	
	i_am_modified = b_tree_insert(db, db->db_info.root_node, key, data,
										pseudo_root, db->db_info.root_id);
							
	if(i_am_modified) {
		memcpy(db->db_info.root_node, pseudo_root, db->db_info.chunk_size);
		new_block_id = block_allocate(db);
		write_block_to_file(db, db->db_info.root_node, new_block_id);
		db->db_info.root_id = new_block_id;
	}
	
	free(pseudo_root);
	return 0;
}
 
int main(int argc, char **argv) {
#define N 15000
#define Kb *1000
#define Mb *1000000

	struct DBC dbc;
	struct DB *db;
	int i, j;
	struct DBT key;
	struct DBT data;
	int clos;
	int choose;

	key.data = malloc(sizeof(int));
	key.size = sizeof(int);
	data.data = malloc(sizeof(int));
	data.size = sizeof(int);
	
	dbc.mem_size = 0;
	dbc.db_size = 1 Mb;
	dbc.chunk_size = 4 Kb;
	 
	if (!(db = dbcreate("testdb.db", dbc))) {
			db = dbopen("testdb.db");
	}
	//printf("close?1/0:");
	//scanf("%d", &clos);
	for(i = 0; i < N; i++) {
		*(int *)(key.data) = i;
		*(int *)(data.data) = i;
		put(db, &key, &data);
	}
	j = 0;
	for(i = -10; i < N; i++) {
		*(int *)(key.data) = i;
		if (!get(db, &key, &data))
		{
			;//printf("k, v: %d, %d", *(int *)(key.data), *(int *)(data.data));
			if(*(int *)(key.data)!= *(int *)(data.data))
				printf("alert!!!\n");
			j++;
		} else
			printf("not found\n");
	}
	
	printf("j = %d\n", j);
	
	close(db);
	unlink("testdb.db");
	 
	return 0;
}
