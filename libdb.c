
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
	
	//return *(int *)(key1) - *(int *)(key2);
	
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
long int block_offset_in_file(struct MY_DB *db, int block_id)
{
	long int offset = block_id;
	return offset *= db->db_info.chunk_size;
}

/* returns id of allocated block */
/* numeration from 0 */
int block_allocate(struct MY_DB *db)
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
void block_free(struct MY_DB *db, int block_id)
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

void write_block_to_file(struct MY_DB *db, void *block, int block_id)
{
	long int offset = block_offset_in_file(db, block_id);
	fseek(db->db_info.fp, offset, SEEK_SET);
	fwrite(block, db->db_info.chunk_size, 1, db->db_info.fp);
	return;
}

void read_block_from_file(struct MY_DB *db, void *block, int block_id)
{
	long int offset = block_offset_in_file(db, block_id);
	fseek(db->db_info.fp, offset, SEEK_SET);
	fread(block, db->db_info.chunk_size, 1, db->db_info.fp);
	return;
}


/* We can not create db, if db with same name already exists */
struct DB *dbcreate(const char *file, struct DBC confg)
{
	struct MY_DB *res;
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
	
	res = (struct MY_DB *) malloc(sizeof(struct MY_DB));
	/* just for good form :) */
	memset( res, 0, sizeof(struct MY_DB));
		
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
	//hdr.mem_size = conf.mem_size;
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
	//res->db_info.mem_size = conf.mem_size;
	
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
	*(char *)(int_ptr + 1 ) = (char) 1;
	
	/* write to file meta-info */
	fseek(res->db_info.fp, 0, SEEK_SET);
	fwrite(&hdr, sizeof(hdr), 1, res->db_info.fp);
	fwrite(res->db_info.bitmap, sizeof(char), hdr.bitmap_size, res->db_info.fp);
	
	/* write to file root node */
	write_block_to_file(res, root_block, hdr.root_id);
	
	/* init functions */
	res->close = close;
	res->del = del;
	res->get = get;
	res->put = put;
	
	return (struct DB *)res;
}

/* We can not open db, if there no db with that name */
struct DB *dbopen (const char *file)
{
	struct MY_DB *res;
	db_header hdr;
	char *bitmap;
	
	res = (struct MY_DB *) malloc(sizeof(struct MY_DB));
	
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
	
	/* init functions */
	res->close = close;
	res->del = del;
	res->get = get;
	res->put = put;
	
	return (struct DB *)res;
}

int close(struct DB *db_in){
	
	db_header hdr;
	struct MY_DB *db = (struct MY_DB *) db_in;
	
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


int b_tree_search(struct MY_DB *db, void *node, struct DBT *key, struct DBT *data)
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

int get(struct DB *db_in, struct DBT *key, struct DBT *data)
{
	struct MY_DB *db = (struct MY_DB *) db_in;
	 return b_tree_search(db, db->db_info.root_node, key, data);
}

/* insert */

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
void add_to_leaf(struct MY_DB *db, void *leaf, struct DBT *key,
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
	//printf("add_to_leaf\n");
	leaf_key_count = (int *)leaf;
	m_leaf_key_count = (int *)m_leaf;
	/* key number and sign of leaf */
	*m_leaf_key_count = *leaf_key_count + 1;
	*(char *)(m_leaf + sizeof(int)) = *(char *)(leaf + sizeof(int));
	leaf_keys_size = (int *)(leaf + sizeof(int) + sizeof(char));
	m_leaf_keys_size = (int *)(m_leaf + sizeof(int) + sizeof(char));
	/* */
	leaf_values_size = leaf_keys_size + *leaf_key_count;
	m_leaf_values_size = m_leaf_keys_size + *m_leaf_key_count;	
	/* setting leaf_keys and m_leaf_keys pointers */
	leaf_keys = leaf_values_size + 2 * *leaf_key_count + 1;
	m_leaf_keys = m_leaf_values_size + 2 * *m_leaf_key_count + 1;
	
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

int get_child_block_id_to_insert(struct MY_DB *db, void *node, struct DBT *key)
{
	int child_block_id;
	int key_count;
	int *keys_size;
	int *blocks_id;
	void *keys;
	int i;
	//printf("get_child_block_id\n");
	key_count = *(int *)(node);
	keys_size = (int *)(node + sizeof(int) + sizeof(char));
	blocks_id = keys_size + 2 * key_count;
	keys = (void *)(blocks_id + key_count + 1);
	
	i = 0;
	//printf("key_count %d\n", key_count);
	while( i < key_count && cmpkeys(key->data, keys, key->size, keys_size[i]) > 0)
	{
		keys += keys_size[i];
		i++;
	}
	
	//printf("exit");
	child_block_id = blocks_id[i];
	return child_block_id;
}

int get_split_point(struct MY_DB *db, void *node)
{
	int split_point;
	int key_count;
	int *keys_size;
	int *values_size;
	int left_size;
	int i;
	//printf("get_split_point");
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

void split_child(struct MY_DB *db, void *parent, void *child, int child_block_id)
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
	int i, j;
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

	//printf("split child\n");
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
		
		child1_blocks_id[split_point] = child_blocks_id[split_point];
		child_values += child_keys_size[split_point];
		
		for(i = split_point + 1; i < *child_key_count; i++)
		{
			j = i - split_point - 1;
			child2_keys_size[j] = child_keys_size[i];
			child2_values_size[j] = child_values_size[i];
			child2_blocks_id[j] = child_blocks_id[i];
			child_values += child_keys_size[i];
			child2_values += child_keys_size[i];
		}
		
		child2_blocks_id[*child2_key_count] = child_blocks_id[*child_key_count];
		
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
		//printf("child_block_id = %d, parent_blocks_id[i] = %d, should be equal\n", child_block_id, parent_blocks_id[i]);
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

int b_tree_insert(struct MY_DB *db, void *node, struct DBT *key,
					struct DBT *data, void *modified_parent, int block_id)
{	
	char is_leaf;
	void *modified_me;
	void *child_block;
	int child_block_id;
	int i_am_modified = 0;
	int modified_me_size;
	//printf("b_tree_insert\n");
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

int put(struct DB *db_in, struct DBT *key, struct DBT *data)
{
	struct MY_DB *db = (struct MY_DB *) db_in;
	void *pseudo_root;
	int i_am_modified;
	int pseudo_root_size;
	int *key_count;
	char *is_leaf;
	int *child_id;
	int new_block_id;
	//printf("put\n");
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


int try_shift_left(struct MY_DB *db, void *node, void *l_child, int l_child_pos)
{
	void *r_child;
	int r_child_pos;
	/* size */
	int *n_key_count;
	int *l_key_count;
	int *r_key_count;
	/* sign of leaf is not required */
	/* key sizes */
	int *n_key_sizes;
	int *l_key_sizes;
	int *r_key_sizes;
	/* value sizes */
	int *n_value_sizes;
	int *l_value_sizes;
	int *r_value_sizes;
	/* block ids*/
	int *n_block_ids;
	int *l_block_ids;
	int *r_block_ids;
	/* keys */
	void *n_keys;
	void *l_keys;
	void *r_keys;
	/* values */
	void *n_values;
	void *l_values;
	void *r_values;
	
	int success;
	int i;
	/* middle key and value */
	int mid_key_size;
	int mid_value_size;
	void *mid_key_src;
	void *mid_value_src;
	
	int l_predicted_size;
	int r_predicted_size;
	int shift_count;
	
	int l_block_id;
	int r_block_id;
	
	int first_key_pos;
	
	//printf("begin\n");
	/* init parent params - ok */
	{
		n_key_count = (int *) node;
		n_key_sizes = (int *)(node + sizeof(int) + sizeof(char));
		n_value_sizes = n_key_sizes + *n_key_count;
		n_block_ids = n_value_sizes + *n_key_count;
		n_keys = (void *)(n_block_ids + *n_key_count + 1);
		n_values = n_keys;
		
		for(i = 0; i < *n_key_count; i++) {
			n_values += n_key_sizes[i];
		}
		
		mid_key_src = n_keys;
		mid_value_src = n_values;
		
		for(i = 0; i < l_child_pos; i++) {
			mid_key_src += n_key_sizes[i];
			mid_value_src += n_value_sizes[i];
		}
	}
	
	
	/* load right child - ok */
	{
		r_child = malloc(db->db_info.chunk_size);
		r_child_pos = l_child_pos + 1;
		l_block_id = n_block_ids[l_child_pos];
		r_block_id = n_block_ids[r_child_pos];
		read_block_from_file(db, r_child, r_block_id);
	}
	
	/* init left child and right child params - ok */
	{
		/* key count */
		l_key_count = (int *) l_child;
		r_key_count = (int *) r_child;
		/* key sizes*/
		l_key_sizes = (int *)(l_child + sizeof(int) + sizeof(char));
		r_key_sizes = (int *)(r_child + sizeof(int) + sizeof(char));
		/* values sizes */
		l_value_sizes = l_key_sizes + *l_key_count;
		r_value_sizes = r_key_sizes + *r_key_count;
		/* block ids */
		l_block_ids = l_value_sizes + *l_key_count;
		r_block_ids = r_value_sizes + *r_key_count;
		/* keys */
		l_keys = (void *)(l_block_ids + *l_key_count + 1);
		r_keys = (void *)(r_block_ids + *r_key_count + 1);
		/* values */
		l_values = l_keys;
		r_values = r_keys;
		
		for(i = 0; i < *l_key_count; i++){
			l_values += l_key_sizes[i];
		}
		
		for(i = 0; i < *r_key_count; i++){
			r_values += r_key_sizes[i];
		}
	}
	
	/* pre-calculation - seems ok */
	{
		/* middle key size */
		mid_key_size = n_key_sizes[r_child_pos - 1];
		mid_value_size = n_value_sizes[r_child_pos - 1];
		
		l_predicted_size = get_block_size(l_child);
		r_predicted_size = get_block_size(r_child);
		
				
		first_key_pos = 0;
		
		l_predicted_size += mid_key_size + mid_value_size + 3 * sizeof(int);
		r_predicted_size -= r_key_sizes[first_key_pos] + r_value_sizes[first_key_pos] + 3 * sizeof(int);
		first_key_pos++;
		
		while(l_predicted_size < db->db_info.chunk_size / 2 && first_key_pos < *r_key_count - 1)
		{
			l_predicted_size += r_key_sizes[first_key_pos - 1] + 3 * sizeof(int);
			l_predicted_size += r_value_sizes[first_key_pos - 1];
			r_predicted_size -= r_key_sizes[first_key_pos] + 3 * sizeof(int);
			r_predicted_size -= r_value_sizes[first_key_pos];
			first_key_pos++;
		}
		
		if(r_predicted_size < db->db_info.chunk_size / 2){
			success = 0;
		} else {
			success = 1;
		}
	}
	
	if(!success || first_key_pos < 2)
	{
		free(r_child);
		return 0;
	}
	
	/* shift right for shift_count position */	
	{
		/* tmp node, left child and right child */
		void *t_node;
		void *tl_child;
		void *tr_child;
		/* size */
		int *tn_key_count;
		int *tl_key_count;
		int *tr_key_count;
		/* sign of leaf is not required */
		/* key sizes */
		int *tn_key_sizes;
		int *tl_key_sizes;
		int *tr_key_sizes;
		/* value sizes */
		int *tn_value_sizes;
		int *tl_value_sizes;
		int *tr_value_sizes;
		/* block ids*/
		int *tn_block_ids;
		int *tl_block_ids;
		int *tr_block_ids;
		/* keys */
		void *tn_keys;
		void *tl_keys;
		void *tr_keys;
		/* values */
		void *tn_values;
		void *tl_values;
		void *tr_values;
		
		int j;
		
		t_node = malloc(2 * db->db_info.chunk_size);
		tl_child = malloc(db->db_info.chunk_size);
		tr_child = malloc(db->db_info.chunk_size);
		/* init params */
		shift_count = first_key_pos;
		/* key count */
		tn_key_count = (int *) t_node;
		*tn_key_count = *n_key_count;
		tl_key_count = (int *) tl_child;
		*tl_key_count = *l_key_count + shift_count;
		tr_key_count = (int *) tr_child;
		*tr_key_count = *r_key_count - shift_count;
		/* leaf sign */
		*(char *)(t_node + sizeof(int)) = *(char *)(node + sizeof(int));
		*(char *)(tl_child + sizeof(int)) = *(char *)(l_child + sizeof(int));
		*(char *)(tr_child + sizeof(int)) = *(char *)(r_child + sizeof(int));
		/* key sizes*/
		tn_key_sizes = (int *)(t_node + sizeof(int) + sizeof(char));
		tl_key_sizes = (int *)(tl_child + sizeof(int) + sizeof(char));
		tr_key_sizes = (int *)(tr_child + sizeof(int) + sizeof(char));
		/* values sizes */
		tn_value_sizes = tn_key_sizes + *tn_key_count;
		tl_value_sizes = tl_key_sizes + *tl_key_count;
		tr_value_sizes = tr_key_sizes + *tr_key_count;
		/* block ids */
		tn_block_ids = tn_value_sizes + *tn_key_count;
		tl_block_ids = tl_value_sizes + *tl_key_count;
		tr_block_ids = tr_value_sizes + *tr_key_count;
		/* keys */
		tn_keys = (void *)(tn_block_ids + *tn_key_count + 1);
		tl_keys = (void *)(tl_block_ids + *tl_key_count + 1);
		tr_keys = (void *)(tr_block_ids + *tr_key_count + 1);
		/* values */
		tn_values = tn_keys;
		tl_values = tl_keys;
		tr_values = tr_keys;
		
		/* t_left */
		for(i = 0; i < *l_key_count; i++)
		{
			tl_key_sizes[i] = l_key_sizes[i];
			tl_value_sizes[i] = l_value_sizes[i];
			tl_block_ids[i] = l_block_ids[i];
			tl_values += l_key_sizes[i];
		}
		
		tl_block_ids[*l_key_count] = l_block_ids[*l_key_count];
		tl_key_sizes[*l_key_count] = mid_key_size;
		tl_value_sizes[*l_key_count] = mid_value_size;
		tl_values += mid_key_size;
		
		for(i = 0; i < first_key_pos - 1; i++)
		{
			j = *l_key_count + 1 + i;
			tl_key_sizes[j] = r_key_sizes[i];
			tl_value_sizes[j] = r_value_sizes[i];
			tl_block_ids[j] = r_block_ids[i];
			tl_values += r_key_sizes[i];
		}
		
		tl_block_ids[*tl_key_count] = r_block_ids[first_key_pos - 1];
		
		/* t_node */
		for(i = 0; i < *tn_key_count; i++)
		{
			tn_key_sizes[i] = n_key_sizes[i];
			tn_value_sizes[i] = n_value_sizes[i];
			tn_block_ids[i] = n_block_ids[i];
			tn_values += n_key_sizes[i];
		}
		
		tn_block_ids[*tn_key_count] = n_block_ids[*tn_key_count];
				
		tn_key_sizes[r_child_pos - 1] = r_key_sizes[first_key_pos - 1];
		tn_value_sizes[r_child_pos - 1] = r_value_sizes[first_key_pos - 1];
		tn_values -= n_key_sizes[r_child_pos - 1];
		tn_values += r_key_sizes[first_key_pos - 1];
		
		/* t _right */
		
		for( i = first_key_pos; i < *r_key_count; i++)
		{
			j = i - first_key_pos;
			tr_key_sizes[j] = r_key_sizes[i];
			tr_value_sizes[j] = r_value_sizes[i];
			tr_block_ids[j] = r_block_ids[i];
			tr_values += r_key_sizes[i];
		}
		
		tr_block_ids[*tr_key_count] = r_block_ids[*r_key_count];
		
		/* above seems ok */
		/* copy to tmp nodes */
		/* t_left */
		//printf("t_left\n");
		for(i = 0; i < *l_key_count; i++)
		{
			memcpy(tl_keys, l_keys, l_key_sizes[i]);
			memcpy(tl_values, l_values, l_value_sizes[i]);
			l_keys += l_key_sizes[i];
			tl_keys += l_key_sizes[i];
			l_values += l_value_sizes[i];
			tl_values += l_value_sizes[i];
		}
		
		memcpy(tl_keys, mid_key_src, mid_key_size);
		memcpy(tl_values, mid_value_src, mid_value_size);
		tl_keys += mid_key_size;
		tl_values += mid_value_size;
		
		for(i = 0; i < first_key_pos - 1; i++)
		{
			memcpy(tl_keys, r_keys, r_key_sizes[i]);
			memcpy(tl_values, r_values, r_value_sizes[i]);
			tl_keys += r_key_sizes[i];
			r_keys += r_key_sizes[i];
			tl_values += r_value_sizes[i];
			r_values += r_value_sizes[i];
		}
		
		/* t_node */
		
		for(i = 0; i < r_child_pos - 1; i++)
		{
			memcpy(tn_keys, n_keys, n_key_sizes[i]);
			memcpy(tn_values, n_values, n_value_sizes[i]);
			n_keys += n_key_sizes[i];
			tn_keys += n_key_sizes[i];
			n_values += n_value_sizes[i];
			tn_values += n_value_sizes[i];
		}
		
		memcpy(tn_keys, r_keys, r_key_sizes[first_key_pos - 1]);
		memcpy(tn_values, r_values, r_value_sizes[first_key_pos - 1]);
		r_keys += r_key_sizes[first_key_pos - 1];
		tn_keys += r_key_sizes[first_key_pos - 1];
		r_values += r_value_sizes[first_key_pos - 1];
		tn_values += r_value_sizes[first_key_pos - 1];
		n_keys += mid_key_size;
		n_values += mid_value_size;
	
		for(i = r_child_pos; i < *n_key_count; i++)
		{
			memcpy(tn_keys, n_keys, n_key_sizes[i]);
			memcpy(tn_values, n_values, n_value_sizes[i]);
			n_keys += n_key_sizes[i];
			tn_keys += n_key_sizes[i];
			n_values += n_value_sizes[i];
			tn_values += n_value_sizes[i];
		}
		
		/* t_right */
		//return 0;
		for( i = first_key_pos; i < *r_key_count ; i++)
		{
			memcpy(tr_keys, r_keys, r_key_sizes[i]);
			memcpy(tr_values, r_values, r_value_sizes[i]);
			r_keys += r_key_sizes[i];
			tr_keys += r_key_sizes[i];
			r_values += r_value_sizes[i];
			tr_values += r_value_sizes[i];
		}

		memcpy(node, t_node, 2 * db->db_info.chunk_size);
		memcpy(l_child, tl_child, db->db_info.chunk_size);
		memcpy(r_child, tr_child, db->db_info.chunk_size);
		
		write_block_to_file(db, l_child, l_block_id);
		write_block_to_file(db, r_child, r_block_id);
		
		free(t_node);
		free(tl_child);
		free(tr_child);
	}

	free(r_child);	
	return success;
}

/* it means child actual size < chunk_size / 2 */
int try_shift_right(struct MY_DB *db, void *node, void *r_child, int r_child_pos)
{
	void *l_child;
	int l_child_pos;
	/* size */
	int *n_key_count;
	int *l_key_count;
	int *r_key_count;
	/* sign of leaf is not required */
	/* key sizes */
	int *n_key_sizes;
	int *l_key_sizes;
	int *r_key_sizes;
	/* value sizes */
	int *n_value_sizes;
	int *l_value_sizes;
	int *r_value_sizes;
	/* block ids*/
	int *n_block_ids;
	int *l_block_ids;
	int *r_block_ids;
	/* keys */
	void *n_keys;
	void *l_keys;
	void *r_keys;
	/* values */
	void *n_values;
	void *l_values;
	void *r_values;
	
	int success;
	int i;
	/* middle key and value */
	int mid_key_size;
	int mid_value_size;
	void *mid_key_src;
	void *mid_value_src;
	
	int l_predicted_size;
	int r_predicted_size;
	int shift_count;
	
	int l_block_id;
	int r_block_id;
	
	int last_key_pos;
		
	//printf("try_shift_right\n");
	
	/* init parent params - ok */
	{
		n_key_count = (int *) node;
		n_key_sizes = (int *)(node + sizeof(int) + sizeof(char));
		n_value_sizes = n_key_sizes + *n_key_count;
		n_block_ids = n_value_sizes + *n_key_count;
		n_keys = (void *)(n_block_ids + *n_key_count + 1);
		n_values = n_keys;
		
		for(i = 0; i < *n_key_count; i++) {
			n_values += n_key_sizes[i];
		}
	}
	
	/* load left child - ok */
	{
		l_child = malloc(db->db_info.chunk_size);
		l_child_pos = r_child_pos - 1;
		l_block_id = n_block_ids[l_child_pos];
		r_block_id = n_block_ids[r_child_pos];
		read_block_from_file(db, l_child, l_block_id);
	}
	
	/* init left child and right child params - ok */
	{
		/* key count */
		l_key_count = (int *) l_child;
		r_key_count = (int *) r_child;
		/* key sizes*/
		l_key_sizes = (int *)(l_child + sizeof(int) + sizeof(char));
		r_key_sizes = (int *)(r_child + sizeof(int) + sizeof(char));
		/* values sizes */
		l_value_sizes = l_key_sizes + *l_key_count;
		r_value_sizes = r_key_sizes + *r_key_count;
		/* block ids */
		l_block_ids = l_value_sizes + *l_key_count;
		r_block_ids = r_value_sizes + *r_key_count;
		/* keys */
		l_keys = (void *)(l_block_ids + *l_key_count + 1);
		r_keys = (void *)(r_block_ids + *r_key_count + 1);
		/* values */
		l_values = l_keys;
		r_values = r_keys;
		
		for(i = 0; i < *l_key_count; i++){
			l_values += l_key_sizes[i];
		}
		
		for(i = 0; i < *r_key_count; i++){
			r_values += r_key_sizes[i];
		}
	}
	
	/* pre-calculation - seems ok */
	{
		/* middle key size */
		mid_key_size = n_key_sizes[r_child_pos - 1];
		mid_value_size = n_value_sizes[r_child_pos - 1];
		
		l_predicted_size = get_block_size(l_child);
		r_predicted_size = get_block_size(r_child);		
		
		last_key_pos = *l_key_count - 1;
		shift_count = 0;
		l_predicted_size -= (l_key_sizes[last_key_pos] + l_value_sizes[last_key_pos] + 3 * sizeof(int));
		r_predicted_size += mid_key_size + mid_value_size + 3 * sizeof(int);
		shift_count++;
		last_key_pos--;
		
		while(r_predicted_size < db->db_info.chunk_size / 2 && last_key_pos > 0)
		{
			r_predicted_size += l_key_sizes[last_key_pos + 1] + 3 * sizeof(int);
			r_predicted_size += l_value_sizes[last_key_pos + 1];
			l_predicted_size -= l_key_sizes[last_key_pos] + 3 * sizeof(int);
			l_predicted_size -= l_value_sizes[last_key_pos];
			shift_count++;
			last_key_pos--;
		}
		
		if(l_predicted_size < db->db_info.chunk_size / 2){
			success = 0;
		} else {
			success = 1;
		}
	}
	
	
	if(!success)
	{
		//printf("ret_shift_right\n");
		free(l_child);
		return 0;
	}
	/* shift right for shift_count position */	
	{
		/* tmp node, left child and right child */
		void *t_node;
		void *tl_child;
		void *tr_child;
		/* size */
		int *tn_key_count;
		int *tl_key_count;
		int *tr_key_count;
		/* sign of leaf is not required */
		/* key sizes */
		int *tn_key_sizes;
		int *tl_key_sizes;
		int *tr_key_sizes;
		/* value sizes */
		int *tn_value_sizes;
		int *tl_value_sizes;
		int *tr_value_sizes;
		/* block ids*/
		int *tn_block_ids;
		int *tl_block_ids;
		int *tr_block_ids;
		/* keys */
		void *tn_keys;
		void *tl_keys;
		void *tr_keys;
		/* values */
		void *tn_values;
		void *tl_values;
		void *tr_values;
		
		int j;
		
		t_node = malloc(2 * db->db_info.chunk_size);
		tl_child = malloc(db->db_info.chunk_size);
		tr_child = malloc(db->db_info.chunk_size);
		/* init params */
		
		/* key count */
		tn_key_count = (int *) t_node;
		*tn_key_count = *n_key_count;
		tl_key_count = (int *) tl_child;
		*tl_key_count = *l_key_count - shift_count;
		tr_key_count = (int *) tr_child;
		*tr_key_count = *r_key_count + shift_count;
		/* leaf sign */
		*(char *)(t_node + sizeof(int)) = *(char *)(node + sizeof(int));
		*(char *)(tl_child + sizeof(int)) = *(char *)(l_child + sizeof(int));
		*(char *)(tr_child + sizeof(int)) = *(char *)(r_child + sizeof(int));
		/* key sizes*/
		tn_key_sizes = (int *)(t_node + sizeof(int) + sizeof(char));
		tl_key_sizes = (int *)(tl_child + sizeof(int) + sizeof(char));
		tr_key_sizes = (int *)(tr_child + sizeof(int) + sizeof(char));
		/* values sizes */
		tn_value_sizes = tn_key_sizes + *tn_key_count;
		tl_value_sizes = tl_key_sizes + *tl_key_count;
		tr_value_sizes = tr_key_sizes + *tr_key_count;
		/* block ids */
		tn_block_ids = tn_value_sizes + *tn_key_count;
		tl_block_ids = tl_value_sizes + *tl_key_count;
		tr_block_ids = tr_value_sizes + *tr_key_count;
		/* keys */
		tn_keys = (void *)(tn_block_ids + *tn_key_count + 1);
		tl_keys = (void *)(tl_block_ids + *tl_key_count + 1);
		tr_keys = (void *)(tr_block_ids + *tr_key_count + 1);
		/* values */
		tn_values = tn_keys;
		tl_values = tl_keys;
		tr_values = tr_keys;
				
		/* t_left */
		for(i = 0; i < *tl_key_count; i++){
			tl_key_sizes[i] = l_key_sizes[i];
			tl_value_sizes[i] = l_value_sizes[i];
			tl_block_ids[i] = l_block_ids[i];
			tl_values += tl_key_sizes[i];
		}
		
		tl_block_ids[*tl_key_count] = l_block_ids[*tl_key_count];
		
		/* t_node */
		for(i = 0; i < *tn_key_count; i++){
			tn_key_sizes[i] = n_key_sizes[i];
			tn_value_sizes[i] = n_value_sizes[i];
			tn_block_ids[i] = n_block_ids[i];
			tn_values += n_key_sizes[i];
		}
		tn_block_ids[*tn_key_count] = n_block_ids[*tn_key_count];
		
		tn_key_sizes[r_child_pos - 1] = l_key_sizes[*tl_key_count];
		tn_value_sizes[r_child_pos - 1] = l_value_sizes[*tl_key_count];
		tn_values -= n_key_sizes[r_child_pos - 1];
		tn_values += l_key_sizes[*tl_key_count];
		
		/* t _right */
		for(i = *tl_key_count + 1; i < *l_key_count; i++)
		{
			j = i - *tl_key_count - 1;
			tr_key_sizes[j] = l_key_sizes[i];
			tr_value_sizes[j] = l_value_sizes[i];
			tr_block_ids[j] = l_block_ids[i];
			tr_values += l_key_sizes[i];
		}
		tr_block_ids[shift_count - 1] = l_block_ids[*l_key_count];
		tr_key_sizes[shift_count - 1] = mid_key_size;
		tr_value_sizes[shift_count - 1] = mid_value_size;
		tr_values += mid_key_size;
		
		for(i = 0; i < *r_key_count; i++)
		{
			j = i + shift_count;
			tr_key_sizes[j] = r_key_sizes[i];
			tr_value_sizes[j] = r_value_sizes[i];
			tr_block_ids[j] = r_block_ids[i];
			tr_values += r_key_sizes[i];
		}
		
		tr_block_ids[*tr_key_count] = r_block_ids[*r_key_count];
		/* above seems ok */
		/* copy to tmp nodes */
		/* t_left */
		for(i = 0; i < *tl_key_count; i++)
		{
			memcpy(tl_keys, l_keys, l_key_sizes[i]);
			memcpy(tl_values, l_values, l_value_sizes[i]);
			l_keys += l_key_sizes[i];
			tl_keys += l_key_sizes[i];
			l_values += l_value_sizes[i];
			tl_values += l_value_sizes[i];
		}

		/* t_node */
		for(i = 0; i < r_child_pos - 1; i++) {
			memcpy(tn_keys, n_keys, tn_key_sizes[i]);
			memcpy(tn_values, n_values, tn_value_sizes[i]);
			n_keys += tn_key_sizes[i];
			tn_keys += tn_key_sizes[i];
			n_values += tn_value_sizes[i];
			tn_values += tn_value_sizes[i];
		}
		
		mid_key_src = n_keys;
		mid_value_src = n_values;
		n_keys += mid_key_size;
		n_values += mid_value_size;
		
		memcpy(tn_keys, l_keys, l_key_sizes[*tl_key_count]);
		memcpy(tn_values, l_values, l_value_sizes[*tl_key_count]);
		
		l_keys += l_key_sizes[*tl_key_count];
		tn_keys += l_key_sizes[*tl_key_count];
		l_values += l_value_sizes[*tl_key_count];
		tn_values += l_value_sizes[*tl_key_count];
		
		for(i = r_child_pos; i < *tn_key_count; i++)
		{
			memcpy(tn_keys, n_keys, tn_key_sizes[i]);
			memcpy(tn_values, n_values, tn_value_sizes[i]);
			n_keys += tn_key_sizes[i];
			tn_keys += tn_key_sizes[i];
			n_values += tn_value_sizes[i];
			tn_values += tn_value_sizes[i];
		}

		/* t_right */
		
		for(i = *tl_key_count + 1; i < *l_key_count; i++)
		{
			memcpy(tr_keys, l_keys, l_key_sizes[i]);
			memcpy(tr_values, l_values, l_value_sizes[i]);
			l_keys += l_key_sizes[i];
			tr_keys += l_key_sizes[i];
			l_values += l_value_sizes[i];
			tr_values += l_value_sizes[i];
		}
		
		memcpy(tr_keys, mid_key_src, mid_key_size);
		memcpy(tr_values, mid_value_src, mid_value_size);
		tr_keys += mid_key_size;
		tr_values += mid_value_size;
		
		for( i = 0; i < *r_key_count; i++)
		{
			memcpy(tr_keys, r_keys, r_key_sizes[i]);
			memcpy(tr_values, r_values, r_value_sizes[i]);
			r_keys += r_key_sizes[i];
			tr_keys += r_key_sizes[i];
			r_values += r_value_sizes[i];
			tr_values += r_value_sizes[i];
		}
		
			
		memcpy(node, t_node, 2 * db->db_info.chunk_size);
		memcpy(l_child, tl_child, db->db_info.chunk_size);
		memcpy(r_child, tr_child, db->db_info.chunk_size);
		
		write_block_to_file(db, l_child, l_block_id);
		write_block_to_file(db, r_child, r_block_id);
		
		free(t_node);
		free(tl_child);
		free(tr_child);
	}

	//printf("exit_shift_right\n");
	free(l_child);
	return success;
}


int try_merge_with_left(struct MY_DB *db, void *node, void *r_child, int r_child_pos)
{
	void *l_child;
	int *n_key_count;
	int *n_key_sizes;
	int *n_value_sizes;
	int *n_block_ids;
	void *n_keys;
	void *n_values;
	
	int merged_size;
	int l_child_pos;
	int l_block_id;
	int r_block_id;
	int mid_key_pos;
	int mid_key_size;
	void *mid_key_src;
	int mid_value_size;
	void *mid_value_src;
	int success;
	int i, j;
	
	int *l_key_count;
	int *r_key_count;
	int *l_key_sizes;
	int *r_key_sizes;
	int *l_value_sizes;
	int *r_value_sizes;
	int *l_block_ids;
	int *r_block_ids;
	void *l_keys;
	void *r_keys;
	void *l_values;
	void *r_values;	
	
	/* m_block - merged block */
	void *m_block;
	int *m_key_count;
	int *m_key_sizes;
	int *m_value_sizes;
	int *m_block_ids;
	void *m_keys;
	void *m_values;
	
	/* init parent params - ok */
	{
		n_key_count = (int *) node;
		n_key_sizes = (int *)(node + sizeof(int) + sizeof(char));
		n_value_sizes = n_key_sizes + *n_key_count;
		n_block_ids = n_value_sizes + *n_key_count;
		n_keys = (void *)(n_block_ids + *n_key_count + 1);
		n_values = n_keys;
		
		for(i = 0; i < *n_key_count; i++) {
			n_values += n_key_sizes[i];
		}
				
		mid_key_pos = r_child_pos - 1;
		mid_key_size = n_key_sizes[mid_key_pos];
		mid_value_size = n_value_sizes[mid_key_pos];
		mid_key_src = n_keys;
		mid_value_src = n_values;
		
		for(i = 0; i < mid_key_pos; i++) {
			mid_key_src += n_key_sizes[i];
			mid_value_src += n_value_sizes[i];
		}
		
	}
	
	/* pre-calculating ok */
	{		
		l_child = malloc(db->db_info.chunk_size);
		l_child_pos = r_child_pos - 1;
		l_block_id = n_block_ids[l_child_pos];
		r_block_id = n_block_ids[r_child_pos];
		read_block_from_file(db, l_child, l_block_id);
		
		merged_size = get_block_size(l_child) + get_block_size(r_child) - sizeof(int) - sizeof(char);
		merged_size += mid_key_size + mid_value_size + 2 * sizeof(int);
		
		if(merged_size > db->db_info.chunk_size) {
			success = 0;
		} else {
			success = 1;
		}
	}
	
	if(!success) {
		free(l_child);
		return 0;
	}
	
	/*init child blocks - ok */
	{
		/* key count */
		l_key_count = (int *) l_child;
		r_key_count = (int *) r_child;
		/* key sizes */
		l_key_sizes = (int *)(l_child + sizeof(int) + sizeof(char));
		r_key_sizes = (int *)(r_child + sizeof(int) + sizeof(char));
		/* value sizes */
		l_value_sizes = l_key_sizes + *l_key_count;
		r_value_sizes = r_key_sizes + *r_key_count;
		/* block ids */
		l_block_ids = l_value_sizes + *l_key_count;
		r_block_ids = r_value_sizes + *r_key_count;
		/* keys */
		l_keys = (void *)(l_block_ids + *l_key_count + 1);
		r_keys = (void *)(r_block_ids + *r_key_count + 1);
		/* values */
		l_values = l_keys;
		r_values = r_keys;
		
		for(i = 0; i < *l_key_count; i++){
			l_values += l_key_sizes[i];
		}
		
		for(i = 0; i < *r_key_count; i++){
			r_values += r_key_sizes[i];
		}	
	}
		
	
	/* merged block init - ok */
	{
		
		m_block = malloc(db->db_info.chunk_size);
		m_key_count = (int *) m_block;
		*(char *)(m_block + sizeof(int)) = *(char *)(r_child + sizeof(int));
		*m_key_count = *l_key_count + *r_key_count + 1;
		m_key_sizes = (int *)(m_block + sizeof(int) + sizeof(char));
		m_value_sizes = m_key_sizes + *m_key_count;
		m_block_ids = m_value_sizes + *m_key_count;
		m_keys = (void *)(m_block_ids + *m_key_count + 1);
		m_values = m_keys;
		
		for(i = 0; i < *l_key_count; i++)
		{
			m_key_sizes[i] = l_key_sizes[i];
			m_value_sizes[i] = l_value_sizes[i];
			m_block_ids[i] = l_block_ids[i];
			m_values += l_key_sizes[i];
		}
		
		m_block_ids[*l_key_count] = l_block_ids[*l_key_count];
		m_key_sizes[*l_key_count] = mid_key_size;
		m_value_sizes[*l_key_count] = mid_value_size;
		m_values += mid_key_size;
		
		for(i = 0; i < *r_key_count; i++)
		{
			j = *l_key_count + 1 + i;
			m_key_sizes[j] = r_key_sizes[i];
			m_value_sizes[j] = r_value_sizes[i];
			m_block_ids[j] = r_block_ids[i];
			m_values += r_key_sizes[i];
		}
		
		m_block_ids[*l_key_count + *r_key_count + 1] = r_block_ids[*r_key_count];
	}
	
	/* merge */
	{
		for(i = 0; i < *l_key_count; i++)
		{
			memcpy(m_keys, l_keys, l_key_sizes[i]);
			memcpy(m_values, l_values, l_value_sizes[i]);
			m_keys += l_key_sizes[i];
			l_keys += l_key_sizes[i];
			m_values += l_value_sizes[i];
			l_values += l_value_sizes[i];
		}
		
		memcpy(m_keys, mid_key_src, mid_key_size);
		memcpy(m_values, mid_value_src, mid_value_size);
		m_keys += mid_key_size;
		m_values += mid_value_size;
				
		for(i = 0; i < *r_key_count; i++)
		{
			memcpy(m_keys, r_keys, r_key_sizes[i]);
			memcpy(m_values, r_values, r_value_sizes[i]);
			m_keys += r_key_sizes[i];
			r_keys += r_key_sizes[i];
			m_values += r_value_sizes[i];
			r_values += r_value_sizes[i];
		}
		
		memcpy(r_child, m_block, db->db_info.chunk_size);
		write_block_to_file(db, r_child, r_block_id);
		free(l_child);
		free(m_block);
		block_free(db, l_block_id);
	}
			
	/* modify parent a.k.a. node */
	{
		void *t_node;
		int *tn_key_count;
		int *tn_key_sizes;
		int *tn_value_sizes;
		int *tn_block_ids;
		void *tn_keys;
		void *tn_values;
		
		t_node = malloc(2 * db->db_info.chunk_size);
		tn_key_count = (int *) t_node;
		*(char *)(t_node + sizeof(int)) = *(char *)(node + sizeof(int));
		*tn_key_count = *n_key_count - 1;
		tn_key_sizes = (int *)(t_node + sizeof(int) + sizeof(char));
		tn_value_sizes = tn_key_sizes + *tn_key_count;
		tn_block_ids = tn_value_sizes + *tn_key_count;
		tn_keys = (void *)(tn_block_ids + *tn_key_count + 1);
		tn_values = tn_keys;
		/* init */
		for(i = 0; i < mid_key_pos; i++)
		{
			tn_key_sizes[i] = n_key_sizes[i];
			tn_value_sizes[i] = n_value_sizes[i];
			tn_block_ids[i] = n_block_ids[i];
			tn_values += n_key_sizes[i];
		}
		
		for(i = mid_key_pos + 1; i < *n_key_count; i++)
		{
			tn_key_sizes[i - 1] = n_key_sizes[i];
			tn_value_sizes[i - 1] = n_value_sizes[i];
			tn_block_ids[i - 1] = n_block_ids[i];
			tn_values += n_key_sizes[i];
		}
		
		tn_block_ids[*tn_key_count] = n_block_ids[*n_key_count];
		
		/* copy */
		
		for(i = 0; i < mid_key_pos; i++)
		{
			memcpy(tn_keys, n_keys, n_key_sizes[i]);
			memcpy(tn_values, n_values, n_value_sizes[i]);
			n_keys += n_key_sizes[i];
			tn_keys += n_key_sizes[i];
			n_values += n_value_sizes[i];
			tn_values += n_value_sizes[i];
		}
		
			n_keys += mid_key_size;
			n_values += mid_value_size;
					
		for(i = mid_key_pos + 1; i < *n_key_count; i++)
		{
			memcpy(tn_keys, n_keys, n_key_sizes[i]);
			memcpy(tn_values, n_values, n_value_sizes[i]);
			n_keys += n_key_sizes[i];
			tn_keys += n_key_sizes[i];
			n_values += n_value_sizes[i];
			tn_values += n_value_sizes[i];
		}
				
		memcpy(node, t_node, 2 * db->db_info.chunk_size);
		free(t_node);
	}

	return 1;
}


int try_merge_with_right(struct MY_DB *db, void *node, void *l_child, int l_child_pos)
{
	void *r_child;
	int *n_key_count;
	int *n_key_sizes;
	int *n_value_sizes;
	int *n_block_ids;
	void *n_keys;
	void *n_values;
	
	int merged_size;
	int l_block_id;
	int r_block_id;
	int mid_key_pos;
	int mid_key_size;
	void *mid_key_src;
	int mid_value_size;
	void *mid_value_src;
	int success;
	int i, j;
	int r_child_pos;
	
	int *l_key_count;
	int *r_key_count;
	int *l_key_sizes;
	int *r_key_sizes;
	int *l_value_sizes;
	int *r_value_sizes;
	int *l_block_ids;
	int *r_block_ids;
	void *l_keys;
	void *r_keys;
	void *l_values;
	void *r_values;		
	
	/* m_block - merged block */
	void *m_block;
	int *m_key_count;
	int *m_key_sizes;
	int *m_value_sizes;
	int *m_block_ids;
	void *m_keys;
	void *m_values;
	
	/* init parent params - ok */
	{
		r_child_pos = l_child_pos + 1;
		
		n_key_count = (int *) node;
		n_key_sizes = (int *)(node + sizeof(int) + sizeof(char));
		n_value_sizes = n_key_sizes + *n_key_count;
		n_block_ids = n_value_sizes + *n_key_count;
		n_keys = (void *)(n_block_ids + *n_key_count + 1);
		n_values = n_keys;
		
		for(i = 0; i < *n_key_count; i++) {
			n_values += n_key_sizes[i];
		}
				
		mid_key_pos = r_child_pos - 1;
		mid_key_size = n_key_sizes[mid_key_pos];
		mid_value_size = n_value_sizes[mid_key_pos];
		
		mid_key_src = n_keys;
		mid_value_src = n_values;
		
		for(i = 0; i < mid_key_pos; i++) {
			mid_key_src += n_key_sizes[i];
			mid_value_src += n_value_sizes[i];
		}
		
	}
	
	/* pre-calculating */
	{	
		r_child = malloc(db->db_info.chunk_size);
		l_block_id = n_block_ids[l_child_pos];
		r_block_id = n_block_ids[r_child_pos];
		read_block_from_file(db, r_child, r_block_id);
		
		merged_size = get_block_size(l_child) + get_block_size(r_child) - sizeof(int) - sizeof(char);
		merged_size += mid_key_size + mid_value_size + 2 * sizeof(int);
		
		if(merged_size > db->db_info.chunk_size) {
			success = 0;
		} else {
			success = 1;
		}
	}
	
	if(!success) {
		free(r_child);
		return 0;
	}
	
	/*init child blocks - ok */
	{
		/* key count */
		l_key_count = (int *) l_child;
		r_key_count = (int *) r_child;
		/* key sizes */
		l_key_sizes = (int *)(l_child + sizeof(int) + sizeof(char));
		r_key_sizes = (int *)(r_child + sizeof(int) + sizeof(char));
		/* value sizes */
		l_value_sizes = l_key_sizes + *l_key_count;
		r_value_sizes = r_key_sizes + *r_key_count;
		/* block ids */
		l_block_ids = l_value_sizes + *l_key_count;
		r_block_ids = r_value_sizes + *r_key_count;
		/* keys */
		l_keys = (void *)(l_block_ids + *l_key_count + 1);
		r_keys = (void *)(r_block_ids + *r_key_count + 1);
		/* values */
		l_values = l_keys;
		r_values = r_keys;
		
		for(i = 0; i < *l_key_count; i++){
			l_values += l_key_sizes[i];
		}
		
		for(i = 0; i < *r_key_count; i++){
			r_values += r_key_sizes[i];
		}	
	}
		
	
	/* merged block init - ok */
	{
		m_block = malloc(db->db_info.chunk_size);
		m_key_count = (int *) m_block;
		*(char *)(m_block + sizeof(int)) = *(char *)(r_child + sizeof(int));
		*m_key_count = *l_key_count + *r_key_count + 1;
		m_key_sizes = (int *)(m_block + sizeof(int) + sizeof(char));
		m_value_sizes = m_key_sizes + *m_key_count;
		m_block_ids = m_value_sizes + *m_key_count;
		m_keys = (void *)(m_block_ids + *m_key_count + 1);
		m_values = m_keys;
		
		for(i = 0; i < *l_key_count; i++)
		{
			m_key_sizes[i] = l_key_sizes[i];
			m_value_sizes[i] = l_value_sizes[i];
			m_block_ids[i] = l_block_ids[i];
			m_values += l_key_sizes[i];
		}
		
		m_block_ids[*l_key_count] = l_block_ids[*l_key_count];
		m_key_sizes[*l_key_count] = mid_key_size;
		m_value_sizes[*l_key_count] = mid_value_size;
		m_values += mid_key_size;
		
		for(i = 0; i < *r_key_count; i++)
		{
			j = *l_key_count + 1 + i;
			m_key_sizes[j] = r_key_sizes[i];
			m_value_sizes[j] = r_value_sizes[i];
			m_block_ids[j] = r_block_ids[i];
			m_values += r_key_sizes[i];
		}
		
		m_block_ids[*l_key_count + *r_key_count + 1] = r_block_ids[*r_key_count];
	}
	
	/* merge - ok */
	{
		for(i = 0; i < *l_key_count; i++)
		{
			memcpy(m_keys, l_keys, l_key_sizes[i]);
			memcpy(m_values, l_values, l_value_sizes[i]);
			m_keys += l_key_sizes[i];
			l_keys += l_key_sizes[i];
			m_values += l_value_sizes[i];
			l_values += l_value_sizes[i];
		}
		
		memcpy(m_keys, mid_key_src, mid_key_size);
		memcpy(m_values, mid_value_src, mid_value_size);
		m_keys += mid_key_size;
		m_values += mid_value_size;
				
		for(i = 0; i < *r_key_count; i++)
		{
			memcpy(m_keys, r_keys, r_key_sizes[i]);
			memcpy(m_values, r_values, r_value_sizes[i]);
			m_keys += r_key_sizes[i];
			r_keys += r_key_sizes[i];
			m_values += r_value_sizes[i];
			r_values += r_value_sizes[i];
		}
		
		memcpy(r_child, m_block, db->db_info.chunk_size);
		write_block_to_file(db, r_child, r_block_id);
		free(r_child);
		free(m_block);
		block_free(db, l_block_id);
	}
			
	/* modify parent a.k.a. node */
	{
		void *t_node;
		int *tn_key_count;
		int *tn_key_sizes;
		int *tn_value_sizes;
		int *tn_block_ids;
		void *tn_keys;
		void *tn_values;
		
		
		t_node = malloc(2 * db->db_info.chunk_size);
		tn_key_count = (int *) t_node;
		*(char *)(t_node + sizeof(int)) = *(char *)(node + sizeof(int));
		*tn_key_count = *n_key_count - 1;
		tn_key_sizes = (int *)(t_node + sizeof(int) + sizeof(char));
		tn_value_sizes = tn_key_sizes + *tn_key_count;
		tn_block_ids = tn_value_sizes + *tn_key_count;
		tn_keys = (void *)(tn_block_ids + *tn_key_count + 1);
		tn_values = tn_keys;
		/* init - ok */
		for(i = 0; i < mid_key_pos; i++)
		{
			tn_key_sizes[i] = n_key_sizes[i];
			tn_value_sizes[i] = n_value_sizes[i];
			tn_block_ids[i] = n_block_ids[i];
			tn_values += n_key_sizes[i];
		}
		
		for(i = mid_key_pos + 1; i < *n_key_count; i++)
		{
			tn_key_sizes[i - 1] = n_key_sizes[i];
			tn_value_sizes[i - 1] = n_value_sizes[i];
			tn_block_ids[i - 1] = n_block_ids[i];
			tn_values += n_key_sizes[i];
		}
		
		tn_block_ids[*tn_key_count] = n_block_ids[*n_key_count];
		
		/* copy */
		for(i = 0; i < mid_key_pos; i++)
		{
			memcpy(tn_keys, n_keys, n_key_sizes[i]);
			memcpy(tn_values, n_values, n_value_sizes[i]);
			n_keys += n_key_sizes[i];
			tn_keys += n_key_sizes[i];
			n_values += n_value_sizes[i];
			tn_values += n_value_sizes[i];
		}
		
			n_keys += mid_key_size;
			n_values += mid_value_size;
					
		for(i = mid_key_pos + 1; i < *n_key_count; i++)
		{
			memcpy(tn_keys, n_keys, n_key_sizes[i]);
			memcpy(tn_values, n_values, n_value_sizes[i]);
			n_keys += n_key_sizes[i];
			tn_keys += n_key_sizes[i];
			n_values += n_value_sizes[i];
			tn_values += n_value_sizes[i];
		}
				
		memcpy(node, t_node, 2 * db->db_info.chunk_size);
		free(t_node);
	}

	return 1;
}


/* returns non zero if we delete key and zero value if there is no such key*/
int delete_from_leaf(struct MY_DB *db, void *leaf, const struct DBT *key)
{
	void *t_leaf;
	int *key_count;
	int *t_key_count;
	int *keys_size;
	int *t_keys_size;
	int *values_size;
	int *t_values_size;
	int *block_ids;
	int *t_block_ids;
	void *keys;
	void *t_keys;
	void *values;
	void *t_values;
	int i, key_position;
	void *tmp;
	//printf("delete from leaf\n");
	/* */
	{
		key_count = (int *) leaf;
		keys_size = (int *)(leaf + sizeof(int) + sizeof(char));
		values_size = keys_size + *key_count;
		block_ids = values_size + *key_count;
		keys = (void *)(block_ids + *key_count + 1);
		values = keys;
		for( i = 0; i < *key_count; i++)
		{
			values += keys_size[i];
		}
	}
	
	/* search */
	{
		tmp = keys;
		i = 0;
		while( i < *key_count && cmpkeys(key->data, tmp, key->size, keys_size[i]) != 0 )
		{
			tmp += keys_size[i];
			i++;
		}

		if(i == *key_count) {
			/*such key not found*/
			return 0;
		}
		
		key_position = i;		
	}
	
	t_leaf = malloc(db->db_info.chunk_size);
	/* */
	{
		t_key_count = (int *) t_leaf;
		*t_key_count = *key_count - 1;
		*(char *)(t_leaf + sizeof(int)) = *(char *)(leaf + sizeof(int));
		t_keys_size = (int *)(t_leaf + sizeof(int) + sizeof(char));
		t_values_size = t_keys_size + *t_key_count;
		t_block_ids = t_values_size + *t_key_count;
		t_keys = (void *)(t_block_ids + *t_key_count + 1);
		t_values = t_keys;
		for( i = 0; i < key_position; i++) {
			t_values += keys_size[i];
		}
		for( i = key_position + 1; i < *key_count; i++) {
			t_values += keys_size[i];
		}
	}
	/* */
	{
		for( i = 0; i < key_position; i++)
		{
			memcpy(t_keys, keys, keys_size[i]);
			memcpy(t_values, values, values_size[i]);
			keys += keys_size[i];
			t_keys += keys_size[i];
			values += values_size[i];
			t_values += values_size[i];
			t_keys_size[i] = keys_size[i];
			t_values_size[i] = values_size[i];
		}
			keys += keys_size[key_position];
			values += values_size[key_position];
			
		for( i = key_position + 1; i < *key_count; i++)
		{
			memcpy(t_keys, keys, keys_size[i]);
			memcpy(t_values, values, values_size[i]);
			keys += keys_size[i];
			t_keys += keys_size[i];
			values += values_size[i];
			t_values += values_size[i];
			t_keys_size[i - 1] = keys_size[i];
			t_values_size[i - 1] = values_size[i];
		}
	}
	
	memcpy(leaf, t_leaf, db->db_info.chunk_size);
	free(t_leaf);
	return 1;	
}

int get_prev_key(struct MY_DB *db, void *node, struct DBT *key, struct DBT *value)
{
	int *key_count;
	char *is_leaf;
	int *key_sizes;
	int *value_sizes;
	int *block_ids;
	void *keys;
	void *values;
	/* */
	int i;
	int child_size;
	int child_actual_size;
	int child_block;
	int child_modified;
	void *child;
	int success;
	
	/* init */
	{
		key_count = (int *) node;
		is_leaf = (char *)(node + sizeof(int));
		key_sizes = (int *)(node + sizeof(int) + sizeof(char));
		value_sizes = key_sizes + *key_count;
		block_ids = value_sizes + *key_count;
		keys = (void *)(block_ids + *key_count + 1);
		values = keys;
		for( i = 0; i <*key_count; i++)
		{
			values += key_sizes[i];
		}
		
	//	printf("  %d\n", *(int *)(values - sizeof(int)));
	}
	
	if(*is_leaf) {
		
		/* copy prev key and value */
		for( i = 0; i < *key_count - 1; i++)
		{
			keys += key_sizes[i];
			values += value_sizes[i];
		}
		
		key->size = key_sizes[*key_count - 1];
		key->data = malloc(key->size);
		memcpy(key->data, keys, key->size);
		value->size = value_sizes[*key_count - 1];
		value->data = malloc(value->size);
		memcpy(value->data, values, value->size);
		delete_from_leaf(db, node, key);
		return 1;		
	}
	 
	/* if not leaf */
	child_size = 2 * db->db_info.chunk_size;
	child_block = block_ids[*key_count];
	child = malloc(child_size);
	read_block_from_file(db, child, child_block);
	child_modified = get_prev_key(db, child, key, value);
	if(!child_modified) {
		free(child);
		return 0;
	}	
	
	success = 0;
	child_actual_size = get_block_size(child);
	if(child_actual_size < db->db_info.chunk_size / 2)
	{
		if(*key_count > 0) {
			/* if there at least one element */
			success = try_shift_right(db, node, child, *key_count);
		}
		if(!success && *key_count > 0) {
			/* if there at least one element */
		;//	success = try_merge_with_left(db, node, child, *key_count);
		}
	} else if(child_actual_size > db->db_info.chunk_size){
		split_child(db, node, child, block_ids[*key_count]);
		success = 1;
	} else {
		;/* nothing */
	}
	
	if(!success) {
		write_block_to_file(db, child, block_ids[*key_count]);
	}
	
	free(child);
	return success;
}


/* returns non zero if we delete key and zero value if there is no such key*/
int delete_from_tree(struct MY_DB *db, void *node, const struct DBT *key)
{
	/* node key count */
	int *key_count;
	char *is_leaf;
	int *key_sizes;
	int *value_sizes;
	int *block_ids;
	void *keys;
	void *values;
	int i;
	
	/* */
	void *tmp_keys;
	int found;
	
	void *child;
	int child_size;
	int child_actual_size;
	int child_modified;
	int success = 0;
	
	struct DBT key1;
	struct DBT value1;
	
	/* init */
	{
		key_count = (int *) node;
		is_leaf = (char *)(node + sizeof(int));
		key_sizes = (int *)(node + sizeof(int) + sizeof(char));
		value_sizes = key_sizes + *key_count;
		block_ids = value_sizes + *key_count;
		keys = (void *)(block_ids + *key_count + 1);
		values = keys;
		for(i = 0; i < *key_count; i++)
		{
			values += key_sizes[i];
		}
	}
	
	if(*is_leaf){
		return delete_from_leaf(db, node, key);
	}
	/* search key in current block */
	{
		tmp_keys = keys;
		i = 0;
		while(i < *key_count && cmpkeys(key->data, tmp_keys, key->size, key_sizes[i]) > 0)
		{
			tmp_keys += key_sizes[i];
			i++;
		}
		
		if(i < *key_count && cmpkeys(key->data, tmp_keys, key->size, key_sizes[i]) == 0){
			found = 1;
		} else {
			found = 0;
		}
	}
	
	child_size = 2 * db->db_info.chunk_size;
	child = malloc(child_size);
	read_block_from_file(db, child, block_ids[i]);
	if(found) {
	//	printf("%d ==> \n", *(int *)key->data);
		child_modified = get_prev_key(db, child, &key1, &value1);
		/* replace key */
		{
			void *t_node;
			int *t_key_count;
			int *t_key_sizes;
			int *t_value_sizes;
			int *t_block_ids;
			void *t_keys;
			void *t_values;
			void *keys_src;
			void *values_src;
			int j;
			
			/* init */
			t_node = malloc(2 * db->db_info.chunk_size);
			t_key_count = (int *) t_node;
			*t_key_count = *key_count;
			*(char *)(t_node + sizeof(int)) = *(char *)(node + sizeof(int));
			t_key_sizes = (int *)(t_node + sizeof(int) + sizeof(char));
			t_value_sizes = t_key_sizes + *t_key_count;
			t_block_ids = t_value_sizes + *t_key_count;
			t_keys = (void *)(t_block_ids + *t_key_count + 1);
			t_values = t_keys;
			
			for(j = 0; j < *t_key_count; j++) 
			{
				t_key_sizes[j] = key_sizes[j];
				t_value_sizes[j] = value_sizes[j];
				t_block_ids[j] = block_ids[j];
				t_values += key_sizes[j];
			}
			
			t_block_ids[*t_key_count] = block_ids[*t_key_count];
			t_values -= t_key_sizes[i];
			t_key_sizes[i] = key1.size;
			t_value_sizes[i] = value1.size;
			t_values += t_key_sizes[i];
			
			keys_src = keys;
			values_src = values;
			
			for(j = 0; j < i; j++) 
			{
				memcpy(t_keys, keys_src, key_sizes[j]);
				memcpy(t_values, values_src, value_sizes[j]);
				t_keys += key_sizes[j];
				keys_src += key_sizes[j];
				t_values += value_sizes[j];
				values_src += value_sizes[j];
			}
			
			memcpy(t_keys, key1.data, key1.size);
			memcpy(t_values, value1.data, value1.size);
			t_keys += key1.size;
			t_values += value1.size;
			keys_src += key_sizes[i];
			values_src += value_sizes[i];
			
			for(j = i + 1; j < *key_count; j++) 
			{
				memcpy(t_keys, keys_src, key_sizes[j]);
				memcpy(t_values, values_src, value_sizes[j]);
				t_keys += key_sizes[j];
				keys_src += key_sizes[j];
				t_values += value_sizes[j];
				values_src += value_sizes[j];
			}
			
			memcpy(node, t_node, 2 * db->db_info.chunk_size);
			free(t_node);
		}
		/* dealloc memory */
		free(key1.data);
		free(value1.data);
		
		if(!child_modified) {
			free(child);
			return 1;
		}
	} else {
		child_modified = delete_from_tree(db, child, key);
		if(!child_modified) {
			free(child);
			return 0;
		}
	}
	
	child_actual_size = get_block_size(child);
	if(child_actual_size < db->db_info.chunk_size / 2)
	{
		if (i < *key_count) {
			/* if not rightmost */
			success = try_shift_left(db, node, child, i);
		}
		if(!success && i > 0) {
			/* if not leftmost */
			success = try_shift_right(db, node, child, i);
		}
		if(!success && i > 0) {
			/* if not leftmost */
			success = try_merge_with_left(db, node, child, i);
		}
		if (!success && i < *key_count) {
			/* if not rightmost */
			success = try_merge_with_right(db, node, child, i);
		}
	} else if(child_actual_size > db->db_info.chunk_size){
		split_child(db, node, child, block_ids[i]);
		success = 1;
	} else {
		;/* nothing */
	}
	
	if(!success) {
		write_block_to_file(db, child, block_ids[i]);
	}
	
	free(child);
	return success + found;
}

int del(struct DB *db_in, struct DBT *key)
{
	/* t_node - tmp node*/
	struct MY_DB *db = (struct MY_DB *) db_in;
	void *t_node;
	int t_node_size;
	//int t_node_actual_size;
	int modified;
	int *t_key_count;
	
	t_node_size = 2 * db->db_info.chunk_size;
	t_node = malloc(t_node_size);
	memcpy(t_node, db->db_info.root_node, db->db_info.chunk_size);
	modified = delete_from_tree(db, t_node, key);
	
	if(!modified) {
		free(t_node);
		return 0;
	}
	
	t_key_count = (int *) t_node;
	if(*t_key_count == 0) {
		//TODO make child the root
		//printf("wtf\n");
	} else if (get_block_size(t_node) > db->db_info.chunk_size){
		//TODO split root node
		//printf("too_bad\n");
	}		
	memcpy(db->db_info.root_node, t_node, db->db_info.chunk_size);
	write_block_to_file(db, t_node, db->db_info.root_id);
	
	free(t_node);
	return 0;
}




/* ok */

void printdb(struct DB *db_in, void *node)
{
	int count = *(int *)(node);
	char leaf = *(char *)(node + sizeof(int));
	int *k_s = (int *)(node + sizeof(char) + sizeof(int));
	int *v_s = k_s + count;
	int *b_ids = v_s + count;
	int *keys =  b_ids + count + 1;
	int i;
	void *child;
	struct MY_DB *db = (struct MY_DB *) db_in;
	
	if(leaf) {
		for(i = 0; i < count; i++)
			printf("%d\n", keys[i]);
		return;
	}
	
	child = malloc(db->db_info.chunk_size);
	for(i = 0; i < count; i++)
	{
		read_block_from_file(db, child, b_ids[i]);
		printdb((struct DB *)db, child);
		printf("%d\n", keys[i]);
	}
	
	read_block_from_file(db, child, b_ids[i]);
	printdb((struct DB *)db, child);

	free(child);
	return;
}
