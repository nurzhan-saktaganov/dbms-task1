#ifndef GET_H
#define GET_H

#include <stdlib.h>
#include <string.h>

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

#endif
