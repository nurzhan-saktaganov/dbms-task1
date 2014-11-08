#ifndef GET_PREV_KEY_H
#define GET_PREV_KEY_H

#include <stdlib.h>
#include <string.h>

int get_prev_key(struct MY_DB *db, void *node, struct DBT *key, struct DBT *value, int *delete_sign)
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
		delete_from_leaf(db, node, key, delete_sign);
		return 1;		
	}
	 
	/* if not leaf */
	child_size = 2 * db->db_info.chunk_size;
	child_block = block_ids[*key_count];
	child = malloc(child_size);
	read_block_from_file(db, child, child_block);
	child_modified = get_prev_key(db, child, key, value, delete_sign);
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

#endif
