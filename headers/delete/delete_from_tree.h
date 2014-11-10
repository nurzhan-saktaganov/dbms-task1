#ifndef DELETE_FROM_TREE_H
#define DELETE_FROM_TREE_H

#include <stdlib.h>
#include <string.h>

/* returns non zero if we delete key and zero value if there is no such key*/
int delete_from_tree(struct MY_DB *db, void *node, struct DBT *key, int *delete_sign)
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
		return delete_from_leaf(db, node, key, delete_sign);
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
		child_modified = get_prev_key(db, child, &key1, &value1, delete_sign);
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
		child_modified = delete_from_tree(db, child, key, delete_sign);
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

#endif
