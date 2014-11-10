#ifndef DELETE_FROM_LEAF_H
#define DELETE_FROM_LEAF_H

#include <stdlib.h>
#include <string.h>

/* returns non zero if we delete key and zero value if there is no such key*/
int delete_from_leaf(struct MY_DB *db, void *leaf, struct DBT *key, int *delete_sign)
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
	*delete_sign = 1;
	return 1;	
}


#endif
