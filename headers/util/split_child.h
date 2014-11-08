#ifndef SPLIT_CHILD_H
#define SPLIT_CHILD_H

#include <stdlib.h>
#include <string.h>

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

#endif
