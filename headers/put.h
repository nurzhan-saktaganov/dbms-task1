#ifndef PUT_H
#define PUT_H

#include <stdlib.h>
#include <string.h>

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
	int tmp;
	
	tmp = db->db_info.free_block_count - db->db_info.tree_depth;
	if(tmp < 3)
		return -1;
		
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
		db->db_info.tree_depth++;
	}
	
	free(pseudo_root);
	return 0;
}

#endif
