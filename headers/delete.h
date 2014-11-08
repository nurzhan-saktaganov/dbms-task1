#ifndef DELETE_H
#define DELETE_H

#include <stdlib.h>
#include <string.h>

#include "delete/delete_from_leaf.h"
#include "delete/delete_from_tree.h"
#include "delete/get_prev_key.h"
#include "delete/try_merge_with_left.h"
#include "delete/try_merge_with_right.h"
#include "delete/try_shift_left.h"
#include "delete/try_shift_right.h"


int del(struct DB *db_in, struct DBT *key)
{
	/* t_node - tmp node*/
	struct MY_DB *db = (struct MY_DB *) db_in;
	void *t_node;
	int t_node_size;
	//int t_node_actual_size;
	int modified;
	int *t_key_count;
	char is_leaf;
	int delete_sign = 0;
	
	t_node_size = 2 * db->db_info.chunk_size;
	t_node = malloc(t_node_size);
	memcpy(t_node, db->db_info.root_node, db->db_info.chunk_size);
	modified = delete_from_tree(db, t_node, key, &delete_sign);
	
	if(!delete_sign){
		free(t_node);
		return -1;
	}
		
	if(!modified) {
		free(t_node);
		return 0;
	}
	
	is_leaf = *(char *)(t_node + sizeof(int));
	t_key_count = (int *) t_node;
	
	if(*t_key_count == 0 && !is_leaf) {
		//TODO make child the root
		int child_id;
		child_id = *(int *)(t_node + sizeof(int) + sizeof(char));
		read_block_from_file(db, t_node, child_id);
		block_free(db, db->db_info.root_id);
		db->db_info.root_id = child_id;
		db->db_info.tree_depth--;
	} else if (get_block_size(t_node) > db->db_info.chunk_size){
		//for(;;);
		//TODO split root node
		//printf("too_bad\n");
	}		
	memcpy(db->db_info.root_node, t_node, db->db_info.chunk_size);
	write_block_to_file(db, t_node, db->db_info.root_id);	
	free(t_node);

	return 0;
}

#endif
