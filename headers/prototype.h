#ifndef PROTOTYPE_H
#define PROTOTYPE_H

int 		cmpkeys(void *key1, void *key2, int size1, int size2);

long int 	block_offset_in_file(struct MY_DB *db, int block_id);

int 		block_allocate(struct MY_DB *db);

void 		block_free(struct MY_DB *db, int block_id);

void 		write_block_to_file(struct MY_DB *db, void *block,
														int block_id);

void		read_block_from_file(struct MY_DB *db, void *block, 
														int block_id);

struct DB*	dbcreate(const char *file, struct DBC confg);

struct DB*	dbopen (const char *file);

int 		db_close(struct DB *db_in);

int 		b_tree_search(struct MY_DB *db, void *node,
									struct DBT *key, struct DBT *data);

int 		get(struct DB *db_in, struct DBT *key, struct DBT *data);

int 		get_block_size(void *block);

void 		add_to_leaf(struct MY_DB *db, void *leaf, struct DBT *key,
										struct DBT *data, void *m_leaf);

int 		get_child_block_id_to_insert(struct MY_DB *db, void *node,
													struct DBT *key);

int 		get_split_point(struct MY_DB *db, void *node);

void 		split_child(struct MY_DB *db, void *parent, void *child,
													int child_block_id);

int 		b_tree_insert(struct MY_DB *db, void *node, struct DBT *key,
				struct DBT *data, void *modified_parent, int block_id);

int 		put(struct DB *db_in, struct DBT *key, struct DBT *data);

int 		try_shift_left(struct MY_DB *db, void *node, void *l_child,
													int l_child_pos);

int 		try_shift_right(struct MY_DB *db, void *node, void *r_child,
													int r_child_pos);

int 		try_merge_with_left(struct MY_DB *db, void *node,
										void *r_child, int r_child_pos);

int 		try_merge_with_right(struct MY_DB *db, void *node,
										void *l_child, int l_child_pos);

int 		delete_from_leaf(struct MY_DB *db, void *leaf, 
									struct DBT *key, int *delete_sign);

int 		get_prev_key(struct MY_DB *db, void *node, struct DBT *key,
									struct DBT *value, int *delete_sign);

int 		delete_from_tree(struct MY_DB *db, void *node,
									struct DBT *key, int *delete_sign);

int 		del(struct DB *db_in, struct DBT *key);

void 		printdb(struct DB *db_in, void *node);

void*		address_in_cache(struct MY_DB *db, cache_block *c_b);

void 		read_through_cache(struct MY_DB *db, void *block, int block_id);

void 		write_through_cache(struct MY_DB *db, void *block, int block_id);

void 		flush_cache(struct MY_DB *db);

void 		free_cache(struct MY_DB *db);



#endif
