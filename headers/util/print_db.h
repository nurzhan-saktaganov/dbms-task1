#ifndef PRINT_DB
#define PRINT_DB

#include <stdlib.h>
#include <stdio.h>

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



#endif
