#include <stdio.h>
#include <stdlib.h>
#include "libdb.c"



void print_tree_depth(struct DB * db_in, char *str)
{
	struct MY_DB *db = (struct MY_DB *) db_in;
	printf("%s ", str);
	printf("tree depth = %d\n", db->db_info.tree_depth);
	return;
}

void print_free_block(struct DB *db_in, char *str)
{
	struct MY_DB *db = (struct MY_DB *) db_in;
	printf("%s ", str);
	printf("free blocks = %d\n", db->db_info.free_block_count);
	return;
}

void print_total_block(struct DB *db_in, char *str)
{
	struct MY_DB *db = (struct MY_DB *) db_in;
	int tmp;
	tmp = (db->db_info.bitmap_size - db->db_info.bitmap_start_index) * 8;
	printf("%s ", str);
	printf("total blocks = %d\n", tmp);
	return;
}

#include <limits.h>

int main(int argc, char **argv) {
#define N 50000
#define M 0
#define REPEAT 100
#define KEY_LEN 19
#define KiB *1024
#define Mb *1000000

	struct DBC dbc;
	struct DB *db;
	int i, j;
	struct DBT key;
	struct DBT data;
	char buff[KEY_LEN];
	FILE *fp;
	//FILE *putfile;
	//FILE *getfile;
	
	int count;
	int c;
	//unlink("testdb.db");
	//fp = fopen("bigtest.out", "r");
	fp = fopen("workload100k.out", "r");
	//putfile = fopen("putcheck.txt", "w+");
	//getfile = fopen("getcheck.txt", "w+");

	key.data = malloc(KEY_LEN);
	key.size = KEY_LEN;
	data.data = malloc(KEY_LEN);
	data.size = KEY_LEN;
	
	/*dbc.mem_size = 0;*/
	dbc.db_size = 1.7 Mb;
	dbc.chunk_size = 1 KiB;
#ifdef WITH_CACHE
	dbc.mem_size = 85 KiB;
#endif
	
	int inserted = 0;
	int deleted = 0;
	int found = 0;
#ifdef WITH_CACHE	
#ifdef WITH_AVL
	printf("WITH AVL\n");
#endif
#ifdef WITH_UTHASH
	printf("WITH UTHASH\n");
#else
	printf("WITH NOTHING\n");
#endif
#else
	printf("NO CACHE\n");
#endif
	
	if((db = dbcreate("testdb.db", dbc))){
		
		print_tree_depth(db, "Empty");
		print_total_block(db, "");
		print_free_block(db, "Empty");
		
		printf("DB created\n");

		
		inserted = 0;
		for(j = 0; j < 10; j++){
		fseek(fp, 0, SEEK_SET);
		for(i = 0; i < N; i++) {
			//TODO
			//read from file
			count = 0;
			while(count < KEY_LEN)
			{
				buff[count] = fgetc(fp);
				count++;
			}
			
			while((c = fgetc(fp)) != '\n');
			
			memcpy(data.data, buff, KEY_LEN);
			memcpy(key.data, buff, KEY_LEN);
			if(put(db, &key, &data) != -1){
		/*
				fwrite(key.data, 1, key.size, putfile);
					
				fwrite(" ", 1, 1, putfile);
				
				fwrite(data.data, 1, data.size, putfile);
				
				fwrite("\n", 1, 1, putfile);
		*/
				inserted++;
			}
		}
		print_free_block(db, "After insert");
	}
		
		printf("inserted %d elements of %d\n", inserted, N);
		/*printf("success = %d\nmissmatch = %d\n", success, missmatch);
		printf("search = %d\n", search);*/
		print_free_block(db, "After insert");
		print_tree_depth(db, "After insert");
		db_close(db);
		exit(0);
		//print_free_block(db, "After insert");
		fseek(fp, 0, SEEK_SET);
		deleted = 0;
		for(i = 0; i < N ; i++) {
			//TODO
			//read from file
			count = 0;
			while(count < KEY_LEN)
			{
				buff[count] = fgetc(fp);
				count++;
			}
			
			while((c = fgetc(fp)) != '\n');
			
			if( i < M)
				continue;
			
			memcpy(data.data, buff, KEY_LEN);
			memcpy(key.data, buff, KEY_LEN);
			if(del(db, &key) != -1)
				deleted++;
		}
		
		printf("deleted %d elements of %d from %d-th\n", deleted, N, M + 1);
	
		print_tree_depth(db, "After delete");
		print_free_block(db, "After delete");
		print_tree_depth(db, "Opened db");
		print_free_block(db, "Opened db");
		fseek(fp, 0, SEEK_SET);
		found = 0;
		for(i = 0; i < N ; i++) {
			//TODO
			//read from file
			count = 0;
			while(count < KEY_LEN)
			{
				buff[count] = fgetc(fp);
				count++;
			}
			
			while((c = fgetc(fp)) != '\n');
			
			
			memcpy(key.data, buff, KEY_LEN);
			if (!get(db, &key, &data)) {
				if(memcmp(key.data, data.data, key.size))
					printf("alert!!!\n");
				found++;
				/*fwrite(key.data, 1, key.size, getfile);
					
				fwrite(" ", 1, 1, getfile);
				
				fwrite(data.data, 1, data.size, getfile);
				
				fwrite("\n", 1, 1, getfile);*/
			
			} else {
				;//printf("not found\n");
			} 
		}
		
		
		printf("deleted %d elements of %d from %d-th\n", deleted, N, M + 1);
		printf("%d/%d of data founded\nj = %d", found, N, found);
		db_close(db);
		//unlink("testdb.db");
		
		//db_close(db);
	} else if ((db = dbopen("testdb.db"))) {
		
		print_tree_depth(db, "Opened db");
		print_free_block(db, "Opened db");
		
	for(j = 0; j < REPEAT; j++){
		fseek(fp, 0, SEEK_SET);
		for(i = 0; i < N ; i++) {
			//TODO
			//read from file
			count = 0;
			while(count < KEY_LEN)
			{
				buff[count] = fgetc(fp);
				count++;
			}
			
			while((c = fgetc(fp)) != '\n');
			
			
			memcpy(key.data, buff, KEY_LEN);
			if (!get(db, &key, &data)) {
				if(memcmp(key.data, data.data, key.size))
					printf("alert!!!\n");
				found++;
			//	printf("found %d\n", *(int *) key.data);
			} else {
				;//printf("not found\n");
			} 
		}
		
	}
		
		printf("%d/%d of data founded\n", found, N);
	
		db_close(db);
		//unlink("testdb.db");
	} else {
		printf("There is some error\n");
	}
	 
	return 0;
}

