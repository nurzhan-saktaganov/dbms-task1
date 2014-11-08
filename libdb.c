
#include "libdb.h"
#include "headers/prototype.h"
#include "headers/dbcrud.h"
#include "headers/delete.h"
#include "headers/fileio.h"
#include "headers/get.h"
#include "headers/put.h"
#include "headers/util.h"

/*
int main(int argc, char **argv) {
#define M 100000
#define N 150000

#define Kb *1000
#define Mb *1000000

	struct DBC dbc;
	struct DB *db;
	int i, j;
	struct DBT key;
	struct DBT data;

	key.data = malloc(sizeof(int));
	key.size = sizeof(int);
	data.data = malloc(sizeof(int));
	data.size = sizeof(int);
	
	//dbc.mem_size = 0;
	dbc.db_size = 5 Mb;
	dbc.chunk_size = 4 Kb;
	 
	if((db = dbcreate("testdb.db", dbc))){
		
		printf("DB created\n");
		for(i = 0; i < N; i++) {
			*(int *)(key.data) = i;
			*(int *)(data.data) = i;
			put(db, &key, &data);
		}	
		
		printf("inserted %d elements\n", N);
		
		for(i = M; i < N ; i++) {
			*(int *)(key.data) = i;
			*(int *)(data.data) = i;
			del(db, &key);
		}
		
		printf("deleted %d elements\n", N - M);
		
		//printdb(db, db->db_info.root_node);
		db_close(db);
	} else if ((db = dbopen("testdb.db"))) {
		
		j = 0;
		for(i = 0; i < N ; i++) {
			
			*(int *)(key.data) = i;
			if (!get(db, &key, &data)) {
				if(*(int *)(key.data)!= *(int *)(data.data))
					printf("alert!!!\n");
				j++;
			//	printf("found %d\n", *(int *) key.data);
			} else {
				;//printf("not found\n");
			} 
		}
		
		printf("%d/%d of data founded\nj = %d", j, N, j);
	
		db_close(db);
		unlink("testdb.db");
	} else {
		printf("There is some error\n");
	}
	 
	return 0;
}*/
