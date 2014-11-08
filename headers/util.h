#ifndef UTIL_H
#define UTIL_H

#include <string.h>
#include "util/block.h"
#include "util/split_child.h"
#include "util/print_db.h"

/* compare two keys */
int cmpkeys(void *key1, void *key2, int size1, int size2)
{
	int minsize;
	int res;
	
	//return *(int *)(key1) - *(int *)(key2);
	
	minsize = (size1 < size2 ? size1 : size2);
	
	res = memcmp(key1, key2, minsize);
	if (res == 0 && size1 != size2) {
		if(size1 > size2)
			res = 1;
		else
			res = -1;
	}
	
	return res;
}


#endif
