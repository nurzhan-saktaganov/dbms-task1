#ifndef BLOCK_H
#define BLOCK_H

#define b0000_0000 (char) 0
#define b0000_0001 (char) 1
#define b0000_0010 (char) 2
#define b0000_0100 (char) 4
#define b0000_1000 (char) 8
#define b0001_0000 (char) 16
#define b0010_0000 (char) 32
#define b0100_0000 (char) 64
#define b1000_0000 (char) 128

/* returns block offset in file by id */
long int block_offset_in_file(struct MY_DB *db, int block_id)
{
	long int offset = block_id;
	return offset *= db->db_info.chunk_size;
}

/* returns id of allocated block */
/* numeration from 0 */
int block_allocate(struct MY_DB *db)
{
	int i;
	int j = 0;
	int i0 = db->db_info.bitmap_start_index;
	int size = db->db_info.bitmap_size;
	char *bitmap = db->db_info.bitmap;
	
	/* find first free chunk and mark it as not free */
	for(i = i0; i < size; i++)
	{
		if(bitmap[i] == ~b0000_0000) {
			continue;
		} else if (!(b1000_0000 & bitmap[i])){
			j = 1;
			bitmap[i] = b1000_0000 | bitmap[i];
			break;
		} else if (!(b0100_0000 & bitmap[i])){
			j = 2;
			bitmap[i] = b0100_0000 | bitmap[i];
			break;
		} else if (!(b0010_0000 & bitmap[i])){
			j = 3;
			bitmap[i] = b0010_0000 | bitmap[i];
			break;
		} else if (!(b0001_0000 & bitmap[i])){
			j = 4;
			bitmap[i] = b0001_0000 | bitmap[i];
			break;
		} else if (!(b0000_1000 & bitmap[i])){
			j = 5;
			bitmap[i] = b0000_1000 | bitmap[i];
			break;
		} else if (!(b0000_0100 & bitmap[i])){
			j = 6;
			bitmap[i] = b0000_0100 | bitmap[i];
			break;
		} else if (!(b0000_0010 & bitmap[i])){
			j = 7;
			bitmap[i] = b0000_0010 | bitmap[i];
			break;
		} else if (!(b0000_0001 & bitmap[i])){
			j = 8;
			bitmap[i] = b0000_0001 | bitmap[i];
			break;
		}
	}
	
	if (j){
		/* return block/chunk id*/
		db->db_info.free_block_count--;
		return i*8 + j - 1;
	}

	return -1;
}

/* numeration from 0 */
void block_free(struct MY_DB *db, int block_id)
{
	int i = block_id / 8; /* byte position in bitmap */
	int j = block_id % 8 + 1; /* bit position in byte */
	char *bitmap = db->db_info.bitmap;
	
	/* boundaries check */
	if(i >= db->db_info.bitmap_size || block_id < 0)
		return;
	
	db->db_info.free_block_count++;
	/* mark block as free */
	if (j == 1) {
		bitmap[i] = ~ b1000_0000 & bitmap[i];
	} else if (j == 2) {
		bitmap[i] = ~ b0100_0000 & bitmap[i];
	} else if (j == 3) {
		bitmap[i] = ~ b0010_0000 & bitmap[i];
	} else if (j == 4) {
		bitmap[i] = ~ b0001_0000 & bitmap[i];
	} else if (j == 5) {
		bitmap[i] = ~ b0000_1000 & bitmap[i];
	} else if (j == 6) {
		bitmap[i] = ~ b0000_0100 & bitmap[i];
	} else if (j == 7) {
		bitmap[i] = ~ b0000_0010 & bitmap[i];
	} else if (j == 8) {
		bitmap[i] = ~ b0000_0001 & bitmap[i];
	} 
		
	return;
}

int get_block_size(void *block)
{
	int block_size;
	int key_count;
	int *keys_size;
	int *values_size;
	int i;
	
	key_count = *(int *)(block);
	keys_size = (int *)(block + sizeof(int) + sizeof(char));
	values_size = keys_size + key_count;
	
	block_size = sizeof(char) + (2 + 3 * key_count) * sizeof(int);
	
	for(i = 0; i < key_count; i++)
	{
		block_size += keys_size[i];
		block_size += values_size[i];
	}
	
	return block_size;
}

#endif
