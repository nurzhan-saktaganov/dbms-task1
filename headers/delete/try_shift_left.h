#ifndef TRY_SHIFT_LEFT_H
#define TRY_SHIFT_LEFT_H

#include <stdlib.h>
#include <string.h>

int try_shift_left(struct MY_DB *db, void *node, void *l_child, int l_child_pos)
{
	void *r_child;
	int r_child_pos;
	/* size */
	int *n_key_count;
	int *l_key_count;
	int *r_key_count;
	/* sign of leaf is not required */
	/* key sizes */
	int *n_key_sizes;
	int *l_key_sizes;
	int *r_key_sizes;
	/* value sizes */
	int *n_value_sizes;
	int *l_value_sizes;
	int *r_value_sizes;
	/* block ids*/
	int *n_block_ids;
	int *l_block_ids;
	int *r_block_ids;
	/* keys */
	void *n_keys;
	void *l_keys;
	void *r_keys;
	/* values */
	void *n_values;
	void *l_values;
	void *r_values;
	
	int success;
	int i;
	/* middle key and value */
	int mid_key_size;
	int mid_value_size;
	void *mid_key_src;
	void *mid_value_src;
	
	int l_predicted_size;
	int r_predicted_size;
	int shift_count;
	
	int l_block_id;
	int r_block_id;
	
	int first_key_pos;
	
	
	//printf("begin\n");
	/* init parent params - ok */
	{
		n_key_count = (int *) node;
		n_key_sizes = (int *)(node + sizeof(int) + sizeof(char));
		n_value_sizes = n_key_sizes + *n_key_count;
		n_block_ids = n_value_sizes + *n_key_count;
		n_keys = (void *)(n_block_ids + *n_key_count + 1);
		n_values = n_keys;
		
		for(i = 0; i < *n_key_count; i++) {
			n_values += n_key_sizes[i];
		}
		
		mid_key_src = n_keys;
		mid_value_src = n_values;
		
		for(i = 0; i < l_child_pos; i++) {
			mid_key_src += n_key_sizes[i];
			mid_value_src += n_value_sizes[i];
		}
	}
	
	
	/* load right child - ok */
	{
		r_child = malloc(db->db_info.chunk_size);
		r_child_pos = l_child_pos + 1;
		l_block_id = n_block_ids[l_child_pos];
		r_block_id = n_block_ids[r_child_pos];
		read_block_from_file(db, r_child, r_block_id);
	}
	
	/* init left child and right child params - ok */
	{
		/* key count */
		l_key_count = (int *) l_child;
		r_key_count = (int *) r_child;
		/* key sizes*/
		l_key_sizes = (int *)(l_child + sizeof(int) + sizeof(char));
		r_key_sizes = (int *)(r_child + sizeof(int) + sizeof(char));
		/* values sizes */
		l_value_sizes = l_key_sizes + *l_key_count;
		r_value_sizes = r_key_sizes + *r_key_count;
		/* block ids */
		l_block_ids = l_value_sizes + *l_key_count;
		r_block_ids = r_value_sizes + *r_key_count;
		/* keys */
		l_keys = (void *)(l_block_ids + *l_key_count + 1);
		r_keys = (void *)(r_block_ids + *r_key_count + 1);
		/* values */
		l_values = l_keys;
		r_values = r_keys;
		
		for(i = 0; i < *l_key_count; i++){
			l_values += l_key_sizes[i];
		}
		
		for(i = 0; i < *r_key_count; i++){
			r_values += r_key_sizes[i];
		}
	}
	
	/* pre-calculation - seems ok */
	{
		/* middle key size */
		mid_key_size = n_key_sizes[r_child_pos - 1];
		mid_value_size = n_value_sizes[r_child_pos - 1];
		
		l_predicted_size = get_block_size(l_child);
		r_predicted_size = get_block_size(r_child);
		
				
		first_key_pos = 0;
		
		l_predicted_size += mid_key_size + mid_value_size + 3 * sizeof(int);
		r_predicted_size -= r_key_sizes[first_key_pos] + r_value_sizes[first_key_pos] + 3 * sizeof(int);
		first_key_pos++;
		
		while(l_predicted_size < db->db_info.chunk_size / 2 && first_key_pos < *r_key_count - 1)
		{
			l_predicted_size += r_key_sizes[first_key_pos - 1] + 3 * sizeof(int);
			l_predicted_size += r_value_sizes[first_key_pos - 1];
			r_predicted_size -= r_key_sizes[first_key_pos] + 3 * sizeof(int);
			r_predicted_size -= r_value_sizes[first_key_pos];
			first_key_pos++;
		}
		
		if(r_predicted_size < db->db_info.chunk_size / 2){
			success = 0;
		} else {
			success = 1;
		}
	}
	
	if(!success || first_key_pos < 2)
	{
		free(r_child);
		return 0;
	}
	
	/* shift right for shift_count position */	
	{
		/* tmp node, left child and right child */
		void *t_node;
		void *tl_child;
		void *tr_child;
		/* size */
		int *tn_key_count;
		int *tl_key_count;
		int *tr_key_count;
		/* sign of leaf is not required */
		/* key sizes */
		int *tn_key_sizes;
		int *tl_key_sizes;
		int *tr_key_sizes;
		/* value sizes */
		int *tn_value_sizes;
		int *tl_value_sizes;
		int *tr_value_sizes;
		/* block ids*/
		int *tn_block_ids;
		int *tl_block_ids;
		int *tr_block_ids;
		/* keys */
		void *tn_keys;
		void *tl_keys;
		void *tr_keys;
		/* values */
		void *tn_values;
		void *tl_values;
		void *tr_values;
		
		int j;
		
		t_node = malloc(2 * db->db_info.chunk_size);
		tl_child = malloc(db->db_info.chunk_size);
		tr_child = malloc(db->db_info.chunk_size);
		/* init params */
		shift_count = first_key_pos;
		/* key count */
		tn_key_count = (int *) t_node;
		*tn_key_count = *n_key_count;
		tl_key_count = (int *) tl_child;
		*tl_key_count = *l_key_count + shift_count;
		tr_key_count = (int *) tr_child;
		*tr_key_count = *r_key_count - shift_count;
		/* leaf sign */
		*(char *)(t_node + sizeof(int)) = *(char *)(node + sizeof(int));
		*(char *)(tl_child + sizeof(int)) = *(char *)(l_child + sizeof(int));
		*(char *)(tr_child + sizeof(int)) = *(char *)(r_child + sizeof(int));
		/* key sizes*/
		tn_key_sizes = (int *)(t_node + sizeof(int) + sizeof(char));
		tl_key_sizes = (int *)(tl_child + sizeof(int) + sizeof(char));
		tr_key_sizes = (int *)(tr_child + sizeof(int) + sizeof(char));
		/* values sizes */
		tn_value_sizes = tn_key_sizes + *tn_key_count;
		tl_value_sizes = tl_key_sizes + *tl_key_count;
		tr_value_sizes = tr_key_sizes + *tr_key_count;
		/* block ids */
		tn_block_ids = tn_value_sizes + *tn_key_count;
		tl_block_ids = tl_value_sizes + *tl_key_count;
		tr_block_ids = tr_value_sizes + *tr_key_count;
		/* keys */
		tn_keys = (void *)(tn_block_ids + *tn_key_count + 1);
		tl_keys = (void *)(tl_block_ids + *tl_key_count + 1);
		tr_keys = (void *)(tr_block_ids + *tr_key_count + 1);
		/* values */
		tn_values = tn_keys;
		tl_values = tl_keys;
		tr_values = tr_keys;
		
		/* t_left */
		for(i = 0; i < *l_key_count; i++)
		{
			tl_key_sizes[i] = l_key_sizes[i];
			tl_value_sizes[i] = l_value_sizes[i];
			tl_block_ids[i] = l_block_ids[i];
			tl_values += l_key_sizes[i];
		}
		
		tl_block_ids[*l_key_count] = l_block_ids[*l_key_count];
		tl_key_sizes[*l_key_count] = mid_key_size;
		tl_value_sizes[*l_key_count] = mid_value_size;
		tl_values += mid_key_size;
		
		for(i = 0; i < first_key_pos - 1; i++)
		{
			j = *l_key_count + 1 + i;
			tl_key_sizes[j] = r_key_sizes[i];
			tl_value_sizes[j] = r_value_sizes[i];
			tl_block_ids[j] = r_block_ids[i];
			tl_values += r_key_sizes[i];
		}
		
		tl_block_ids[*tl_key_count] = r_block_ids[first_key_pos - 1];
		
		/* t_node */
		for(i = 0; i < *tn_key_count; i++)
		{
			tn_key_sizes[i] = n_key_sizes[i];
			tn_value_sizes[i] = n_value_sizes[i];
			tn_block_ids[i] = n_block_ids[i];
			tn_values += n_key_sizes[i];
		}
		
		tn_block_ids[*tn_key_count] = n_block_ids[*tn_key_count];
				
		tn_key_sizes[r_child_pos - 1] = r_key_sizes[first_key_pos - 1];
		tn_value_sizes[r_child_pos - 1] = r_value_sizes[first_key_pos - 1];
		tn_values -= n_key_sizes[r_child_pos - 1];
		tn_values += r_key_sizes[first_key_pos - 1];
		
		/* t _right */
		
		for( i = first_key_pos; i < *r_key_count; i++)
		{
			j = i - first_key_pos;
			tr_key_sizes[j] = r_key_sizes[i];
			tr_value_sizes[j] = r_value_sizes[i];
			tr_block_ids[j] = r_block_ids[i];
			tr_values += r_key_sizes[i];
		}
		
		tr_block_ids[*tr_key_count] = r_block_ids[*r_key_count];
		
		/* above seems ok */
		/* copy to tmp nodes */
		/* t_left */
		//printf("t_left\n");
		for(i = 0; i < *l_key_count; i++)
		{
			memcpy(tl_keys, l_keys, l_key_sizes[i]);
			memcpy(tl_values, l_values, l_value_sizes[i]);
			l_keys += l_key_sizes[i];
			tl_keys += l_key_sizes[i];
			l_values += l_value_sizes[i];
			tl_values += l_value_sizes[i];
		}
		
		memcpy(tl_keys, mid_key_src, mid_key_size);
		memcpy(tl_values, mid_value_src, mid_value_size);
		tl_keys += mid_key_size;
		tl_values += mid_value_size;
		
		for(i = 0; i < first_key_pos - 1; i++)
		{
			memcpy(tl_keys, r_keys, r_key_sizes[i]);
			memcpy(tl_values, r_values, r_value_sizes[i]);
			tl_keys += r_key_sizes[i];
			r_keys += r_key_sizes[i];
			tl_values += r_value_sizes[i];
			r_values += r_value_sizes[i];
		}
		
		/* t_node */
		
		for(i = 0; i < r_child_pos - 1; i++)
		{
			memcpy(tn_keys, n_keys, n_key_sizes[i]);
			memcpy(tn_values, n_values, n_value_sizes[i]);
			n_keys += n_key_sizes[i];
			tn_keys += n_key_sizes[i];
			n_values += n_value_sizes[i];
			tn_values += n_value_sizes[i];
		}
		
		memcpy(tn_keys, r_keys, r_key_sizes[first_key_pos - 1]);
		memcpy(tn_values, r_values, r_value_sizes[first_key_pos - 1]);
		r_keys += r_key_sizes[first_key_pos - 1];
		tn_keys += r_key_sizes[first_key_pos - 1];
		r_values += r_value_sizes[first_key_pos - 1];
		tn_values += r_value_sizes[first_key_pos - 1];
		n_keys += mid_key_size;
		n_values += mid_value_size;
	
		for(i = r_child_pos; i < *n_key_count; i++)
		{
			memcpy(tn_keys, n_keys, n_key_sizes[i]);
			memcpy(tn_values, n_values, n_value_sizes[i]);
			n_keys += n_key_sizes[i];
			tn_keys += n_key_sizes[i];
			n_values += n_value_sizes[i];
			tn_values += n_value_sizes[i];
		}
		
		/* t_right */
		//return 0;
		for( i = first_key_pos; i < *r_key_count ; i++)
		{
			memcpy(tr_keys, r_keys, r_key_sizes[i]);
			memcpy(tr_values, r_values, r_value_sizes[i]);
			r_keys += r_key_sizes[i];
			tr_keys += r_key_sizes[i];
			r_values += r_value_sizes[i];
			tr_values += r_value_sizes[i];
		}

		memcpy(node, t_node, 2 * db->db_info.chunk_size);
		memcpy(l_child, tl_child, db->db_info.chunk_size);
		memcpy(r_child, tr_child, db->db_info.chunk_size);
		
		write_block_to_file(db, l_child, l_block_id);
		write_block_to_file(db, r_child, r_block_id);
		
		free(t_node);
		free(tl_child);
		free(tr_child);
	}

	free(r_child);	
	return success;
}

#endif
