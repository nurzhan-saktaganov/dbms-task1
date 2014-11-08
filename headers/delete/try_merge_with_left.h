#ifndef TRY_MERGE_WITH_LEFT
#define TRY_MERGE_WITH_LEFT

#include <stdlib.h>
#include <string.h>

int try_merge_with_left(struct MY_DB *db, void *node, void *r_child, int r_child_pos)
{
	void *l_child;
	int *n_key_count;
	int *n_key_sizes;
	int *n_value_sizes;
	int *n_block_ids;
	void *n_keys;
	void *n_values;
	
	int merged_size;
	int l_child_pos;
	int l_block_id;
	int r_block_id;
	int mid_key_pos;
	int mid_key_size;
	void *mid_key_src;
	int mid_value_size;
	void *mid_value_src;
	int success;
	int i, j;
	
	int *l_key_count;
	int *r_key_count;
	int *l_key_sizes;
	int *r_key_sizes;
	int *l_value_sizes;
	int *r_value_sizes;
	int *l_block_ids;
	int *r_block_ids;
	void *l_keys;
	void *r_keys;
	void *l_values;
	void *r_values;	
	
	/* m_block - merged block */
	void *m_block;
	int *m_key_count;
	int *m_key_sizes;
	int *m_value_sizes;
	int *m_block_ids;
	void *m_keys;
	void *m_values;
	
		
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
				
		mid_key_pos = r_child_pos - 1;
		mid_key_size = n_key_sizes[mid_key_pos];
		mid_value_size = n_value_sizes[mid_key_pos];
		mid_key_src = n_keys;
		mid_value_src = n_values;
		
		for(i = 0; i < mid_key_pos; i++) {
			mid_key_src += n_key_sizes[i];
			mid_value_src += n_value_sizes[i];
		}
		
	}
	
	/* pre-calculating ok */
	{		
		l_child = malloc(db->db_info.chunk_size);
		l_child_pos = r_child_pos - 1;
		l_block_id = n_block_ids[l_child_pos];
		r_block_id = n_block_ids[r_child_pos];
		read_block_from_file(db, l_child, l_block_id);
		
		merged_size = get_block_size(l_child) + get_block_size(r_child) - sizeof(int) - sizeof(char);
		merged_size += mid_key_size + mid_value_size + 2 * sizeof(int);
		
		if(merged_size > db->db_info.chunk_size) {
			success = 0;
		} else {
			success = 1;
		}
	}
	
	if(!success) {
		free(l_child);
		return 0;
	}
	
	/*init child blocks - ok */
	{
		/* key count */
		l_key_count = (int *) l_child;
		r_key_count = (int *) r_child;
		/* key sizes */
		l_key_sizes = (int *)(l_child + sizeof(int) + sizeof(char));
		r_key_sizes = (int *)(r_child + sizeof(int) + sizeof(char));
		/* value sizes */
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
		
	
	/* merged block init - ok */
	{
		
		m_block = malloc(db->db_info.chunk_size);
		m_key_count = (int *) m_block;
		*(char *)(m_block + sizeof(int)) = *(char *)(r_child + sizeof(int));
		*m_key_count = *l_key_count + *r_key_count + 1;
		m_key_sizes = (int *)(m_block + sizeof(int) + sizeof(char));
		m_value_sizes = m_key_sizes + *m_key_count;
		m_block_ids = m_value_sizes + *m_key_count;
		m_keys = (void *)(m_block_ids + *m_key_count + 1);
		m_values = m_keys;
		
		for(i = 0; i < *l_key_count; i++)
		{
			m_key_sizes[i] = l_key_sizes[i];
			m_value_sizes[i] = l_value_sizes[i];
			m_block_ids[i] = l_block_ids[i];
			m_values += l_key_sizes[i];
		}
		
		m_block_ids[*l_key_count] = l_block_ids[*l_key_count];
		m_key_sizes[*l_key_count] = mid_key_size;
		m_value_sizes[*l_key_count] = mid_value_size;
		m_values += mid_key_size;
		
		for(i = 0; i < *r_key_count; i++)
		{
			j = *l_key_count + 1 + i;
			m_key_sizes[j] = r_key_sizes[i];
			m_value_sizes[j] = r_value_sizes[i];
			m_block_ids[j] = r_block_ids[i];
			m_values += r_key_sizes[i];
		}
		
		m_block_ids[*l_key_count + *r_key_count + 1] = r_block_ids[*r_key_count];
	}
	
	/* merge */
	{
		for(i = 0; i < *l_key_count; i++)
		{
			memcpy(m_keys, l_keys, l_key_sizes[i]);
			memcpy(m_values, l_values, l_value_sizes[i]);
			m_keys += l_key_sizes[i];
			l_keys += l_key_sizes[i];
			m_values += l_value_sizes[i];
			l_values += l_value_sizes[i];
		}
		
		memcpy(m_keys, mid_key_src, mid_key_size);
		memcpy(m_values, mid_value_src, mid_value_size);
		m_keys += mid_key_size;
		m_values += mid_value_size;
				
		for(i = 0; i < *r_key_count; i++)
		{
			memcpy(m_keys, r_keys, r_key_sizes[i]);
			memcpy(m_values, r_values, r_value_sizes[i]);
			m_keys += r_key_sizes[i];
			r_keys += r_key_sizes[i];
			m_values += r_value_sizes[i];
			r_values += r_value_sizes[i];
		}
		
		memcpy(r_child, m_block, db->db_info.chunk_size);
		write_block_to_file(db, r_child, r_block_id);
		free(l_child);
		free(m_block);
		block_free(db, l_block_id);
	}
			
	/* modify parent a.k.a. node */
	{
		void *t_node;
		int *tn_key_count;
		int *tn_key_sizes;
		int *tn_value_sizes;
		int *tn_block_ids;
		void *tn_keys;
		void *tn_values;
		
		t_node = malloc(2 * db->db_info.chunk_size);
		tn_key_count = (int *) t_node;
		*(char *)(t_node + sizeof(int)) = *(char *)(node + sizeof(int));
		*tn_key_count = *n_key_count - 1;
		tn_key_sizes = (int *)(t_node + sizeof(int) + sizeof(char));
		tn_value_sizes = tn_key_sizes + *tn_key_count;
		tn_block_ids = tn_value_sizes + *tn_key_count;
		tn_keys = (void *)(tn_block_ids + *tn_key_count + 1);
		tn_values = tn_keys;
		/* init */
		for(i = 0; i < mid_key_pos; i++)
		{
			tn_key_sizes[i] = n_key_sizes[i];
			tn_value_sizes[i] = n_value_sizes[i];
			tn_block_ids[i] = n_block_ids[i];
			tn_values += n_key_sizes[i];
		}
		
		for(i = mid_key_pos + 1; i < *n_key_count; i++)
		{
			tn_key_sizes[i - 1] = n_key_sizes[i];
			tn_value_sizes[i - 1] = n_value_sizes[i];
			tn_block_ids[i - 1] = n_block_ids[i];
			tn_values += n_key_sizes[i];
		}
		
		tn_block_ids[*tn_key_count] = n_block_ids[*n_key_count];
		
		/* copy */
		
		for(i = 0; i < mid_key_pos; i++)
		{
			memcpy(tn_keys, n_keys, n_key_sizes[i]);
			memcpy(tn_values, n_values, n_value_sizes[i]);
			n_keys += n_key_sizes[i];
			tn_keys += n_key_sizes[i];
			n_values += n_value_sizes[i];
			tn_values += n_value_sizes[i];
		}
		
			n_keys += mid_key_size;
			n_values += mid_value_size;
					
		for(i = mid_key_pos + 1; i < *n_key_count; i++)
		{
			memcpy(tn_keys, n_keys, n_key_sizes[i]);
			memcpy(tn_values, n_values, n_value_sizes[i]);
			n_keys += n_key_sizes[i];
			tn_keys += n_key_sizes[i];
			n_values += n_value_sizes[i];
			tn_values += n_value_sizes[i];
		}
				
		memcpy(node, t_node, 2 * db->db_info.chunk_size);
		free(t_node);
	}

	return 1;
}

#endif
