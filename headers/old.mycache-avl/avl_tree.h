#ifndef AVL_TREE_H
#define AVL_TREE_H

/* avl-tree realization from http://habrahabr.ru/post/150732/ */

bin_tree_node* bin_tree_create_new(int k, cache_block *cache_block_ptr)
{
	bin_tree_node *new_node;
	new_node = (bin_tree_node *) malloc(sizeof(bin_tree_node));
	new_node->key = k;
	new_node->cache_block_ptr = cache_block_ptr;
	new_node->left = NULL;
	new_node->right = NULL;
	new_node->height = 1;
	return new_node;
}

int bin_tree_height(bin_tree_node* p)
{
    return p ? p->height : 0;
}

int bin_tree_bfactor(bin_tree_node* p)
{
    return bin_tree_height(p->right) - bin_tree_height(p->left);
}

void bin_tree_fixheight(bin_tree_node* p)
{
    int hl = bin_tree_height(p->left);
    int hr = bin_tree_height(p->right);
    p->height = (hl > hr ? hl : hr) + 1;
}

bin_tree_node* bin_tree_rotateright(bin_tree_node* p) // правый поворот вокруг p
{
    bin_tree_node* q = p->left;
    p->left = q->right;
    q->right = p;
    bin_tree_fixheight(p);
    bin_tree_fixheight(q);
    return q;
}

bin_tree_node* bin_tree_rotateleft(bin_tree_node* q) // левый поворот вокруг q
{
    bin_tree_node* p = q->right;
    q->right = p->left;
    p->left = q;
    bin_tree_fixheight(q);
    bin_tree_fixheight(p);
    return p;
}

bin_tree_node* bin_tree_balance(bin_tree_node* p) // балансировка узла p
{
    bin_tree_fixheight(p);
    
    if( bin_tree_bfactor(p) == 2 ) {
        if( bin_tree_bfactor(p->right) < 0 )
            p->right = bin_tree_rotateright(p->right);
        return bin_tree_rotateleft(p);
    }
    if( bin_tree_bfactor(p) == -2 ) {
        if( bin_tree_bfactor(p->left) > 0  )
            p->left = bin_tree_rotateleft(p->left);
        return bin_tree_rotateright(p);
    }
    return p; // балансировка не нужна
}

bin_tree_node* bin_tree_insert(bin_tree_node* p, int k, cache_block *cache_block_ptr) // вставка ключа k в дерево с корнем p
{
    if( p == NULL)
		return bin_tree_create_new(k, cache_block_ptr);
		
    if( k < p->key )
        p->left = bin_tree_insert(p->left, k, cache_block_ptr);
    else
        p->right = bin_tree_insert(p->right, k, cache_block_ptr);
        
    return bin_tree_balance(p);
}

bin_tree_node* bin_tree_findmin(bin_tree_node* p) // поиск узла с минимальным ключом в дереве p 
{
    return p->left ? bin_tree_findmin(p->left) : p;
}

bin_tree_node* bin_tree_removemin(bin_tree_node* p) // удаление узла с минимальным ключом из дерева p
{
    if( p->left == 0 )
        return p->right;
        
    p->left = bin_tree_removemin(p->left);
    return bin_tree_balance(p);
}

bin_tree_node* bin_tree_remove(bin_tree_node* p, int k) // удаление ключа k из дерева p
{
    if( p == NULL )
		return NULL;
    if( k < p->key )
        p->left = bin_tree_remove(p->left, k);
    else if( k > p->key )
        p->right = bin_tree_remove(p->right, k);	
    else //  k == p->key
    {
        bin_tree_node* q = p->left;
        bin_tree_node* r = p->right;
        free(p);
        if( r == NULL ) return q;
        bin_tree_node* min = bin_tree_findmin(r);
        min->right = bin_tree_removemin(r);
        min->left = q;
        return bin_tree_balance(min);
    }
    return bin_tree_balance(p);
}

cache_block* bin_tree_search(bin_tree_node* p, int k)
{
	//search++;
	if(p == NULL)
		return NULL;
		
	if(k < p->key)
		return bin_tree_search(p->left, k);
	if(k > p->key)
		return bin_tree_search(p->right, k);
	
	/* if(k == p->key) */
	return p->cache_block_ptr;
}

void bin_tree_clear(bin_tree_node* p)
{
	if(p == NULL)
		return;
	bin_tree_clear(p->left);
	bin_tree_clear(p->right);
	free(p);

	return;
}


#endif
