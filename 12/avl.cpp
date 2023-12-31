#include <stddef.h>
#include <stdint.h>
struct AVLNode{
    uint32_t depth = 0;
    uint32_t cnt = 0;
    int val = 0;
    AVLNode *parent = NULL;
    AVLNode *left = NULL;
    AVLNode *right = NULL;
};

static void avl_init(AVLNode *node){
    node->depth = 1;
    node->cnt = 1;
    node->parent = node->left = node->right = NULL;
}

static uint32_t avl_depth(AVLNode *node){
    return node ? node->depth : 0;
}

static uint32_t avl_cnt(AVLNode *node){
    return node ? node->cnt : 0;
}

static uint32_t max(uint32_t lhs, uint32_t rhs){
    return lhs < rhs ? rhs : lhs;
}

static void avl_update(AVLNode *node){
    node->depth = 1 + max(avl_depth(node->left), avl_depth(node->right));
    node->cnt = 1 + avl_cnt(node->left) + avl_cnt(node->right);
}

static AVLNode *rot_left(AVLNode *node){
    AVLNode *new_node = node->right;
    if(new_node->left){
        new_node->left->parent = node;
    }
    node->right = new_node->left;
    new_node->left = node;
    new_node->parent = node->parent;
    node->parent = new_node;
    avl_update(node);
    avl_update(new_node);
    return new_node;
}

static AVLNode *rot_right(AVLNode *node){
    AVLNode *new_node = node->left;
    if(new_node->right){
        new_node->right->parent = node;
    }
    node->left = new_node->right;
    new_node->right = node;
    new_node->parent = node->parent;
    node->parent = new_node;
    avl_update(node);
    avl_update(new_node);
    return new_node;
}

// left node too deep 
// LL and LR
static AVLNode *avl_fix_left(AVLNode *root){
    // make LR to be LL
    if(avl_depth(root->left->left) < avl_depth(root->left->right)){
        root->left = rot_left(root->left);
    }
    return rot_right(root);
}
// right node too deep
// RR and RL
static AVLNode *avl_fix_right(AVLNode *root){
    // make RL to be RR
    if(avl_depth(root->right->right) < avl_depth(root->right->left)){
        root->right = rot_right(root->right);
    }
    return rot_left(root);
}


// core
static AVLNode *avl_fix(AVLNode *node){
    // from the first not balance point to update the total avl tree
    while(true){
        avl_update(node);
        uint32_t l = avl_depth(node->left);
        uint32_t r = avl_depth(node->right);
        AVLNode **from = NULL;  // store parent second pointer
        if(node->parent){
            from = (node->parent->left == node) ? &node->parent->left : &node->parent->right;
        }
        if(l == r + 2){
            node = avl_fix_left(node);
        }else if(r == l + 2){
            node = avl_fix_right(node);
        }
        if(!from){
            return node;
        }
        *from = node;
        node = node->parent;
    }
}

// detach a node and return a new root
static AVLNode *avl_del(AVLNode *node){
    if(node->right == NULL){
        AVLNode *parent = node->parent;
        if(node->left){
            node->left->parent = parent;
        }
        if(parent){
            (parent->left == node ? parent->left : parent->right) = node->left;
            return avl_fix(parent);
        }else{
            // removing root
            return node->left;
        }
    }else{
        // swap "equal" node
        AVLNode *victim = node->right;
        while(victim->left){
            victim = victim->left;
        }
        AVLNode *root = avl_del(victim);
        if(node->left){
            node->left->parent = victim;
        }
        if(node->right){
            node->right->parent = victim;
        }
        AVLNode *parent = node->parent;
        victim->parent = parent;
        victim->left = node->left;
        victim->right = node->right;
        victim->cnt = node->cnt;
        victim->depth = node->depth;
        if(parent){
            (parent->left == node ? parent->left : parent->right) = victim;
            return root;
        }else{
            // removing root
            return victim;
        }
    }
}


// 官方做法
// detach a node and returns the new root of the tree
// static AVLNode *avl_del(AVLNode *node) {
//     if (node->right == NULL) {
//         // no right subtree, replace the node with the left subtree
//         // link the left subtree to the parent
//         AVLNode *parent = node->parent;
//         if (node->left) {
//             node->left->parent = parent;
//         }
//         if (parent) {
//             // attach the left subtree to the parent
//             (parent->left == node ? parent->left : parent->right) = node->left;
//             return avl_fix(parent);
//         } else {
//             // removing root?
//             return node->left;
//         }
//     } else {
//         // swap the node with its next sibling
//         AVLNode *victim = node->right;
//         while (victim->left) {
//             victim = victim->left;
//         }
//         AVLNode *root = avl_del(victim);

//         *victim = *node;
//         if (victim->left) {
//             victim->left->parent = victim;
//         }
//         if (victim->right) {
//             victim->right->parent = victim;
//         }
//         AVLNode *parent = node->parent;
//         if (parent) {
//             (parent->left == node ? parent->left : parent->right) = victim;
//             return root;
//         } else {
//             // removing root?
//             return victim;
//         }
//     }
// }



// offset into the succeeding or preceding node.
// note: the worst-case is O(log(n)) regardless of how long the offset is.
static AVLNode *avl_offset(AVLNode *node, int64_t offset) {
    int64_t pos = 0;    // relative to the starting node
    while (offset != pos) {
        if (pos < offset && pos + avl_cnt(node->right) >= offset) {
            // the target is inside the right subtree
            node = node->right;
            pos += avl_cnt(node->left) + 1;
        } else if (pos > offset && pos - avl_cnt(node->left) <= offset) {
            // the target is inside the left subtree
            node = node->left;
            pos -= avl_cnt(node->right) + 1;
        } else {
            // go to the parent
            AVLNode *parent = node->parent;
            if (!parent) {
                return NULL;
            }
            if (parent->right == node) {
                pos -= avl_cnt(node->left) + 1;
            } else {
                pos += avl_cnt(node->right) + 1;
            }
            node = parent;
        }
    }
    return node;
}
