#pragma once
#include <stddef.h>
#include <stdint.h>

struct HNode{
    HNode *next = NULL;
    uint64_t hcode = 0;
};

struct HTab{
    HNode **tab = NULL;
    size_t mask = 0;
    size_t size = 0;
};

// it uses 2 hashtables for progressive resizing
struct HMap{
    HTab ht1;
    HTab ht2;
    size_t resizing_pos = 0;
};

HNode* hm_lookup(HMap* hmap, HNode *key,bool(*cmp)(HNode*,HNode*));
HNode* hm_pop(HMap* hmap, HNode *key, bool(*cmp)(HNode*,HNode*));
void hm_insert(HMap *hmap, HNode *node);
void hm_destroy(HMap *hmap);
size_t hm_size(HMap *hmap);