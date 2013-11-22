/*-----------------------------------------------------------------------------
 *
 * prov_utility.c
 *			  
 *		
 *		AUTHOR: lord_pretzel
 *
 *		
 *
 *-----------------------------------------------------------------------------
 */

#include "provenance_rewriter/prov_utility.h"
#include "model/list/list.h"

void
addProvenanceAttrsToSchema(QueryOperator *target, QueryOperator *source)
{
//    target->schema = concatTwoLists(target->schema, copyObject(getProvenanceAttrs(source)));
}

/*
 * Given a subtree rooted a "orig" replace this subtree with the tree rooted
 * at "new". This method adapts all input lists of all parents of "orig" to point
 * to "new" instead.
 */
void
switchSubtrees(QueryOperator *orig, QueryOperator *new)
{
    // copy list of parents to new subtree
    new->parents = orig->parents;
    orig->parents = NIL;

    // adapt original parent's inputs
    FOREACH(QueryOperator,parent,orig->parents)
    {
        FOREACH(QueryOperator,pChild,parent->inputs)
        {
            if (equal(pChild,orig))
                pChild_his_cell->data.ptr_value = new;
        }
    }
}
