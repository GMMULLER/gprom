/*-----------------------------------------------------------------------------
 *
 * sql_serializer.c
 *			  
 *		
 *		AUTHOR: lord_pretzel
 *
 *		
 *
 *-----------------------------------------------------------------------------
 */

#include "common.h"
#include "mem_manager/mem_mgr.h"
<<<<<<< HEAD
=======

#include "log/logger.h"

>>>>>>> cf3d9c3c0c86bb43425224c9eba0f8f22639dcbd
#include "sql_serializer/sql_serializer.h"
#include "model/node/nodetype.h"
#include "model/query_operator/query_operator.h"
#include "model/list/list.h"



/* data structures */
typedef enum MatchState {
    MATCH_START,
    MATCH_DISTINCT,
    MATCH_FIRST_PROJ,
    MATCH_HAVING,
    MATCH_AGGREGATION,
    MATCH_SECOND_PROJ,
    MATCH_WHERE,
    MATCH_NEXTBLOCK
} MatchState;

<<<<<<< HEAD
typedef struct QueryBlockMatch {
    QueryOperator *firstProj;
    QueryOperator *having;
    QueryOperator *aggregation;
    QueryOperator *secondProj;
    QueryOperator *where;
    QueryOperator *fromRoot;
} QueryBlockMatch;

=======
#define OUT_MATCH_STATE(_state) \
    (_state == MATCH_START ? "MATCH_START" : \
     _state == MATCH_DISTINCT ? "MATCH_DISTINCT" : \
     _state == MATCH_FIRST_PROJ ? "MATCH_FIRST_PROJ" : \
     _state == MATCH_HAVING ? "MATCH_HAVING" : \
     _state == MATCH_AGGREGATION ? "MATCH_AGGREGATION" : \
     _state == MATCH_SECOND_PROJ ? "MATCH_SECOND_PROJ" : \
     _state == MATCH_WHERE ? "MATCH_WHERE" : \
             "MATCH_NEXTBLOCK" \
     )

typedef struct QueryBlockMatch {
    DuplicateRemoval *distinct;
    ProjectionOperator *firstProj;
    SelectionOperator *having;
    AggregationOperator *aggregation;
    ProjectionOperator *secondProj;
    SelectionOperator *where;
    QueryOperator *fromRoot;
} QueryBlockMatch;

#define OUT_BLOCK_MATCH(_level,_m) \
    do { \
        _level ## _LOG ("distinct: %s", operatorToOverviewString((Node *) _m->distinct)); \
        _level ## _LOG ("firstProj: %s", operatorToOverviewString((Node *) _m->firstProj)); \
        _level ## _LOG ("having: %s", operatorToOverviewString((Node *) _m->having)); \
        _level ## _LOG ("aggregation: %s", operatorToOverviewString((Node *) _m->aggregation)); \
        _level ## _LOG ("secondProj: %s", operatorToOverviewString((Node *) _m->secondProj)); \
        _level ## _LOG ("where: %s", operatorToOverviewString((Node *) _m->where)); \
        _level ## _LOG ("fromRoot: %s", operatorToOverviewString((Node *) _m->fromRoot)); \
    } while(0)

>>>>>>> cf3d9c3c0c86bb43425224c9eba0f8f22639dcbd
typedef struct TemporaryViewMap {
    QueryOperator *viewOp; // the key
    char *viewName;
    char *viewDefinition;
    UT_hash_handle hh;
} TemporaryViewMap;

<<<<<<< HEAD
=======
typedef struct UpdateAggAndGroupByAttrState {
    List *aggNames;
    List *groupByNames;
} UpdateAggAndGroupByAttrState;

>>>>>>> cf3d9c3c0c86bb43425224c9eba0f8f22639dcbd
/* macros */
#define OPEN_PARENS(str) appendStringInfoChar(str, '(')
#define CLOSE_PARENS(str) appendStringInfoChar(str, ')')
#define WITH_PARENS(str,operation) \
    do { \
        OPEN_PARENS(str); \
        operation; \
        CLOSE_PARENS(str); \
    } while(0)

/* variables */
static TemporaryViewMap *viewMap;
static int viewNameCounter;

/* method declarations */
static void serializeQueryOperator (QueryOperator *q, StringInfo str);
static void serializeQueryBlock (QueryOperator *q, StringInfo str);
static void serializeSetOperator (QueryOperator *q, StringInfo str);

static void serializeFrom (QueryOperator *q, StringInfo from);
<<<<<<< HEAD
static void serializeWhere (QueryOperator *q, StringInfo where);
static void serializeSelect (QueryOperator *q, StringInfo select);

static void createTempView (QueryOperator *q, StringInfo str);
static char *createViewName (void);
=======
static void serializeFromItem (QueryOperator *q, StringInfo from,
        int *curFromItem, int *attrOffset);

static void serializeWhere (SelectionOperator *q, StringInfo where);
static boolean updateAttributeNames(Node *node, List *attrs);

static void serializeProjectionAndAggregation (QueryBlockMatch *m, StringInfo select,
        StringInfo having, StringInfo groupBy);

static char *createFromNames (int *attrOffset, int count);

static void createTempView (QueryOperator *q, StringInfo str);
static char *createViewName (void);

char *
serializeOperatorModel(Node *q)
{
    StringInfo str = makeStringInfo();
    char *result = NULL;

    if (isA(q, QueryOperator))
    {
        appendStringInfoString(str, serializeQuery((QueryOperator *) q));
        appendStringInfoChar(str,';');
    }
    else if (isA(q, List))
        FOREACH(QueryOperator,o,(List *) q)
        {
            appendStringInfoString(str, serializeQuery(o));
            appendStringInfoString(str,";\n\n");
        }
    else
        FATAL_LOG("cannot serialize non-operator to SQL: %s", nodeToString(q));

    result = str->data;
    FREE(str);
    return result;
}
>>>>>>> cf3d9c3c0c86bb43425224c9eba0f8f22639dcbd

char *
serializeQuery(QueryOperator *q)
{
    StringInfo str = makeStringInfo();
    MemContext *memC = NEW_MEM_CONTEXT("SQL_SERIALZIER");
    ACQUIRE_MEM_CONTEXT(memC);

    // initialize basic structures and then call the worker
    viewMap = NULL;
    viewNameCounter = 0;

    // call main entry point for translation
    serializeQueryOperator (q, str);

    /*
     *  prepend the temporary view definition to create something like
     *      WITH a AS (q1), b AS (q2) ... SELECT ...
     */
    if (HASH_COUNT(viewMap) > 0)
    {
        StringInfo viewDef = makeStringInfo();
        appendStringInfoString(viewDef, "WITH ");

        // loop through temporary views we have defined
        for(TemporaryViewMap *view = viewMap; view != NULL; view = view->hh.next)
        {
            appendStringInfoString(viewDef, view->viewDefinition);
            if (view->hh.next != NULL)
                appendStringInfoString(str, ",\n\n");
        }

        // prepend to query translation
        appendStringInfoString(str, "\n\n");
        prependStringInfo(str, "%s", viewDef->data);
    }

    // copy result to callers memory context and clean up
<<<<<<< HEAD
    RELEASE_MEM_CONTEXT();
    char *result = strdup(str->data);

    FREE_MEM_CONTEXT(memC);

    return result;
=======
    char *result = strdup(str->data);
    RELEASE_MEM_CONTEXT_AND_RETURN_STRING_COPY(result);
>>>>>>> cf3d9c3c0c86bb43425224c9eba0f8f22639dcbd
}

/*
 * Main entry point for serialization.
 */
static void
serializeQueryOperator (QueryOperator *q, StringInfo str)
{
    // operator with multiple parents
    if (LIST_LENGTH(q->parents) > 1)
        createTempView (q, str);
    else if (isA(q, SetOperator))
        serializeSetOperator(q, str);
    else
        serializeQueryBlock(q, str);
}

/*
 * Serialize a SQL query block (SELECT ... FROM ... WHERE ...)
 */
static void
serializeQueryBlock (QueryOperator *q, StringInfo str)
{
    QueryBlockMatch *matchInfo = NEW(QueryBlockMatch);
    StringInfo fromString = makeStringInfo();
    StringInfo whereString = makeStringInfo();
    StringInfo selectString = makeStringInfo();
<<<<<<< HEAD
    MatchState state = MATCH_START;
    QueryOperator *cur = q;
=======
    StringInfo groupByString = makeStringInfo();
    StringInfo havingString = makeStringInfo();
    MatchState state = MATCH_START;
    QueryOperator *cur = q;
    List *attrNames = getQueryOperatorAttrNames(q);
>>>>>>> cf3d9c3c0c86bb43425224c9eba0f8f22639dcbd

    // do the matching
    while(state != MATCH_NEXTBLOCK && cur != NULL)
    {
<<<<<<< HEAD
        // first check that cur does not have more than one parent

=======
        INFO_LOG("STATE: %s", OUT_MATCH_STATE(state));
        INFO_LOG("Operator %s", operatorToOverviewString((Node *) cur));
        // first check that cur does not have more than one parent
        switch(cur->type)
        {
            case T_JoinOperator:
            case T_TableAccessOperator:
            case T_SetOperator:
                matchInfo->fromRoot = cur;
                state = MATCH_NEXTBLOCK;
                cur = OP_LCHILD(cur);
                continue;
                break;
            default:
                break;
        }
>>>>>>> cf3d9c3c0c86bb43425224c9eba0f8f22639dcbd
        switch(state)
        {
            /* START state */
            case MATCH_START:
<<<<<<< HEAD
                {
                    switch(cur->type)
                    {
                        case T_SelectionOperator:
                        {
                            QueryOperator *child = OP_LCHILD(cur);
                            if (child->type == T_AggregationOperator)
                            {

                            }
                            /* WHERE */
                            else
                            {
                                matchInfo->where = cur;
                                state = MATCH_WHERE;
                            }
                        }
                            break;
                        case T_JoinOperator:
                            matchInfo->fromRoot = cur;
                            state = MATCH_NEXTBLOCK;
                            break;
                        default: //TODO add other cases
                            break;
                    }
                }
                break;
            case MATCH_DISTINCT:
                break;
            case MATCH_FIRST_PROJ:
                {
                    switch(cur->type)
                    {
                        case T_SelectionOperator:
                        {
                            QueryOperator *child = OP_LCHILD(cur);
                            if (child->type == T_AggregationOperator)
                            {
                                matchInfo->having = cur;
                                state = MATCH_HAVING;
                            }  
                        }
                            break;

                        case T_JoinOperator:
                            matchInfo->firstProj = cur;
                            state = MATCH_NEXTBLOCK;
                            break;
                        default: //TODO add other cases
                            break;
                    }
                }
                break;
             case MATCH_HAVING:
                {
                    switch(cur->type)
                    {
                        case T_SelectionOperator:
                        {
                            QueryOperator *child = OP_LCHILD(cur);
                            if (child->type == T_AggregationOperator)
                            {
                                matchInfo->aggregation = cur;
                                state = MATCH_AGGREGATION;
                            }  
                        }
                            break;
                        default: //TODO add other cases
                            break;
                    }
                }
                break;
             case MATCH_AGGREGATION:
                {
                    switch(cur->type)
                    {
                        case T_SelectionOperator:
                        {
                            QueryOperator *child = OP_LCHILD(cur);
                            if (child->type == T_AggregationOperator)
                            {
                                matchInfo->where = cur;
                                state = MATCH_WHERE;
                            }
                        }
                            break;
                        case T_JoinOperator:
                            matchInfo->aggregation = cur;
                            state = MATCH_NEXTBLOCK;
                            break;
                       
                        default: //TODO add other cases
                            break;
                    }
                }
                break;
             case MATCH_SECOND_PROJ:
                {
                    switch(cur->type)
                    {
                        case T_SelectionOperator:
                        {
                            QueryOperator *child = OP_LCHILD(cur);
                            if (child->type == T_AggregationOperator)
                            {
                                matchInfo->where = cur;
                                state = MATCH_WHERE;
                            }
                        }
                            break;
                        case T_JoinOperator:
                            matchInfo->secondProj = cur;
                            state = MATCH_NEXTBLOCK;
                            break;
                       
                        default: //TODO add other cases
                            break;
                    }
                }
                break;
             case MATCH_WHERE:
                {
                    switch(cur->type)
                    {
                        case T_SelectionOperator:
                        {
                            QueryOperator *child = OP_LCHILD(cur);
                            if (child->type == T_AggregationOperator)
                            {
                                matchInfo->where = cur;
                                state = MATCH_NEXTBLOCK;
                            }
                        }
                            break;
                        case T_JoinOperator:
                            matchInfo->where = cur;
                            state = MATCH_NEXTBLOCK;
                            break;
                       
                        default: //TODO add other cases
                            break;
                    }
                }
                break;
            default: //TODO remove once all cases are handled
=======
            case MATCH_DISTINCT:
            {
                switch(cur->type)
                {
                    case T_SelectionOperator:
                    {
                        QueryOperator *child = OP_LCHILD(cur);
                        /* HAVING */
                        if (isA(child,AggregationOperator))
                        {
                            matchInfo->having = (SelectionOperator *) cur;
                            state = MATCH_HAVING;
                        }
                        /* WHERE */
                        else
                        {
                            matchInfo->where = (SelectionOperator *) cur;
                            state = MATCH_WHERE;
                        }
                    }
                    break;
                    case T_ProjectionOperator:
                    {
                        QueryOperator *child = OP_LCHILD(cur);
                        QueryOperator *grandChild = (child ? OP_LCHILD(child) : NULL);

                        // is first projection?
                        if (isA(child,AggregationOperator)
                                || (isA(child,SelectionOperator)
                                        && isA(grandChild,AggregationOperator)))
                        {
                            matchInfo->firstProj = (ProjectionOperator *) cur;
                            state = MATCH_FIRST_PROJ;
                        }
                        else
                        {
                            matchInfo->secondProj = (ProjectionOperator *) cur;
                            state = MATCH_SECOND_PROJ;
                        }
                    }
                    break;
                    case T_DuplicateRemoval:
                        if (state == MATCH_START)
                        {
                            matchInfo->distinct = (DuplicateRemoval *) cur;
                            state = MATCH_DISTINCT;
                        }
                        else
                        {
                            matchInfo->fromRoot = cur;
                            state = MATCH_NEXTBLOCK;
                        }
                        break;
                    case T_AggregationOperator:
                        matchInfo->aggregation = (AggregationOperator *) cur;
                        state = MATCH_AGGREGATION;
                        break;
                    default:
                        matchInfo->fromRoot = cur;
                        state = MATCH_NEXTBLOCK;
                        break;
                }
            }
            break;
            case MATCH_FIRST_PROJ:
            {
                switch(cur->type)
                {
                    case T_SelectionOperator:
                    {
                        QueryOperator *child = OP_LCHILD(cur);
                        if (child->type == T_AggregationOperator)
                        {
                            matchInfo->having = (SelectionOperator *) cur;
                            state = MATCH_HAVING;
                        }
                    }
                    break;
                    case T_AggregationOperator:
                        matchInfo->aggregation= (AggregationOperator *) cur;
                        state = MATCH_AGGREGATION;
                        break;
                    default:
                        FATAL_LOG("After matching first projection we should "
                                "match selection or aggregation and not %s",
                                nodeToString(cur));
                    break;
                }
            }
            break;
            case MATCH_HAVING:
            {
                switch(cur->type)
                {
                    case T_AggregationOperator:
                    {
                        matchInfo->aggregation = (AggregationOperator *) cur;
                        state = MATCH_AGGREGATION;
                    }
                    break;
                    default:
                           FATAL_LOG("after matching having we should match "
                                   "aggregation and not %s", nodeToString(cur));
                    break;
                }
            }
            break;
            case MATCH_AGGREGATION:
            {
                switch(cur->type)
                {
                    case T_SelectionOperator:
                    {
                        matchInfo->where = (SelectionOperator *) cur;
                        state = MATCH_WHERE;
                    }
                    break;
                    case T_ProjectionOperator:
                        matchInfo->secondProj = (ProjectionOperator *) cur;
                        state = MATCH_SECOND_PROJ;
                        break;
                    default:
                        matchInfo->fromRoot = cur;
                        state = MATCH_NEXTBLOCK;
                    break;
                }
            }
            break;
            case MATCH_SECOND_PROJ:
            {
                switch(cur->type)
                {
                    case T_SelectionOperator:
                    {
                        matchInfo->where = (SelectionOperator *) cur;
                        state = MATCH_WHERE;
                    }
                    break;
                    default:
                        matchInfo->fromRoot = cur;
                        state = MATCH_NEXTBLOCK;
                    break;
                }
            }
            break;
            case MATCH_WHERE:
            {
                matchInfo->fromRoot = cur;
                state = MATCH_NEXTBLOCK;
            }
            break;
            case MATCH_NEXTBLOCK:
                FATAL_LOG("should not end up here because we already"
                        " have reached MATCH_NEXTBLOCK state");
>>>>>>> cf3d9c3c0c86bb43425224c9eba0f8f22639dcbd
                break;
        }

        // go to child of cur
        cur = OP_LCHILD(cur);
    }

<<<<<<< HEAD
    // translate FROM
    serializeFrom(matchInfo->fromRoot, fromString);

    if(matchInfo->where != NULL)
        serializeWhere(matchInfo->where, whereString);

    if(matchInfo->secondProj != NULL)
        serializeSelect(matchInfo->secondProj, selectString);
    else
        appendStringInfoString(selectString, "*");

    // put everything together
    appendStringInfoString(str, "\nSELECT ");
    appendStringInfoString(str, selectString->data);

    appendStringInfoString(str, "\nFROM ");
    appendStringInfoString(str, fromString->data);

    appendStringInfoString(str, "\nWHERE ");
    appendStringInfoString(str, whereString->data);

=======
    OUT_BLOCK_MATCH(INFO,matchInfo);

    // translate each clause
    DEBUG_LOG("serializeFrom");
    serializeFrom(matchInfo->fromRoot, fromString);

    DEBUG_LOG("serializeWhere");
    if(matchInfo->where != NULL)
        serializeWhere(matchInfo->where, whereString);

    DEBUG_LOG("serialize projection + aggregation + groupBy +  having");
    serializeProjectionAndAggregation(matchInfo, selectString, havingString,
            groupByString);

    // put everything together
    DEBUG_LOG("mergePartsTogether");
    //TODO DISTINCT
    if (STRINGLEN(selectString) > 0)
        appendStringInfoString(str, selectString->data);
    else
        appendStringInfoString(str, "\nSELECT *");

    appendStringInfoString(str, fromString->data);

    if (STRINGLEN(whereString) > 0)
        appendStringInfoString(str, whereString->data);

    if (STRINGLEN(groupByString) > 0)
            appendStringInfoString(str, groupByString->data);

    if (STRINGLEN(havingString) > 0)
            appendStringInfoString(str, havingString->data);
>>>>>>> cf3d9c3c0c86bb43425224c9eba0f8f22639dcbd

    FREE(matchInfo);
}

/*
 * Translate a FROM clause
 */
static void
serializeFrom (QueryOperator *q, StringInfo from)
{
<<<<<<< HEAD
    appendStringInfoString(str, "\nFROM ");
    appendStringInfoString(str, fromString->data);
=======
    int curFromItem = 0, attrOffset = 0;

    appendStringInfoString(from, "\nFROM ");
    serializeFromItem (q, from, &curFromItem, &attrOffset);
}

static void
serializeFromItem (QueryOperator *q, StringInfo from, int *curFromItem,
        int *attrOffset)
{
    char *attrs;

    switch(q->type)
    {
        case T_JoinOperator:
        {
            JoinOperator *j = (JoinOperator *) q;
            appendStringInfoString(from, "((");
            serializeFromItem(OP_LCHILD(j), from, curFromItem, attrOffset);
            switch(j->joinType)
            {
                case JOIN_INNER:
                    appendStringInfoString(from, " JOIN ");
                break;
                case JOIN_CROSS:
                    appendStringInfoString(from, " CROSS JOIN ");
                break;
                case JOIN_LEFT_OUTER:
                    appendStringInfoString(from, " LEFT OUTER JOIN ");
                break;
                case JOIN_RIGHT_OUTER:
                    appendStringInfoString(from, " RIGHT OUTER JOIN ");
                break;
                case JOIN_FULL_OUTER:
                    appendStringInfoString(from, " FULL OUTER JOIN ");
                break;
            }
            serializeFromItem(OP_RCHILD(j), from, curFromItem, attrOffset);

            *attrOffset = 0;
            attrs = createFromNames(attrOffset, LIST_LENGTH(j->op.schema->attrDefs));
            appendStringInfo(from, ") F%u(%s))", (*curFromItem)++, attrs);
        }
        break;
        case T_TableAccessOperator:
        {
            TableAccessOperator *t = (TableAccessOperator *) q;

            *attrOffset = 0;
            attrs = createFromNames(attrOffset, LIST_LENGTH(t->op.schema->attrDefs));
            appendStringInfo(from, "((%s) F%u(%s))", t->tableName, (*curFromItem)++, attrs);
        }
        break;
        default:
        {
            *attrOffset = 0;
            appendStringInfoString(from, "((");
            serializeQueryOperator(q, from);
            attrs = createFromNames(attrOffset, LIST_LENGTH(q->schema->attrDefs));
            appendStringInfo(from, ") F%u(%s))", (*curFromItem)++, attrs);
        }
        break;
    }
}

static char *
createFromNames (int *attrOffset, int count)
{
    char *result = NULL;
    StringInfo str = makeStringInfo();

    for(int i = *attrOffset; i < count + *attrOffset; i++)
        appendStringInfo(str, "%sa%u", (i == *attrOffset)
                ? "" : ", ", i);

    *attrOffset += count;
    result = str->data;
    FREE(str);

    return result;
>>>>>>> cf3d9c3c0c86bb43425224c9eba0f8f22639dcbd
}

/*
 * Translate a selection into a WHERE clause
 */
static void
<<<<<<< HEAD
serializeWhere (QueryOperator *q, StringInfo where)
{
    if(matchInfo->where !=NULL)
    serializeWhere(matchInfo->where, whereString);
    appendStringInfoString(str, "\nWHERE ");
    appendStringInfoString(str, whereString->data);
}

/*
 * Create the SELECT clause
 */
static void
serializeSelect (QueryOperator *q, StringInfo select)
{
    if(matchInfo->aggregation !=NULL)
    serializeSelect(matchInfo->aggregation, selectString);
    appendStringInfoString(str, "\nSELECT ");
    appendStringInfoString(str, selectString->data);
=======
serializeWhere (SelectionOperator *q, StringInfo where)
{
    appendStringInfoString(where, "\nWHERE ");
    updateAttributeNames((Node *) q->cond, NULL);
    appendStringInfoString(where, exprToSQL(q->cond));
}

static boolean
updateAttributeNames(Node *node, List *attrs)
{
    if (node == NULL)
        return TRUE;

    if (isA(node, AttributeReference))
    {
        AttributeReference *a = (AttributeReference *) node;
        char *newName;

        // use from clause attribute nomenclature
        if (LIST_LENGTH(attrs) == 0)
            newName = CONCAT_STRINGS("a", itoa(a->attrPosition));
        // use provided attribute expressions
        else
            newName = strdup(getNthOfListP(attrs,a->attrPosition));

        a->name = newName;
    }

    return visit(node, updateAttributeNames, attrs);
}

static boolean
updateAggsAndGroupByAttrs(Node *node, UpdateAggAndGroupByAttrState *state)
{
    if (node == NULL)
        return TRUE;

    if (isA(node, AttributeReference))
    {
        AttributeReference *a = (AttributeReference *) node;
        char *newName;
        int attrPos = a->attrPosition;

        // is aggregation function
        if (attrPos < LIST_LENGTH(state->aggNames))
            newName = strdup(getNthOfListP(state->aggNames, attrPos));
        else
        {
            attrPos -= LIST_LENGTH(state->aggNames);
            newName = strdup(getNthOfListP(state->groupByNames, attrPos));
        }
        DEBUG_LOG("attr <%d> is <%s>", a->attrPosition, newName);

        a->name = newName;
    }

    return visit(node, updateAggsAndGroupByAttrs, state);
}

/*
 * Create the SELECT, GROUP BY, and HAVING clause
 */
static void
serializeProjectionAndAggregation (QueryBlockMatch *m, StringInfo select,
        StringInfo having, StringInfo groupBy)
{
    int pos = 0;
    List *secondProjs = NIL;
    List *aggs = NIL;
    List *groupBys = NIL;
    List *firstProjs = NIL;
    AggregationOperator *agg = (AggregationOperator *) m->aggregation;
    UpdateAggAndGroupByAttrState *state = NULL;

    appendStringInfoString(select, "\nSELECT ");

    // first projection level for aggregation inputs and group-by
    if (m->secondProj != NULL)
    {
        FOREACH(Node,n,m->secondProj->projExprs)
        {
            updateAttributeNames(n, NULL);
            secondProjs = appendToTailOfList(secondProjs, exprToSQL(n));
        }
        INFO_LOG("second projection (agg and group by inputs) is %s",
                stringListToString(secondProjs));
    }

    // aggregation if need be
    if (agg != NULL)
    {
        // aggregation
        FOREACH(Node,expr,agg->aggrs)
        {
            updateAttributeNames(expr, secondProjs);
            aggs = appendToTailOfList(aggs, exprToSQL(expr));
        }
        INFO_LOG("agg attributes are %s", stringListToString(aggs));

        // group by
        FOREACH(Node,expr,agg->groupBy)
        {
            char *g;
            if (pos++ == 0)
                appendStringInfoString (groupBy, "\nGROUP BY ");
            else
                appendStringInfoString (groupBy, ", ");

            updateAttributeNames(expr, secondProjs);
            g = exprToSQL(expr);

            groupBys = appendToTailOfList(groupBys, g);
            appendStringInfo(groupBy, "%s", strdup(g));
        }
        INFO_LOG("group by attributes are %s", stringListToString(groupBys));

        state = NEW(UpdateAggAndGroupByAttrState);
        state->aggNames = aggs;
        state->groupByNames = groupBys;
    }

    // having
    if (m->having != NULL)
    {
        SelectionOperator *sel = (SelectionOperator *) m->having;
        DEBUG_LOG("having condition %s", nodeToString(sel->cond));
        updateAggsAndGroupByAttrs(sel->cond, state);
        appendStringInfo(having, "\nHAVING %s", exprToSQL(sel->cond));
        INFO_LOG("having translation %s", having->data);
    }

    // second level of projection either if no aggregation or using aggregation
    if (m->firstProj != NULL)
    {
        int pos = 0;
        ProjectionOperator *p = m->firstProj;

        FOREACH(Node,a,p->projExprs)
        {
            if (pos++ != 0)
                appendStringInfoString(select, ", ");

            // is projection over aggregation
            if (agg)
                updateAggsAndGroupByAttrs(a, state);
            // is projection in query without aggregation
            else
                updateAttributeNames(a, NULL);
            appendStringInfoString(select, exprToSQL(a));
        }

        INFO_LOG("second projection expressions %s", select->data);
    }
    // get aggregation result attributes
    else if (agg)
    {
        int pos = 0;

        FOREACH(char,name,aggs)
        {
            if (pos++ != 0)
                appendStringInfoString(select, ", ");
            appendStringInfoString(select, name);
        }
        FOREACH(char,name,groupBys)
        {
            if (pos++ != 0)
                appendStringInfoString(select, ", ");
            appendStringInfoString(select, name);
        }
        INFO_LOG("aggregation result as projection expressions %s", select->data);
    }
    // get attributes from FROM clause root
    else
    {
        List *fromAttrs = getQueryOperatorAttrNames(m->fromRoot);

        FOREACH(char,name,fromAttrs)
        {
            if (pos++ != 0)
                appendStringInfoString(select, ", ");
            appendStringInfoString(select, name);
        }

        INFO_LOG("FROM root attributes as projection expressions %s", select->data);
    }

    if (state)
        FREE(state);
>>>>>>> cf3d9c3c0c86bb43425224c9eba0f8f22639dcbd
}


/*
 * Serialize a set operation UNION/MINUS/INTERSECT
 */
static void
serializeSetOperator (QueryOperator *q, StringInfo str)
{
    SetOperator *setOp = (SetOperator *) q;

    // output left child
    WITH_PARENS(str,serializeQueryOperator(OP_LCHILD(q), str));

    // output set operation
    switch(setOp->setOpType)
    {
        case SETOP_UNION:
            appendStringInfoString(str, " UNION ALL ");
            break;
        case SETOP_INTERSECTION:
            appendStringInfoString(str, " INTERSECT ");
            break;
        case SETOP_DIFFERENCE:
            appendStringInfoString(str, " MINUS ");
            break;
    }

    // output right child
    WITH_PARENS(str,serializeQueryOperator(OP_RCHILD(q), str));
}

/*
 * Create a temporary view
 */
static void
createTempView (QueryOperator *q, StringInfo str)
{
    StringInfo viewDef = makeStringInfo();
    char *viewName = createViewName();
    TemporaryViewMap *view;

    // create sql code to create view
    appendStringInfo(viewDef, "%s AS (", viewName);
    serializeQueryOperator(q, viewDef);
    appendStringInfoString(viewDef, ")\n\n");

    // add to view table
    view = NEW(TemporaryViewMap);
    view->viewName = viewName;
    view->viewOp = q;
    view->viewDefinition = viewDef->data;
    HASH_ADD_PTR(viewMap, viewName, view);
}

static char *
createViewName (void)
{
    StringInfo str = makeStringInfo();

    appendStringInfo(str, "temp_view_of_%u", viewNameCounter++);

    return str->data;
}
