/*************************************************************************
	> File Name: query_block.h
	> Author: Shukun Xie
    > Descriptions: Head file for query_block
 ************************************************************************/

#ifndef QUERY_BLOCK_H
#define QUERY_BLOCK_H

#include "model/list/list.h"
#include "model/node/nodetype.h"
#include "model/expression/expression.h"
#include "utility/enum_magic.h"

#define isQBQuery(node) (isA(node,QueryBlock) \
        || isA(node,SetQuery) \
        || isA(node, ProvenanceStmt) \
        || isA(node, WithStmt))
#define isQBUpdate(node) (isA(node,Insert) \
        || isA(node,Update) \
        || isA(node,Delete))

NEW_ENUM_WITH_TO_STRING(SetOpType,
        SETOP_UNION,
        SETOP_INTERSECTION,
        SETOP_DIFFERENCE);

typedef struct SetQuery
{
    NodeTag type;
    SetOpType setOp;
    boolean all;
    List *selectClause;
    Node *lChild; // either SetOp or QueryBlock
    Node *rChild; // either SetOp or QueryBlock
} SetQuery;

typedef struct QueryBlock
{
    NodeTag type;
    List *selectClause;
    Node *distinct;
    List *fromClause;
    Node *whereClause;
    List *groupByClause;
    Node *havingClause;
    List *orderByClause;
    Node *limitClause;
} QueryBlock;

NEW_ENUM_WITH_TO_STRING(IsolationLevel,
    ISOLATION_SERIALIZABLE,
    ISOLATION_READ_COMMITTED,
    ISOLATION_READ_ONLY
);

typedef struct WithStmt
{
    NodeTag type;
    List *withViews;
    Node *query;
} WithStmt;

typedef struct ProvenanceTransactionInfo
{
    NodeTag type;
    IsolationLevel transIsolation;
    List *updateTableNames;
    List *originalUpdates;
    List *scns;
    Constant *commitSCN;
} ProvenanceTransactionInfo;

typedef struct ProvenanceStmt
{
    NodeTag type;
    Node *query;
    List *selectClause;
    ProvenanceType provType;
    ProvenanceInputType inputType;
    ProvenanceTransactionInfo *transInfo;
    Node *asOf;
    List *options;
} ProvenanceStmt;


typedef struct SelectItem
{
    NodeTag type;
    char *alias;
    Node *expr;
} SelectItem;

#define isFromItem(node) (isA(node,FromItem) || isA(node, FromTableRef) \
        || isA(node, FromSubquery) || isA(node, FromJoinExpr))

typedef struct FromProvInfo
{
    NodeTag type;
    boolean baserel;
    boolean intermediateProv;
    List *userProvAttrs;
} FromProvInfo;

typedef struct FromItem
{
    NodeTag type;
    char *name;
    List *attrNames;
    List *dataTypes;
    FromProvInfo *provInfo;
} FromItem;

typedef struct FromTableRef
{
    FromItem from;
    char *tableId;
} FromTableRef;

typedef struct FromSubquery
{
    FromItem from;
    Node *subquery;
} FromSubquery;

NEW_ENUM_WITH_TO_STRING(JoinType,
    JOIN_INNER,
    JOIN_CROSS,
    JOIN_LEFT_OUTER,
    JOIN_RIGHT_OUTER,
    JOIN_FULL_OUTER
);

NEW_ENUM_WITH_TO_STRING(JoinConditionType,
    JOIN_COND_ON,
    JOIN_COND_USING,
    JOIN_COND_NATURAL
);

typedef struct FromJoinExpr
{
    FromItem from;
    FromItem *left;
    FromItem *right;
    JoinType joinType;
    JoinConditionType joinCond;
    Node *cond;
} FromJoinExpr;

typedef struct DistinctClause
{
    NodeTag type;
    List *distinctExprs;
} DistinctClause;

NEW_ENUM_WITH_TO_STRING(NestingExprType,
    NESTQ_EXISTS,
    NESTQ_ANY,
    NESTQ_ALL,
    NESTQ_UNIQUE,
    NESTQ_SCALAR
);

typedef struct NestedSubquery
{
    NodeTag type;
    NestingExprType nestingType;
    Node *expr;
    char *comparisonOp;
    Node *query;
} NestedSubquery;

typedef struct Insert
{
    NodeTag type;
    char *tableName;
    List *attrList;
    Node *query;
} Insert;

typedef struct Delete
{
    NodeTag type;
    char *nodeName;
    Node *cond;
} Delete;

typedef struct Update
{
    NodeTag type;
    char *nodeName;
    List *selectClause;
    Node *cond;
} Update;


NEW_ENUM_WITH_TO_STRING(TransactionStmtType,
    TRANSACTION_BEGIN,
    TRANSACTION_COMMIT,
    TRANSACTION_ABORT
);

typedef struct TransactionStmt
{
    NodeTag type;
    TransactionStmtType stmtType;
} TransactionStmt;

NEW_ENUM_WITH_TO_STRING(UtiltityStmtType,
    USTMT_LOAD,
    USTMT_EXPORT
);

typedef struct UtilityStatement
{
    NodeTag type;
    UtiltityStmtType stmtType;
} UtilityStatement;

NEW_ENUM_WITH_TO_STRING(DDLStmtType,
    DDL_CREATE_TABLE,
    DDL_ALTER_TABLE
);

typedef struct DDLStatement
{
    NodeTag type;
    DDLStmtType ddlType;
} DDLStatement;


/* functions for creating query block nodes */
/*extern SetQuery *createSetQuery(List *selectClause, SetOp *root);*/
extern SetQuery *createSetQuery(char *opType, boolean all, Node *lChild,
        Node *rChild);
extern QueryBlock *createQueryBlock(void);
extern ProvenanceStmt *createProvenanceStmt(Node *query);
extern SelectItem *createSelectItem(char *alias, Node *expr);
extern FromItem *createFromItem (char *alias, List *attrNames);
extern FromItem *createFromTableRef(char *alias, List *attrNames,
        char *tableId, List *dataTypes);
extern FromItem *createFromSubquery(char *alias, List *attrNames, Node *query);
extern FromItem *createFromJoin(char *alias, List *attrNames, FromItem *left,
        FromItem *right, char *joinType, char *condType,
        Node *cond);
extern JoinConditionType joinConditionTypeFromString (char *condType);
extern JoinType joinTypeFromString (char *joinType);
extern DistinctClause *createDistinctClause (List *distinctExprs);
//extern NestedSubquery *createNestedSubquery (NestingExprType nType, Node *expr,
  //      char *comparisonOp, Node *query);
extern NestedSubquery *createNestedSubquery (char *nType, Node *expr,
     char *comparisonOp, Node *query);
extern Insert *createInsert(char *nodeName, Node *query, List*);
extern Delete *createDelete(char *nodeName, Node *cond);
extern Update *createUpdate(char *nodeName, List *selectClause, Node *cond);
extern TransactionStmt *createTransactionStmt (char *stmtType);
extern WithStmt *createWithStmt (List *views, Node *query);

#endif /* QUERY_BLOCK_H */
