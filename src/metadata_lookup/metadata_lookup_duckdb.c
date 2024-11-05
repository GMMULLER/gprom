#include "common.h"
#include "mem_manager/mem_mgr.h"
#include "log/logger.h"
#include "instrumentation/timing_instrumentation.h"

#include "configuration/option.h"
#include "metadata_lookup/metadata_lookup.h"
#include "metadata_lookup/metadata_lookup_duckdb.h"
#include "model/expression/expression.h"
#include "model/list/list.h"
#include "model/node/nodetype.h"
#include "model/query_block/query_block.h"
#include "model/query_operator/query_operator.h"
#include "model/set/hashmap.h"
#include "model/set/vector.h"
#include "operator_optimizer/optimizer_prop_inference.h"
#include "utility/string_utils.h"
#include <stdlib.h>

// Mem context
#define CONTEXT_NAME "DuckDBMemContext"

#define QUERY_TABLE_COL_COUNT "DESCRIBE %s" // TODO: Account for the differences in output compared with PRAGMA
#define QUERY_TABLE_ATTR_MIN_MAX "SELECT %s FROM %s"

// Only define real plugin structure and methods if duckdb is present
#ifdef HAVE_DUCKDB_BACKEND

// extends MetadataLookupPlugin with duckdb specific information
typedef struct DuckDBPlugin
{
    MetadataLookupPlugin plugin;
    boolean initialized;
    sqlite3 *conn; // TODO: replace with duck connection type
} DuckDBPlugin; 

// global vars
static DuckDBPlugin *plugin = NULL;
static MemContext *memContext = NULL;

// functions
static sqlite3_stmt *runQuery (char *q); // TODO: adapt the type for duckdb (sqlite3_stmt). Modify runQuery
static DataType stringToDT (char *dataType); // TODO: adapt function
static char *duckdbGetConnectionDescription (void); // TODO: adapt function
static void initCache(CatalogCache *c); // TODO: adapt function

#define HANDLE_ERROR_MSG(_rc,_expected,_message, ...) \
    do { \
        if (_rc != _expected) \
        { \
            StringInfo _newmes = makeStringInfo(); \ // TODO: adapt function
            appendStringInfo(_newmes, _message, ##__VA_ARGS__); \
            StringInfo _errMes = makeStringInfo(); \
            appendStringInfo(_errMes, strdup((char *) sqlite3_errmsg(plugin->conn))); \ // TODO: adapt function
            FATAL_LOG("error (%s)\n%u\n\n%s", _errMes, _rc, _newmes->data); \
        } \
    } while(0)

// TODO: adapt functions but not all
MetadataLookupPlugin *
assembleSqliteMetadataLookupPlugin (void)
{
    plugin = NEW(DuckDBPlugin);
    MetadataLookupPlugin *p = (MetadataLookupPlugin *) plugin;

    p->type = METADATA_LOOKUP_PLUGIN_DUCKDB;

    p->initMetadataLookupPlugin = duckdbInitMetadataLookupPlugin; 
    p->databaseConnectionOpen = duckdbDatabaseConnectionOpen; 
    p->databaseConnectionClose = duckdbDatabaseConnectionClose; 
    p->shutdownMetadataLookupPlugin = duckdbShutdownMetadataLookupPlugin; 
    p->isInitialized = duckdbIsInitialized; 
    p->catalogTableExists = duckdbCatalogTableExists; 
    p->catalogViewExists = duckdbCatalogViewExists;
    p->getAttributes = duckdbGetAttributes;
    p->getAttributeNames = duckdbGetAttributeNames;
    p->isAgg = duckdbIsAgg;
    p->isWindowFunction = duckdbIsWindowFunction;
    p->getFuncReturnType = duckdbGetFuncReturnType;
    p->getOpReturnType = duckdbGetOpReturnType;
    p->getTableDefinition = duckdbGetTableDefinition;
    p->getViewDefinition = duckdbGetViewDefinition;
    p->getTransactionSQLAndSCNs = duckdbGetTransactionSQLAndSCNs; 
    p->executeAsTransactionAndGetXID = duckdbExecuteAsTransactionAndGetXID;
    p->getCostEstimation = duckdbGetCostEstimation;
    p->getKeyInformation = duckdbGetKeyInformation; 
    p->executeQuery = duckdbExecuteQuery;
    p->executeQueryIgnoreResult = duckdbExecuteQueryIgnoreResults;
    p->connectionDescription = duckdbGetConnectionDescription;
    p->sqlTypeToDT = duckdbBackendSQLTypeToDT;
    p->dataTypeToSQL = duckdbBackendDatatypeToSQL;
    p->getMinAndMax = duckdbGetMinAndMax;
    return p;
}

/* plugin methods */
int
duckdbInitMetadataLookupPlugin (void)
{
    if (plugin && plugin->initialized)
    {
        INFO_LOG("tried to initialize metadata lookup plugin more than once");
        return EXIT_SUCCESS;
    }
    memContext = getCurMemContext();

    // create cache
    plugin->plugin.cache = createCache();
    initCache(plugin->plugin.cache);

    plugin->initialized = TRUE;

    return EXIT_SUCCESS;
}

int
duckdbShutdownMetadataLookupPlugin (void)
{

    // clear cache
    //TODO

    return EXIT_SUCCESS;
}

int
duckdbDatabaseConnectionOpen (void)
{
    char *dbfile = getStringOption(OPTION_CONN_DB);
    int rc;
    if (dbfile == NULL)
        FATAL_LOG("no database file given (<connection.db> parameter)");

    rc = sqlite3_open(dbfile, &(plugin->conn)); // TODO: adapt function
    if(rc != SQLITE_OK) // TODO: adapt
    {
          HANDLE_ERROR_MSG(rc, SQLITE_OK, "Can not open database <%s>", dbfile); // TODO: adapt
          sqlite3_close(plugin->conn); // TODO: adapt function
          return EXIT_FAILURE; // TODO: adapt function
    }
    return EXIT_SUCCESS; // TODO: adapt function
}

int
duckdbDatabaseConnectionClose()
{
    int rc;
    rc = sqlite3_close(plugin->conn); // TODO: adapt

    HANDLE_ERROR_MSG(rc, SQLITE_OK, "Can not close database"); // TODO: adapt

    return EXIT_SUCCESS;
}

boolean
duckdbIsInitialized (void)
{
    if (plugin && plugin->initialized)
    {
        if (plugin->conn == NULL)
        {
            if (duckdbDatabaseConnectionOpen() != EXIT_SUCCESS) // TODO: adapt function
                return FALSE;
        }

        return TRUE;
    }

    return FALSE;
}

boolean
duckdbCatalogTableExists (char * tableName)
{
    sqlite3 *c = plugin->conn; // TODO: adapt function
    boolean res = (sqlite3_table_column_metadata(c,NULL,tableName,strdup("rowid"),NULL,NULL,NULL,NULL, NULL) == SQLITE_OK); // TODO: adapt function

    return res;//TODO
}

boolean
duckdbCatalogViewExists (char * viewName)
{
    return FALSE;//TODO
}

List *
duckdbGetAttributes (char *tableName)
{
    sqlite3_stmt *rs; // TODO: adapt function
    StringInfo q;
    List *result = NIL;
    int rc;

    q = makeStringInfo();
    appendStringInfo(q, QUERY_TABLE_COL_COUNT, tableName);
    rs = runQuery(q->data);

    while((rc = sqlite3_step(rs)) == SQLITE_ROW) // TODO: adapt function
    {
        const unsigned char *colName = sqlite3_column_text(rs,1); // TODO: adapt function
        const unsigned char *dt = sqlite3_column_text(rs,2); // TODO: adapt function
        DataType ourDT = stringToDT((char *) dt);

        AttributeDef *a = createAttributeDef(
            strToUpper(strdup((char *) colName)),
            ourDT);
        result = appendToTailOfList(result, a);
    }

    HANDLE_ERROR_MSG(rc, SQLITE_DONE, "error getting attributes of table <%s>", tableName); // TODO: adapt function

    DEBUG_NODE_LOG("columns are: ", result);

    return result;
}

List *
duckdbGetAttributeNames (char *tableName)
{
    return getAttrDefNames(duckdbGetAttributes(tableName));
}

boolean
duckdbIsAgg(char *functionName)
{
    char *f = strToLower(functionName);

    if (hasSetElem(plugin->plugin.cache->aggFuncNames, f))
        return TRUE;

    return FALSE;
}

boolean
duckdbIsWindowFunction(char *functionName)
{
    char *f = strToLower(functionName);

    if (hasSetElem(plugin->plugin.cache->winFuncNames, f))
        return TRUE;

    return FALSE;
}

DataType
duckdbGetFuncReturnType (char *fName, List *argTypes, boolean *funcExists)
{
    *funcExists = TRUE;
    return DT_STRING; //TODO
}

DataType
duckdbGetOpReturnType (char *oName, List *argTypes, boolean *opExists)
{

    *opExists = TRUE;

    if (streq(oName, "+") || streq(oName, "*")  || streq(oName, "-") || streq(oName, "/"))
    {
        if (LIST_LENGTH(argTypes) == 2)
        {
            DataType lType = getNthOfListInt(argTypes, 0);
            DataType rType = getNthOfListInt(argTypes, 1);

            if (lType == rType)
            {            
                if (lType == DT_INT || lType == DT_FLOAT)
                {
                    return lType;
                }
            }
            else
            {
                DataType lca;
                lca = lcaType(lType, rType);
                if(lca == DT_INT || lca == DT_FLOAT)
                {
                    return lca;
                }
            }
        }
    }

    if (streq(oName, "||"))
    {
        DataType lType = getNthOfListInt(argTypes, 0);
        DataType rType = getNthOfListInt(argTypes, 1);

        if (lType == rType && lType == DT_STRING)
            return DT_STRING;
    }
    //TODO more operators
    *opExists = FALSE;

    return DT_STRING;
}

char *
duckdbGetTableDefinition(char *tableName)
{
    return NULL;//TODO
}

char *
duckdbGetViewDefinition(char *viewName)
{
    return NULL;//TODO
}

int
duckdbGetCostEstimation(char *query)
{
    THROW(SEVERITY_RECOVERABLE,"%s","not supported yet");
    return -1;
}

List *
duckdbGetKeyInformation(char *tableName)
{
    sqlite3_stmt *rs; // TODO: adapt function
    StringInfo q;
    Set *key = STRSET();
    List *keys = NIL;
    int rc;

    q = makeStringInfo();
    appendStringInfo(q, QUERY_TABLE_COL_COUNT, tableName);
    rs = runQuery(q->data);

    while((rc = sqlite3_step(rs)) == SQLITE_ROW) // TODO: adapt function
    {
        boolean pk = sqlite3_column_int(rs,5) > 0; // TODO: adapt function
        const unsigned char *colname = sqlite3_column_text(rs,1); // TODO: adapt function

        if(pk)
        {
            addToSet(key, strToUpper(strdup((char *) colname)));
        }
    }

    HANDLE_ERROR_MSG(rc, SQLITE_DONE, "error getting attributes of table <%s>", tableName); // TODO: adapt function

    DEBUG_LOG("Key for %s are: %s", tableName, beatify(nodeToString(key)));

    if(!EMPTY_SET(key))
    {
        keys = singleton(key);
    }

    return keys;
}

DataType
duckdbBackendSQLTypeToDT (char *sqlType)
{
    if (regExMatch("INT", sqlType))
    {
        return DT_INT;
    }
    if (regExMatch("NUMERIC", sqlType)
            || regExMatch("REAL", sqlType)
            || regExMatch("FLOA", sqlType)
            || regExMatch("DOUB", sqlType))
    {
        return DT_FLOAT;
    }
    if (regExMatch("CHAR", sqlType)
            || regExMatch("CLOB", sqlType)
            || regExMatch("TEXT", sqlType))
    {
        return DT_STRING;
    }

    return DT_FLOAT;
}

char *
duckdbBackendDatatypeToSQL (DataType dt)
{
    switch(dt)
    {
        case DT_INT:
        case DT_LONG:
            return "INT";
            break;
        case DT_FLOAT:
            return "DOUBLE";
            break;
        case DT_STRING:
        case DT_VARCHAR2:
            return "TEXT";
            break;
        case DT_BOOL:
            return "BOOLEAN";
            break;
    }

    // keep compiler quiet
    return "TEXT";
}

HashMap *
duckdbGetMinAndMax(char* tableName, char* colName)
{
    HashMap *result_map = NEW_MAP(Constant, HashMap);
    sqlite3_stmt *rs; // TODO: adapt function
    StringInfo q;
    StringInfo colMinMax;
    List *attr = sqliteGetAttributes(tableName);
    List *aNames = getAttrDefNames(attr);
    List *aDTs = getAttrDataTypes(attr);
    int rc;

    q = makeStringInfo();
    colMinMax = makeStringInfo();

    // calculate min and max for each attribute
    FOREACH(char,a,aNames)
    {
        appendStringInfo(colMinMax, "min(%s) AS min_%s, max(%s) AS max_%s", a, a, a, a);
        appendStringInfo(colMinMax, "%s", FOREACH_HAS_MORE(a) ? ", " : "");
    }

    appendStringInfo(q, QUERY_TABLE_ATTR_MIN_MAX, colMinMax->data, tableName);
    rs = runQuery(q->data);

    while((rc = sqlite3_step(rs)) == SQLITE_ROW) // TODO: adapt function
    {
        int pos = 0;
        FORBOTH_LC(ac, dtc, aNames, aDTs)
        {
            char *aname = LC_STRING_VAL(ac);
            DataType dt = (DataType) LC_INT_VAL(dtc);
            HashMap *minmax = NEW_MAP(Constant,Constant);
            const unsigned char *minVal = sqlite3_column_text(rs,pos++); // TODO: adapt function
            const unsigned char *maxVal = sqlite3_column_text(rs,pos++); // TODO: adapt function
            Constant *min, *max;

            switch(dt)
            {
            case DT_INT:
                min = createConstInt(atoi((char *) minVal));
                max = createConstInt(atoi((char *) maxVal));
                break;
            case DT_LONG:
                min = createConstLong(atol((char *) minVal));
                max = createConstLong(atol((char *) maxVal));
                break;
            case DT_FLOAT:
                min = createConstFloat(atof((char *) minVal));
                max = createConstFloat(atof((char *) maxVal));
                break;
            case DT_STRING:
                min = createConstString((char *) minVal);
                max = createConstString((char *) maxVal);
                break;
            default:
                THROW(SEVERITY_RECOVERABLE, "received unkown DT from sqlite: %s", DataTypeToString(dt));
                break;
            }
            MAP_ADD_STRING_KEY(minmax, MIN_KEY, min);
            MAP_ADD_STRING_KEY(minmax, MAX_KEY, max);
            MAP_ADD_STRING_KEY(result_map, aname, minmax);
        }
    }

    HANDLE_ERROR_MSG(rc, SQLITE_DONE, "error getting min and max values of attributes for table <%s>", tableName); // TODO: adapt function

    DEBUG_NODE_BEATIFY_LOG("min maxes", MAP_GET_STRING(result_map, colName));

    return (HashMap *) MAP_GET_STRING(result_map, colName);
}

void
duckdbGetTransactionSQLAndSCNs (char *xid, List **scns, List **sqls,
        List **sqlBinds, IsolationLevel *iso, Constant *commitScn)
{
    THROW(SEVERITY_RECOVERABLE,"%s","not supported yet");
}

Node *
duckdbExecuteAsTransactionAndGetXID (List *statements, IsolationLevel isoLevel)
{
    THROW(SEVERITY_RECOVERABLE,"%s","not supported yet");
    return NULL;
}

Relation *
duckdbExecuteQuery(char *query)
{
    Relation *r = makeNode(Relation);
    sqlite3_stmt *rs = runQuery(query); // TODO: adapt function
    int numFields = sqlite3_column_count(rs); // TODO: adapt function
    int rc = SQLITE_OK; // TODO: adapt function

    // set schema
    r->schema = NIL;
    for(int i = 0; i < numFields; i++)
    {
        const char *name = sqlite3_column_name(rs, i); // TODO: adapt function
        r->schema = appendToTailOfList(r->schema, strdup((char *) name));
    }

    // read rows
    r->tuples = makeVector(VECTOR_NODE, T_Vector);
    while((rc = sqlite3_step(rs)) == SQLITE_ROW) // TODO: adapt function
    {
        Vector *tuple = makeVector(VECTOR_STRING, -1);
        for (int j = 0; j < numFields; j++)
        {
            if (sqlite3_column_type(rs,j) == SQLITE_NULL) // TODO: adapt function
            {
                vecAppendString(tuple, strdup("NULL"));
            }
            else
            {
                const unsigned char *val = sqlite3_column_text(rs,j); // TODO: adapt function
                vecAppendString(tuple, strdup((char *) val));
            }
        }
        VEC_ADD_NODE(r->tuples, tuple);
        DEBUG_NODE_LOG("read tuple <%s>", tuple);
    }

    HANDLE_ERROR_MSG(rc,SQLITE_DONE, "failed to execute query <%s>", query);

    rc = sqlite3_finalize(rs); // TODO: adapt function
    HANDLE_ERROR_MSG(rc,SQLITE_OK, "failed to finalize query <%s>", query);

    return r;
}

void
duckdbExecuteQueryIgnoreResults(char *query)
{
    sqlite3_stmt *rs = runQuery(query); // TODO: adapt function
    int rc = SQLITE_OK; // TODO: adapt function

    while((rc = sqlite3_step(rs)) == SQLITE_ROW) // TODO: adapt function
        ;

    HANDLE_ERROR_MSG(rc,SQLITE_DONE, "failed to execute query <%s>", query); // TODO: adapt function

    rc = sqlite3_finalize(rs); // TODO: adapt function
    HANDLE_ERROR_MSG(rc,SQLITE_OK, "failed to finalize query <%s>", query); // TODO: adapt function
}

static sqlite3_stmt * // TODO: adapt function
runQuery (char *q)
{ 
    sqlite3 *conn = plugin->conn; // TODO: adapt function
    sqlite3_stmt *stmt; // TODO: adapt function
    int rc;

    DEBUG_LOG("run query:\n<%s>", q);
    rc = sqlite3_prepare(conn, strdup(q), -1, &stmt, NULL); // TODO: adapt function
//    HANDLE_ERROR_MSG(rc, SQLITE_OK, "failed to prepare query <%s>", q);

   if (rc != SQLITE_OK) // TODO: adapt function
   {
       StringInfo _newmes = makeStringInfo();
       appendStringInfo(_newmes, "failed to prepare query <%s>", q);
       StringInfo _errMes = makeStringInfo();
       appendStringInfo(_errMes, strdup((char *) sqlite3_errmsg(plugin->conn))); // TODO: adapt function
       FATAL_LOG("error (%s)\n%u\n\n%s", _errMes->data, rc, _newmes->data);
   }

    return stmt;
}

static DataType
stringToDT (char *dataType) // TODO: double check if data types are the same
{
   DEBUG_LOG("data type %s", dataType);
   char *lowerDT = strToLower(dataType);

   if (isSubstr(lowerDT, "int"))
       return DT_INT;
   if (isSubstr(lowerDT, "char") || isSubstr(lowerDT, "clob") || isSubstr(lowerDT, "text"))
       return DT_STRING;
   if (isSubstr(lowerDT, "real") || isSubstr(lowerDT, "floa") || isSubstr(lowerDT, "doub"))
       return DT_FLOAT;

   return DT_STRING;
}

static char *
duckdbGetConnectionDescription (void)
{
    return CONCAT_STRINGS("DuckDB:", getStringOption("connection.db"));
}

#define ADD_AGGR_FUNC(name) addToSet(plugin->plugin.cache->aggFuncNames, strdup(name))
#define ADD_WIN_FUNC(name) addToSet(plugin->plugin.cache->winFuncNames, strdup(name))
#define ADD_BOTH_FUNC(name) \
    do { \
        addToSet(plugin->plugin.cache->aggFuncNames, strdup(name)); \
        addToSet(plugin->plugin.cache->winFuncNames, strdup(name)); \
    } while (0)

static void
initCache(CatalogCache *c)
{
    ADD_BOTH_FUNC("avg");
    ADD_BOTH_FUNC("count");
    ADD_BOTH_FUNC("group_concat");
    ADD_BOTH_FUNC("max");
    ADD_BOTH_FUNC("min");
    ADD_BOTH_FUNC("sum");
    ADD_BOTH_FUNC("total");

    ADD_WIN_FUNC("row_number");
    ADD_WIN_FUNC("rank");
    ADD_WIN_FUNC("dense_rank");
    ADD_WIN_FUNC("percent_rank");
    ADD_WIN_FUNC("cum_dist");
    ADD_WIN_FUNC("ntile");
    ADD_WIN_FUNC("lag");
    ADD_WIN_FUNC("lead");
    ADD_WIN_FUNC("first_value");
    ADD_WIN_FUNC("last_value");
    ADD_WIN_FUNC("nth_value");
}

#else


MetadataLookupPlugin *
assembleDuckDBMetadataLookupPlugin (void)
{
    return NULL;
}

int
duckdbInitMetadataLookupPlugin (void)
{
    return EXIT_SUCCESS;
}

int
duckdbShutdownMetadataLookupPlugin (void)
{
    return EXIT_SUCCESS;
}

int
duckdbDatabaseConnectionOpen (void)
{
    return EXIT_SUCCESS;
}

int
duckdbDatabaseConnectionClose()
{
    return EXIT_SUCCESS;
}

boolean
duckdbIsInitialized (void)
{
    return FALSE;
}

boolean
duckdbCatalogTableExists (char * tableName)
{
    return FALSE;
}

boolean
duckdbCatalogViewExists (char * viewName)
{
    return FALSE;
}

List *
duckdbGetAttributes (char *tableName)
{
    return NIL;
}

List *
duckdbGetAttributeNames (char *tableName)
{
    return NIL;
}

boolean
duckdbIsAgg(char *functionName)
{
    return FALSE;
}

boolean
duckdbIsWindowFunction(char *functionName)
{
    return FALSE;
}

char *
duckdbGetTableDefinition(char *tableName)
{
    return NULL;
}

char *
duckdbGetViewDefinition(char *viewName)
{
    return NULL;
}

DataType
duckdbBackendSQLTypeToDT (char *sqlType)
{
    return DT_INT;
}

char *
duckdbBackendDatatypeToSQL (DataType dt)
{
    return NULL;
}

HashMap *
duckdbGetMinAndMax(char* tableName, char* colName)
{
    return NULL;
}

void
duckdbGetTransactionSQLAndSCNs (char *xid, List **scns, List **sqls,
        List **sqlBinds, IsolationLevel *iso, Constant *commitScn)
{
}

Node *
duckdbExecuteAsTransactionAndGetXID (List *statements, IsolationLevel isoLevel)
{
    return NULL;
}

int
duckdbGetCostEstimation(char *query)
{
    return 0;
}

List *
duckdbGetKeyInformation(char *tableName)
{
    return NULL;
}

Relation *
duckdbExecuteQuery(char *query)
{
    return NULL;
}

#endif