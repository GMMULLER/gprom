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
#ifdef HAVE_DUCKDB_BACKEND // TODO: implement duck plugin infrastructure

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

    p->type = METADATA_LOOKUP_PLUGIN_DUCKDB; // TODO: add this plugin option

    p->initMetadataLookupPlugin = duckdbInitMetadataLookupPlugin; 
    p->databaseConnectionOpen = duckdbDatabaseConnectionOpen; 
    p->databaseConnectionClose = duckdbDatabaseConnectionClose; 
    p->shutdownMetadataLookupPlugin = duckdbShutdownMetadataLookupPlugin; 
    p->isInitialized = duckdbIsInitialized; 
    p->catalogTableExists = duckdbCatalogTableExists; 
    p->catalogViewExists = duckdbCatalogViewExists;
    p->getAttributes = duckdbGetAttributes; // TODO: adapt function
    p->getAttributeNames = duckdbGetAttributeNames; // TODO: adapt function
    p->isAgg = duckdbIsAgg; // TODO: adapt function
    p->isWindowFunction = duckdbIsWindowFunction; // TODO: adapt function
    p->getFuncReturnType = duckdbGetFuncReturnType; // TODO: adapt function
    p->getOpReturnType = duckdbGetOpReturnType; // TODO: adapt function
    p->getTableDefinition = duckdbGetTableDefinition; // TODO: adapt function
    p->getViewDefinition = duckdbGetViewDefinition; // TODO: adapt function
    p->getTransactionSQLAndSCNs = duckdbGetTransactionSQLAndSCNs; // TODO: adapt function
    p->executeAsTransactionAndGetXID = duckdbExecuteAsTransactionAndGetXID; // TODO: adapt function
    p->getCostEstimation = duckdbGetCostEstimation; // TODO: adapt function
    p->getKeyInformation = duckdbGetKeyInformation; // TODO: adapt function
    p->executeQuery = duckdbExecuteQuery; // TODO: adapt function
    p->executeQueryIgnoreResult = duckdbExecuteQueryIgnoreResults; // TODO: adapt function
    p->connectionDescription = duckdbGetConnectionDescription; // TODO: adapt function
    p->sqlTypeToDT = duckdbBackendSQLTypeToDT; // TODO: adapt function
    p->dataTypeToSQL = duckdbBackendDatatypeToSQL; // TODO: adapt function
    p->getMinAndMax = duckdbGetMinAndMax; // TODO: adapt function
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

