#ifndef INCLUDE_METADATA_LOOKUP_METADATA_LOOKUP_DUCKDB_H_
#define INCLUDE_METADATA_LOOKUP_METADATA_LOOKUP_DUCKDB_H_

#include "metadata_lookup/metadata_lookup.h"

#ifdef HAVE_DUCKDB_BACKEND
#include "duckdb.h"
#endif

extern MetadataLookupPlugin *assembleDuckDBMetadataLookupPlugin (void);

/* plugin methods */
extern int duckdbInitMetadataLookupPlugin (void);
extern int duckdbShutdownMetadataLookupPlugin (void);
extern int duckdbDatabaseConnectionOpen (void);
extern int duckdbDatabaseConnectionClose();
extern boolean duckdbIsInitialized (void);

extern boolean duckdbCatalogTableExists (char * tableName);
extern boolean duckdbCatalogViewExists (char * viewName);
extern List *duckdbGetAttributes (char *tableName);
extern List *duckdbGetAttributeNames (char *tableName);
extern boolean duckdbIsAgg(char *functionName);
extern boolean duckdbIsWindowFunction(char *functionName);
extern DataType duckdbGetFuncReturnType (char *fName, List *argTypes, boolean *funcExists);
extern DataType duckdbGetOpReturnType (char *oName, List *argTypes, boolean *opExists);
extern char *duckdbGetTableDefinition(char *tableName);
extern char *duckdbGetViewDefinition(char *viewName);
extern int duckdbGetCostEstimation(char *query);
extern List *duckdbGetKeyInformation(char *tableName);
extern DataType duckdbBackendSQLTypeToDT (char *sqlType);
extern char * duckdbBackendDatatypeToSQL (DataType dt);
extern HashMap *duckdbGetMinAndMax(char* tableName, char* colName);

extern Relation *sqliteExecuteQuery(char *query); // TODO: adapt
extern void sqliteExecuteQueryIgnoreResults(char *query); // TODO: adapt
extern void sqliteGetTransactionSQLAndSCNs (char *xid, List **scns, List **sqls, 
        List **sqlBinds, IsolationLevel *iso, Constant *commitScn); // TODO: adapt
extern Node *sqliteExecuteAsTransactionAndGetXID (List *statements, IsolationLevel isoLevel); // TODO: adapt


#endif /* INCLUDE_METADATA_LOOKUP_METADATA_LOOKUP_DUCKDB_H_ */