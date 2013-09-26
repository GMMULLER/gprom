/*
 * Sql_Parser.y
 *     This is a bison file which contains grammar rules to parse SQLs
 */

%{
#include <stdio.h>
#include "model/expression/expression.h"
#include "model/list/list.h"
#include "model/node/nodetype.h"
#include "sql_parser.lex.h"

//int yylex(void);
//void yyerror (char const *);
%}

%union {
	/* 
	 * Declare some C structure those will be used as data type
     * for various tokens used in grammar rules.
	 */
	 Node *node;
	 List *list;
	 char *stringVal;
	 int intVal;
	 double floatVal;
}

/*
 * Declare tokens for name and literal values
 * Declare tokens for user variables
 */
%token <intVal> intConst
%token <floatVal> floatConst
%token <stringVal> stringConst
%token <stringVal> identifier
%token <stringVal> comparisonop arithmeticop

/*
 * Tokens for in-built keywords
 *		Currently keywords related to basic query are considered.
 *		Later on other keywords will be added.
 */
%token <stringVal> SELECT
%token <stringVal> PROVENANCE OF
%token <stringVal> FROM
%token <stringVal> AS
%token <stringVal> WHERE
%token <stringVal> DISTINCT
%token <stringVal> ON
%token <stringVal> STARALL
%token <StringVal> UPDATE DELETE
%token <stringVal> AND OR LIKE NOT IN NULL BETWEEN

/*
 * Declare token for operators specify their associativity and precedence
 */
/* Comparison operator */
%left comparisonop arithmeticop
%left AND OR NOT IN NULL BETWEEN LIKE

/* Arithmetic operators : FOR TESTING 
%left operator
%left '&' '|'
%left '+' '-'
%left '*' '/' '%'
%left '^'
*/


/*
 * Tokens for functions
 */
//%token FUNCIDENTIFIER	
		// Added a token for all functions inbuilt and user defined functions.

/*
 * Types of non-terminal symbols
 */
%type <node> stmt provStmt dmlStmt
%type <node> selectQuery deleteQuery updateQuery
		// Its a query block model that defines the structure of query.
%type <list> selectClause optionlFrom fromClause exprList // select and from clauses are lists
%type <node> selectItem fromClauseItem optionalDistinct optionalWhere
%type <node> expression constant attributeRef sqlFunctionCall
//%type <stringVal> operatorExpression operator arithmaticOperator comparisonOperator sqlOperators

%type <stringVal> optionalAlias

%start stmt

%%

/* Rule for all types of statements */
stmt: 
		provStmt ';'
    	| dmlStmt ';'		// DML statement can be select, update, insert, delete
    ;

/* 
 * Rule to parse a query asking for provenance
 */
provStmt: 
 		PROVENANCE OF '(' dmlStmt ')' 	{ $$ = createProvStmt($4); }
 	;

/*
 * Rule to parse all DML queries.
 */
dmlStmt:
		selectQuery
		| deleteQuery
		| updateQuery
	;
	
	
/*
 * Rule to parse delete query
 */ 
 deleteQuery: 
 		DELETE 		{ $$ = NULL; }
 		
 /*
  * Rules to parse update query
  */
updateQuery:
		UPDATE		{ $$ = NULL; }
 
/*
 * Rule to parse select query
 * Currently it will parse following type of select query:
 * 			'SELECT [DISTINCT clause] selectClause FROM fromClause WHERE whereClause'
 */
selectQuery: 
		SELECT optionalDistinct selectClause optionlFrom optionalWhere
			{
				$$ = (Node *) createQueryBlock($2,$3,$4,$5);
			}
	;


/*
 * Rule to parse optional distinct clause.
 */ 
optionalDistinct: 
		/* empty */ 					{ $$ = NULL; }
		| DISTINCT						{ $$ = createDistinct(NULL); }
		| DISTINCT ON '(' exprList ')'	{ $$ = createDistinct($4); }
	;
						

/*
 * Rule to parse the select clause items.
 */
selectClause: 
		selectItem 						{ $$ = $1; }
		| selectClause ',' selectItem 
			{ 
				$$ = appendToTailOfList($1, $3); 
			}
	;

selectItem:
 		expression							
 			{ 
 				$$ = createProjectionNode($1, NULL); 
 			}
 		| expression AS identifier 			
 			{ 
 				$$ = createProjectionNode($1, $3);
			}
	; 

/*
 * Rule to parse an expression list
 */
exprList: 
		expression						{ $$ = singleton($1); }		
		 | exprList ',' expression 		{ $$ = appendToTailOfList($1, $3); }
	;
		 

/*
 * Rule to parse expressions used in various lists
 */
expression: 
		constant
		| attributeRef 
/*		| operatorExpression */
		| sqlFunctionCall
/*		| STARALL				{ return NULL;} //TODO /* this token is for '*' */
	;
			
/*
 * Constant parsing
 */
constant: 
		intConst			{ $$ = createIntConst($1); }
		| floatConst		{ $$ = createFloatConst($1); }
		| stringConst		{ $$ = createStringConst($1); }
	;
			
/*
 * Parse attribute reference
 */
attributeRef: 
		identifier 		{ $$ = createAttributeReference($1); }
	;

/*
 * Parse operator expression
 */
/* operatorExpression: 
		expression operator expression 		{ $$ = createOpExpression($1,$2,$3) }
	;

operator: 
		arithmaticOperator
		| logicalOperator 
		| comparisonOperator
		| sqlOperators
	;
	
arithmaticOperator:
		arithmeticop 		{ $$ = $1; }
	;

logicalOperator:
		'&&' | '||' | '!'		{ $$ = $1; }
	;

	
comparisonOperator:
		comparisonop		{ $$ = $1; }
	;
	
sqlOperators:
		AND | OR | NOT | IN | NULL | BETWEEN | LIKE 	{ $$ = $1; }
	;
*/
	
/*
 * Rule to parse function calls
 */
sqlFunctionCall: 
		identifier '(' exprList ')'  		
			{ 
				$$ = createFunctionCall($1, $3); 
			}
	;

/*
 * Rule to parse from clause
 *			Currently implemented for basic from clause.
 *			Later on other forms of from clause will be added.
 */
optionlFrom: 
		/* empty */ 		 	{ $$ = NULL; }
		| FROM fromClause		{ $$ = $2; }
	;
			
fromClause: 
		fromClauseItem					{ $$ = singeltonP($1); }
		| fromClause ',' fromClauseItem { $$ = appendToTailOfList($1, $3); }
	;
	
fromClauseItem:
		identifier optionalAlias { $$ = createTableNode($1, $2); }
	;
	
optionalAlias: 
		identifier				{ $$ = $1; }
		| AS identifier			{ $$ = $2; }
	;
		  
/*
 * Rule to parse the where clause.
 */
optionalWhere: 
		/* empty */ 			{ $$ = NULL; }
		| WHERE expression			{ $$ = $2; } 
	;

%%