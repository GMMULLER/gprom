%{
#include "common.h"
#include "mem_manager/mem_mgr.h"
#include "model/expression/expression.h"
#include "model/list/list.h"
#include "model/node/nodetype.h"
#include "model/datalog/datalog_model.h"
#include "model/query_block/query_block.h"
#include "parser/parse_internal_dl.h"
#include "log/logger.h"
#include "model/query_operator/operator_property.h"

#define RULELOG(grule) \
    { \
        TRACE_LOG("Parsing grammer rule <%s>", #grule); \
    }
    
#undef free

Node *dlParseResult = NULL;
%}

%name-prefix "dl"

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
 %token <stringVal> StringLiteral Identifier
  
/*
 * Tokens for in-built keywords
 *        Currently keywords related to basic query are considered.
 *        Later on other keywords will be added.
 */
%token <stringVal> NEGATION RULE_IMPLICATION

/* tokens for constant and idents */
%token <intVal> intConst
%token <floatVal> floatConst
%token <stringVal> stringConst
%token <stringVal> IDENT
%token <stringVal> VARIDENT
%token <stringVal> parameter
%token <stringVal> comparisonOp arithmeticOp

/* comparison and arithmetic operators */
/* %token <stringVal> '+' '-' '*' '/' '%' '^' '&' '|' '!' ')' '(' '=' */


/*
 * Declare token for operators specify their associativity and precedence
 *
%left NEGATION

%nonassoc '(' ')'


/*
 * Types of non-terminal symbols
 */
/* statements and their parts */ 
%type <list> stmtList
%type <node> statement program

%type <node> rule fact rulehead atom arg variable constant comparison 
%type <list> atomList argList rulebody 

/* start symbol */
%start program

/*************************************************************/
/* RULE SECTION 											 */
/*************************************************************/
%%

program:
		stmtList 
			{ 
				RULELOG("program::stmtList");
				$$ = (Node *) createDLProgram ($1, NULL);
				dlParseResult = (Node *) $$; 
			}
		;

/* Rule for all types of statements */
stmtList: 
		statement ';'
			{ 
				RULELOG("stmtList::statement"); 
				$$ = singleton($1);
			}
		| stmtList statement ';' 
			{
				RULELOG("stmtlist::stmtList::statement");
				$$ = appendToTailOfList($1, $2); 
			}
	;
	
statement:
		rule { RULELOG("statement::rule"); $$ = $1; }
		| fact { RULELOG("statement::fact"); $$ = $1; }
	;
	
rule:
		rulehead RULE_IMPLICATION rulebody '.' 
			{ 
				RULELOG("rule::head::body"); 
				$$ = (Node *) createDLRule((DLAtom *) $1,$3); 
			}
	;
	
fact:
		atom '.' { RULELOG("fact::atom"); $$ = $1; } /* do more elegant? */
	;

rulehead:
		atom  { RULELOG("rulehead::atom"); $$ = $1; }
	;
	
rulebody:
		atomList { RULELOG("rulebody::atomList"); $$ = $1; }
	;
	
atomList:
		atomList ',' atom 
			{
				RULELOG("atomList::atom");
				$$ = appendToTailOfList($1,$3); 
			}
		| atom
			{
				RULELOG("atomList::atom");
				$$ = singleton($1); 
			}
	;

atom:
 		NEGATION IDENT '(' argList ')' 
 			{ 
 				RULELOG("atom::NEGATION");
 				$$ = (Node *) createDLAtom($2, $4, TRUE); 
			}
 		| IDENT '(' argList ')' 
 			{
 				RULELOG("atom::IDENT");
 				$$ = (Node *) createDLAtom($1, $3, FALSE); 
			}
		| comparison
			{
				RULELOG("atom::comparison");
				$$ = (Node *) $1;
			}
 	;

/*
constAtom:
		IDENT '(' constList ')' 
			{
				RULELOG("constAtom");
				$$ = createDLAtom($1,$3,FALSE); 
			}
	;
*/

argList:
 		argList ',' arg 
 			{
 				RULELOG("argList::argList::arg");
 				$$ = appendToTailOfList($1,$3); 
			}
 		| arg		
 			{
 				RULELOG("argList::arg");
 				$$ = singleton($1); 
			}
 	;

/* 	
constList:
		constList ',' constant { $$ = appendToTailOfList($1,$2); }
		| constant { $$ = singleton($1); }
	;
*/

comparison:
		arg comparisonOp arg
			{
				RULELOG("comparison::arg::op::arg");
				$$ = (Node *) createDLComparison($2, $1, $3);
			}
	;

/* add skolem */ 	
arg:
 		variable 
 			{
 				RULELOG("arg:variable");
 		 		$$ = $1; 
	 		}
 		| constant 
 			{ 
 				RULELOG("arg:constant");
 				$$ = $1; 
			}
	;
 		
variable:
		VARIDENT 
			{
				RULELOG("variable"); 
				$$ = (Node *) createConstString($1); 
			}
	;
	
constant: 
		intConst 
			{
				RULELOG("constant::intConst"); 
				$$ = (Node *) createConstInt($1); 
			}
		| floatConst
			{
				RULELOG("constant::floatConst"); 
				$$ = (Node *) createConstFloat($1); 
			}
		| stringConst 
			{
				RULELOG("constant::stringConst");
				$$ = (Node *) createConstString($1); 
			}
	;
	
