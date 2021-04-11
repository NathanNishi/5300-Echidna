// sql5300.cpp, Ben Gruher and Priyanka Patil, Seattle University, CPSC 5300, Spring 2021

#include <stdio.h>
#include <stdlib.h>
#include <string.h>
#include "SQLParser.h"
#include "db_cxx.h"
#include "sqlhelper.h"

const char *HOME = "cpsc5300/data";
const char *MLESTONE1 = "milestone1.db";
const unsigned int BLOCK_SZ = 4096;

using namespace std;
using namespace hsql;

//method declarations
string printSelectStatement(const SelectStatement *stmt);
string printCreateStatement(const CreateStatement* stmt);
string printInsertStatement(const InsertStatement* stmt);
string execute(const SQLStatement *stmt);
string tableRefExprToString(const TableRef* table);
string operatorExprToString(const Expr* expr);
string printExpr(const Expr *expression);
string columnDefiinitionToString(const ColumnDefinition* col);

/**
 * Convert the hyrise select statement into the equivalent string
 * @param stmt  select statement to unparse
 * @return     SQL string equivalent to *stmt
 */
string printSelectStatement(const SelectStatement *stmt) {
	string ret("SELECT ");
	bool notFirst = false;
	for (Expr* expr : *stmt->selectList) {
		if(notFirst) {
            ret += ", ";
        } 
		ret += printExpr(expr);
		notFirst = true;
	}
	ret += " FROM " + tableRefExprToString(stmt->fromTable);
	if (stmt->whereClause != NULL) {
		ret += " WHERE " + printExpr(stmt->whereClause);
	}
	return ret;
}

/**
 * Convert the hyrise create statement into the equivalent string
 * @param stmt  create statement to unparse
 * @return     SQL string equivalent to *stmt
 */
string printCreateStatement(const CreateStatement* stmt){
	string ret("CREATE TABLE ");
	bool notFirst = false;
	ret += string(stmt->tableName) + " (";
	for (ColumnDefinition* column : *stmt->columns) {
        if(notFirst) {
            ret += ", ";
        }
        ret += columnDefiinitionToString(column);
        notFirst = true;
    }
    ret += ")";
	return ret;
}

/**
 * Convert the hyrise insert statement into the equivalent string
 * @param stmt  insert statement to unparse
 * @return     SQL string equivalent to *stmt
 */
string printInsertStatement(const InsertStatement* stmt) {
	string ret("INSERT ");
	return ret;
}

/**
 * Converts the hyrise parse tree back into a SQL string
 * @param stmt	hyrise statement
 * @return 		SQL equivalent to parseTree
 */
string execute(const SQLStatement *stmt) {
	string ret;
	switch (stmt->type()) {
		case kStmtSelect:{
			//cout << "Select DETECTED" << endl;
			const SelectStatement* selectStmt = (const SelectStatement *) stmt;
			ret += printSelectStatement(selectStmt);
      		return ret;
      	}
		case kStmtCreate:{
			//cout << "Create DETECTED" << endl;
			const CreateStatement* createStmt = (const CreateStatement *) stmt;
			ret += printCreateStatement(createStmt);
			return ret;
		}
		case kStmtInsert:{
			//cout << "Insert DETECTED" << endl;
			const InsertStatement* insertStmt = (const InsertStatement *) stmt;
            ret += printInsertStatement(insertStmt);
            return ret;
        }
        case kStmtDrop:{
        	//cout << "Drop DETECTED" << endl;
            ret += "DROP";
            return ret;
        }
        case kStmtImport:{
        	//cout << "Import DETECTED" << endl;
        	ret += "IMPORT";
        	return ret;
        }
        case kStmtShow:{
        	//cout << "Show DETECTED" << endl;
        	ret += "SHOW";
        	return ret;
        }
        default:{
            ret += "Not Recognized";
            return ret;
        }
		}
}

/**
 * Convert the hyrise table reference into the equivalent string
 * @param table  TableRef to unparse
 * @return     SQL string equivalent to *table
 */
string tableRefExprToString(const TableRef* table){
	string ret;
	// handle multiple FROM tables
	if (table->list != NULL) {
		bool firstFromTable = true;
		for (TableRef* tbl : *table->list) {
			if (!firstFromTable) {
				ret += ", ";
			}
			else {
				firstFromTable = false;
			}
			ret += tableRefExprToString(tbl);
		}
		return ret;
	}
	switch (table->type) {
		case kTableName:
			ret += table->name;
			if (table->alias != NULL) {
                ret += string(" AS ") + table->alias;
            }
			break;
		case kTableSelect:
			ret += execute(table->select);
			break;
		case kTableJoin:
			//cout << "JOIN Table ";
			ret += tableRefExprToString(table->join->left);
			switch (table->join->type) {
                case kJoinInner:
                    ret += " JOIN ";
                    break;
                case kJoinLeft:
                    ret += " LEFT JOIN ";
                    break;
                default:
                    ret += " UNSUPPORTED JOIN";
                    break;
            }
            ret += tableRefExprToString(table->join->right);
            // If there is a JOIN condition
            if (table->join->condition != NULL) {
                ret += " ON " + printExpr(table->join->condition);
            }
			break;
		default:
			ret += " UNSUPPORTED TYPE";
            break;
	}
	return ret;
}

/**
 * Convert the hyrise operator Expression into the equivalent string
 * @param expr  operator expression to unparse
 * @return     SQL string equivalent to *expr
 */
string operatorExprToString(const Expr* expr){
	string ret;
	if (expr == NULL) {
        return "null";
    }
    switch (expr->opType) {
    	case Expr::SIMPLE_OP:
    		//cout << "Simple Op DETECTED" << endl;
    		ret += printExpr(expr->expr) + expr->opChar + printExpr(expr->expr2);
    		break; 
    	case Expr::AND:
        	//cout << "AND DETECTED" << endl;
        	ret += "AND";
        	break;
    	case Expr::OR:
        	//cout << "OR DETECTED" << endl;
        	ret += "OR";
        	break;
        case Expr::NOT:
        	//cout << "NOT DETECTED" << endl;
        	ret += "NOT";
        	break;
    	default:
    		ret += expr->opType;
        	break;
	}
	return ret;
}

/**
 * Convert the hyrise ColumnDefinition AST back into the equivalent SQL
 * @param col  column definition to unparse
 * @return     SQL equivalent to *col
 */
string columnDefiinitionToString(const ColumnDefinition* col) {
    string ret(col->name);
    switch (col->type) {
        case ColumnDefinition::DOUBLE:
            ret += " DOUBLE";
            break;
        case ColumnDefinition::INT:
            ret += " INT";
            break;
        case ColumnDefinition::TEXT:
            ret += " TEXT";
            break;
        default:
            ret += " UNSUPPORTED TYPE";
            break;
    }
    return ret;
}

/**
 * Convert the hyrise Expression into the equivalent string
 * @param expression  expression to unparse
 * @return     SQL string equivalent to *expression
 */
string printExpr(const Expr *expression){
	string ret;
	switch (expression->type) {
		case kExprStar:
            //cout << "* DETECTED" << endl;
            ret += "*";
            break;
		case kExprColumnRef:
            //cout << "Column Ref DETECTED" << endl;
            if(expression->table != NULL) {
                ret += string(expression->table) + "." + string(expression->name);
            }
			else {
				ret += string(expression->name);
			}
            break;
        case kExprLiteralString:
            //cout << "String DETECTED" << endl;
            ret += expression->name;
            break;
        case kExprLiteralInt:
            //cout << "Int DETECTED" << endl;
            ret += to_string(expression->ival);
            break;
        case kExprLiteralFloat:
            //cout << "Float DETECTED" << endl;
            ret += to_string(expression->fval);
            break;
        case kExprOperator:
            //cout << "Operator DETECTED" << endl;
            ret += operatorExprToString(expression);
            break;
        case kExprFunctionRef:
      		ret += expression->name;
      		break;
        default:
            //cout << "Unrecognized expression type!" << endl;
            ret += "EXPRESSION NOT RECOGNIZED";
            break;
	}
	return ret;
}


int main(void) {
	const char *home = getenv("HOME");
	string envdir = string(home) + "/" + HOME;
	cout<< "running with database environment at " << envdir << endl;


	// create database
	DbEnv env(0U);
	env.set_message_stream(&std::cout);
	env.set_error_stream(&std::cerr);
	env.open(envdir.c_str(), DB_CREATE | DB_INIT_MPOOL, 0);

	Db db(&env, 0);
	db.set_message_stream(env.get_message_stream());
	db.set_error_stream(env.get_error_stream());
	db.set_re_len(BLOCK_SZ); // Set record length to 4K
	db.open(NULL, MLESTONE1, NULL, DB_RECNO, DB_CREATE | DB_TRUNCATE, 0644); // Erases anything already there

	// user input loop
	cout << "quit to end" << endl;
	string query;
	while(true) {
		cout << "SQL>  ";
		getline(cin, query);
		if(query == "quit") {
			return 1;
		}
		// parse query to get parse tree
		SQLParserResult* parseTree = SQLParser::parseSQLString(query);
		// check that parse tree is valid
		if (parseTree->isValid()) {
			for (size_t i = 0; i < parseTree->size(); i++) {
			cout << execute(parseTree->getStatement(i)) << endl;
			}
		} else {
			// print error message
			cout << "Invalid SQL: " << query << endl;
		}
	}

	char block[BLOCK_SZ];
	Dbt data(block, sizeof(block));
	int block_number;
	Dbt key(&block_number, sizeof(block_number));
	block_number = 1;
	strcpy(block, "Milestone1!");
	db.put(NULL, &key, &data, 0);  // write block #1 to the database

	Dbt rdata;
	db.get(NULL, &key, &rdata, 0); // read block #1 from the database
	cout << "Read (block #" << block_number << "): '" << (char *)rdata.get_data() << "'";
	cout << " (expect 'Milestone1!')" << endl;

	return EXIT_SUCCESS;
}
