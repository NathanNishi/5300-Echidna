// sql5300.cpp, Ben Gruher and Priyanka Patil, Seattle University, CPSC 5300, Spring 2021

#include "db_cxx.h"
#include "SQLParser.h"

/**
 * Converts the hyrise parse tree back into a SQL string
 * @param parseTree	hyrise parse tree pointer
 * @return 		SQL equivalent to parseTree
 */
std::string execute(hsql::SQLParserResult* parseTree) {
	// FIXME - implement execute
	return "Execute return value here- query was valid";
}

int main(void) {
	// FIXME - insert Berkeley DB statements here
	
	std::cout << "quit to end" << std::endl;
	std::string query;
	while(true) {
		std::cout << "SQL> ";
		std::getline(std::cin, query);
		if(query == "quit") {
			return 1;
		}

		// parse query to get parse tree
		hsql::SQLParserResult* parseTree = hsql::SQLParser::parseSQLString(query);
		// check that parse tree is valid
		if (parseTree->isValid()) {

			// pass to execute, which returns a string
			// print string
			std::cout << execute(parseTree) << std::endl;
			// std::cout << "VALID" << std::endl;
		}
		else {
			// print error message
			std::cout << "Invalid SQL Statement" << std::endl;
		}

		std::cout << query << std::endl;
	}
	return 1;
}


