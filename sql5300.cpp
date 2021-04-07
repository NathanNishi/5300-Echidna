// sql5300.cpp, Ben Gruher and Priyanka Patil, Seattle University, CPSC 5300, Spring 2021

#include "db_cxx.h"
#include "SQLParser.h"


const char *HOME = "cpsc5300/data";
const char *EXAMPLE = "example.db";
const unsigned int BLOCK_SZ = 4096;


/**
 * Converts the hyrise parse tree back into a SQL string
 * @param parseTree	hyrise parse tree pointer
 * @return 		SQL equivalent to parseTree
 */
std::string execute(hsql::SQLParserResult* parseTree) {
	// FIXME - implement execute
	for(size_t i = 0; i < parseTree->size(); i++) {
		switch (parseTree->getStatement(i)->type()) {
		case hsql::kStmtSelect:
			std::cout << "This was a select statement" << std::endl;
      			break;
		case hsql::kStmtCreate:
			std::cout << "This was a create statement" << std::endl;
		}
		// std::cout << parseTree->getStatement(i) << std::endl;
	}
	return "VALID";
}

int main(void) {
	// Berkeley DB statements here
	const char *home = std::getenv("HOME");
	std::string envdir = std::string(home) + "/" + HOME;
	
	DbEnv env(0U);
	env.set_message_stream(&std::cout);
	env.set_error_stream(&std::cerr);
	env.open(envdir.c_str(), DB_CREATE | DB_INIT_MPOOL, 0);

	Db db(&env, 0);
	db.set_message_stream(env.get_message_stream());
	db.set_error_stream(env.get_error_stream());
	db.set_re_len(BLOCK_SZ); // Set record length to 4K
	db.open(NULL, EXAMPLE, NULL, DB_RECNO, DB_CREATE | DB_TRUNCATE, 0644); // Erases anything already there



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
			std::cout << "Invalid SQL: " << query << std::endl;
		}
		
		delete parseTree;

		std::cout << query << std::endl;
	}
	return 1;
}


