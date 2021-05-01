/**
 * @file SQLExec.cpp - implementation of SQLExec class
 * @author Kevin Lundeen
 * @see "Seattle University, CPSC5300, Spring 2021"
 */
#include "SQLExec.h"

using namespace std;
using namespace hsql;

// define static data
Tables *SQLExec::tables = nullptr;

// make query result be printable
ostream &operator<<(ostream &out, const QueryResult &qres) {
    if (qres.column_names != nullptr) {
        for (auto const &column_name: *qres.column_names)
            out << column_name << " ";
        out << endl << "+";
        for (unsigned int i = 0; i < qres.column_names->size(); i++)
            out << "----------+";
        out << endl;
        for (auto const &row: *qres.rows) {
            for (auto const &column_name: *qres.column_names) {
                Value value = row->at(column_name);
                switch (value.data_type) {
                    case ColumnAttribute::INT:
                        out << value.n;
                        break;
                    case ColumnAttribute::TEXT:
                        out << "\"" << value.s << "\"";
                        break;
                    default:
                        out << "???";
                }
                out << " ";
            }
            out << endl;
        }
    }
    out << qres.message;
    return out;
}

QueryResult::~QueryResult() {
    if (column_names != nullptr) {
        delete column_names;
    }
    if (column_attributes != nullptr) {
        delete column_attributes;
    }
    if (rows != nullptr) {
        for (auto row : *rows) {
            delete row;
        }
        delete rows;
    }
}


QueryResult *SQLExec::execute(const SQLStatement *statement) {
    //initialize _tables table, if not yet present
    if (SQLExec::tables == nullptr) {
        SQLExec::tables = new Tables();
    }
    
    try {
        switch (statement->type()) {
            case kStmtCreate:
                return create((const CreateStatement *) statement);
            case kStmtDrop:
                return drop((const DropStatement *) statement);
            case kStmtShow:
                return show((const ShowStatement *) statement);
            default:
                return new QueryResult("not implemented");
        }
    } catch (DbRelationError &e) {
        throw SQLExecError(string("DbRelationError: ") + e.what());
    }
}

void
SQLExec::column_definition(const ColumnDefinition *col, Identifier &column_name, ColumnAttribute &column_attribute) {
    column_name = col->name;
    switch (col->type) {
    case ColumnDefinition::TEXT:
        column_attribute.set_data_type(ColumnAttribute::TEXT);
        break;
    case ColumnDefinition::INT:
        column_attribute.set_data_type(ColumnAttribute::INT);
        break;
    default:
        throw SQLExecError("Data type is not supported");
    }
}

QueryResult *SQLExec::create(const CreateStatement *statement) {
    if (statement->type != CreateStatement::kTable) {
        return new QueryResult("Unrecognized CREATE type");
    }

    // update table schema
    Identifier table_name = statement->tableName;
    ValueDict row;
    row["table_name"] = table_name;
    Handle table_handle = SQLExec::tables->insert(&row);

    // get column schema
    Identifier col_name;
    ColumnNames col_names;
    ColumnAttribute col_attribute;
    ColumnAttributes col_attributes;
    for (ColumnDefinition* col : *statement->columns) {
        column_definition(col, col_name, col_attribute);
        col_names.push_back(col_name);
        col_attributes.push_back(col_attribute);
    }

    // update column schema
    try {
        Handles col_handles;
        DbRelation& columns = SQLExec::tables->get_table(Columns::TABLE_NAME);

        try {
            for (unsigned int i = 0; i < col_names.size(); i++) {
                row["column_name"] = col_names[i];
                row["data_type"] = Value(
                        col_attributes[i].get_data_type()
                                == ColumnAttribute::INT ? "INT" : "TEXT");
                col_handles.push_back(columns.insert(&row));
            }
            DbRelation& table = SQLExec::tables->get_table(table_name);
            if (statement->ifNotExists) {
                table.create_if_not_exists();
            } else {
                table.create();
            }
        } catch (exception& e) {
            try {
                for (unsigned int i = 0; i < col_handles.size(); i++) {
                    columns.del(col_handles.at(i));
                }
            } catch (...) {
            }
            throw;
        }
    } catch (exception& e) {
        try {
            SQLExec::tables->del(table_handle);
        } catch (...) {
        }
        throw;
    }
    return new QueryResult("Created " + table_name);
}

// DROP ...
QueryResult *SQLExec::drop(const DropStatement *statement) {
    if (statement->type != DropStatement::kTable) {
        return new QueryResult("Unrecognized DROP type");
    }

    Identifier table_name = statement->name;

    if (table_name == Tables::TABLE_NAME || table_name == Columns::TABLE_NAME) {
        throw SQLExecError("Cannot drop a schema table!");
    }

    DbRelation& drop_table = SQLExec::tables->get_table(table_name);

    ValueDict where;
    where["table_name"] = Value(table_name);
    DbRelation& column = SQLExec::tables->get_table(Columns::TABLE_NAME);
    Handles* col_handle = column.select(&where);
    for (unsigned int i = 0; i < col_handle->size(); i++) {
        column.del(col_handle->at(i));
    }

    delete col_handle;
    drop_table.drop();

    SQLExec::tables->del(*SQLExec::tables->select(&where)->begin());
    return new QueryResult("dropped " + table_name);
}

QueryResult *SQLExec::show(const ShowStatement *statement) {
    return new QueryResult("not implemented"); // FIXME
}

QueryResult *SQLExec::show_tables() {
    return new QueryResult("not implemented"); // FIXME
}

QueryResult *SQLExec::show_columns(const ShowStatement *statement) {
    return new QueryResult("not implemented"); // FIXME
}
