#include "db_cxx.h"
#include <cstring>
#include <map>
#include <algorithm>
#include <iostream>
#include "storage_engine.h"
#include "heap_storage.h"
#include <cstdint>

using namespace std;
typedef uint16_t u16;

int DB_BLOCK_SIZE = 4096;

/*
// SlottedPage(Dbt &block, BlockID block_id, bool is_new = false);
// Big 5 - we only need the destructor, copy-ctor, move-ctor, and op= are unnecessary
// but we delete them explicitly just to make sure we don't use them accidentally

// ~SlottedPage() {}

// SlottedPage(const SlottedPage &other) = delete;

// SlottedPage(SlottedPage &&temp) = delete;

// SlottedPage &operator=(const SlottedPage &other) = delete;

// SlottedPage &operator=(SlottedPage &temp) = delete;

RecordID add(const Dbt *data) {
    if (!has_room(data->get_size()))
        throw DbBlockNoRoomError("not enough room for new record");
    u16 id = ++this->num_records;
    u16 size = (u16) data->get_size();
    this->end_free -= size;
    u16 loc = this->end_free + 1;
    put_header();
    put_header(id, size, loc);
    memcpy(this->address(loc), data->get_data(), size);
    return id;
}

// Get 2-byte integer at given offset in block.
u16 get_n(u16 offset) {
    return *(u16*)this->address(offset);
}

// Put a 2-byte integer at given offset in block.
void put_n(u16 offset, u16 n) {
    *(u16*)this->address(offset) = n;
}

// Make a void* pointer for a given offset into the data block.
void *address(u16 offset) {
    return (void*)((char*)this->block.get_data() + offset);
}

// Store the size and offset for given id. For id of zero, store the block header.
void put_header(RecordID id, u16 size, u16 loc) {
    if (id == 0) { // called the put_header() version and using the default params
        size = this->num_records;
        loc = this->end_free;
    }
    put_n(4*id, size);
    put_n(4*id + 2, loc);
}
Dbt *get(RecordID record_id);

void put(RecordID record_id, const Dbt &data);

void del(RecordID record_id);

RecordIDs *ids(void);

virtual void get_header(u_int16_t &size, u_int16_t &loc, RecordID id = 0);

virtual bool has_room(u_int16_t size);

virtual void slide(u_int16_t start, u_int16_t end);
*/

/**
 * @class HeapFile - heap file implementation of DbFile
 *
 * Heap file organization. Built on top of Berkeley DB RecNo file. There is one of our
        database blocks for each Berkeley DB record in the RecNo file. In this way we are using Berkeley DB
        for buffer management and file management.
        Uses SlottedPage for storing records within blocks.
 */
void HeapFile::create(void) {
    this->db_open(DB_CREATE | DB_EXCL);
    SlottedPage *blockPage = get_new();
    delete blockPage;
}

void HeapFile::drop(void) {
    this->close();
    Db db(_DB_ENV, 0);
    db.remove(this->dbfilename.c_str(), nullptr, 0);
}

void HeapFile::open(void) {
    this->db_open();
}

void HeapFile::close(void) {
    this->db.close(0);
    this->closed = true;
}

SlottedPage *HeapFile::get_new(void) {
    char block[DbBlock::BLOCK_SZ];
    memset(block, 0, sizeof(block));
    Dbt data(block, sizeof(block));

    int block_id = ++this->last;
    Dbt key(&block_id, sizeof(block_id));

    // write out an empty block and read it back in so Berkeley DB is managing the memory
    SlottedPage *page = new SlottedPage(data, this->last, true);
    this->db.put(nullptr, &key, &data, 0); // write it out with initialization applied
    this->db.get(nullptr, &key, &data, 0);
    return page;
}

SlottedPage *HeapFile::get(BlockID block_id) {
    Dbt key(&block_id, sizeof(block_id));
    Dbt data;
    this->db.get(nullptr, &key, &data, 0);
    return new SlottedPage(data, block_id, false);
}

void HeapFile::put(DbBlock *block) {
    int block_id = block->get_block_id();
    Dbt key(&block_id, sizeof(block_id));
    this->db.put(nullptr, &key, block->get_block(), 0);
}

BlockIDs* HeapFile::block_ids(){
    vector<BlockID>* blockIds;
    for(BlockID i = 1; i < (BlockID)this->last + 1; i++){
        blockIds->push_back(i);
    }
    return blockIds;
}

void HeapFile::db_open(uint flags) {
	if (!this->closed) 
		return;

	this->db.set_re_len(DbBlock::BLOCK_SZ);
	this->dbfilename = this->name + ".db";
	this->db.open(nullptr, (this->dbfilename).c_str(), nullptr, DB_RECNO, flags, 0644);
	DB_BTREE_STAT *stat;
	this->db.stat(nullptr, &stat, DB_FAST_STAT);
	this->last = flags ? 0 : stat->bt_ndata;
	this->closed = false;
}

/**
 * @class HeapTable - Heap storage engine (implementation of DbRelation)
 */
HeapTable::HeapTable(Identifier table_name, ColumnNames column_names, ColumnAttributes column_attributes) : DbRelation(table_name, column_names, column_attributes) {
    this->file = new HeapFile(table_name);
}

void HeapTable::create(void) {
    this->file.create();

}

void HeapTable::create_if_not_exists(void) {
    try {
        this->open();
    } catch (DbRelationError e) {
        this->create();
    }
}

void HeapTable::drop(void) {
    this->file.drop();
}

void HeapTable::open(void) {
    this->file.open();
}

void HeapTable::close(void) {
    this->file.close();
}

Handle HeapTable::insert(const ValueDict *row) {
    this->open();
    return this->append(this->validate(row));
}

//void update(const Handle handle, const ValueDict *new_values);

// void del(const Handle handle);

Handles* HeapTable::select(void) {
    Handles* handles = new Handles();
    BlockIDs* block_ids = file.block_ids();
    for (auto const& block_id: *block_ids) {
        SlottedPage* block = file.get(block_id);
        RecordIDs* record_ids = block->ids();
        for (auto const& record_id: *record_ids)
            handles->push_back(Handle(block_id, record_id));
        delete record_ids;
        delete block;
    }
    delete block_ids;
    return handles;
}

Handles* HeapTable::select(const ValueDict *where) {
    Handles* handles = new Handles();
    BlockIDs* block_ids = file.block_ids();
    for (auto const& block_id: *block_ids) {
        SlottedPage* block = file.get(block_id);
        RecordIDs* record_ids = block->ids();
        for (auto const& record_id: *record_ids)
            handles->push_back(Handle(block_id, record_id));
        delete record_ids;
        delete block;
    }
    delete block_ids;
    return handles;
}

// ValueDict *project(Handle handle);

// ValueDict *project(Handle handle, const ColumnNames *column_names);

ValueDict* HeapTable::validate(const ValueDict *row) {
    ValueDict tempRow = *row;
    ValueDict* fullRow = new ValueDict();
    Value v;
    for (Identifier column_name : this->column_names) {
        if (tempRow.find(column_name) == tempRow.end()) {
            throw new DbRelationError("DbRelationError");
        } else {
            v = tempRow[column_name];
        }
        fullRow->insert(pair<Identifier , Value>(column_name, v));
    }
    return fullRow;
}

Handle HeapTable::append(const ValueDict *row) {
    Dbt *data = this->marshal(row);
    SlottedPage * block = this->file.get(this->file.get_last_block_id());
    u_int16_t record_id;
    try {
        record_id = block->add(data);
    } catch (DbRelationError) {
    } catch (DbRelationError const&) {
        block = this->file.get_new();
        record_id = block->add(data);
    }
    this->file.put(block);
    unsigned int id = this->file.get_last_block_id();
    std::pair<BlockID, RecordID> Handle (id, record_id);
    return Handle;
}

Dbt* HeapTable::marshal(const ValueDict *row) {
    char *bytes = new char[DbBlock::BLOCK_SZ]; // more than we need (we insist that one row fits into DbBlock::BLOCK_SZ)
    uint offset = 0;
    uint col_num = 0;
    for (auto const& column_name: this->column_names) {
        ColumnAttribute ca = this->column_attributes[col_num++];
        ValueDict::const_iterator column = row->find(column_name);
        Value value = column->second;
        if (ca.get_data_type() == ColumnAttribute::DataType::INT) {
            *(int32_t*) (bytes + offset) = value.n;
            offset += sizeof(int32_t);
        } else if (ca.get_data_type() == ColumnAttribute::DataType::TEXT) {
            uint size = value.s.length();
            *(u16*) (bytes + offset) = size;
            offset += sizeof(u16);
            memcpy(bytes+offset, value.s.c_str(), size); // assume ascii for now
            offset += size;
        } else {
            throw DbRelationError("Only know how to marshal INT and TEXT");
        }
    }
    char *right_size_bytes = new char[offset];
    memcpy(right_size_bytes, bytes, offset);
    delete[] bytes;
    Dbt *data = new Dbt(right_size_bytes, offset);
    return data;

}

//ValueDict *unmarshal(Dbt *data);

bool test_heap_storage();
