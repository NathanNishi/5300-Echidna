/**
 * @file heap_storage.cpp - Implementation of Heap_storage as per milestone2.
 * @author Priyanka Patil and Benjamin Gruher
 */
#include <vector>
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

/*
SlottedPage class implementation
*/

const u16 DELETED_RECORD_SIZE = -1;

SlottedPage::SlottedPage(Dbt &block, BlockID block_id, bool is_new) : DbBlock(block, block_id, is_new) {
    if (is_new) {
        this->num_records = 0;
        this->end_free = DbBlock::BLOCK_SZ - 1;
        put_header();
    } else {
        get_header(this->num_records, this->end_free);
    }
}

RecordID SlottedPage::add(const Dbt *data) {
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

Dbt *SlottedPage::get(RecordID record_id) {
    u16 recordSize, recordLocation;
    get_header(recordSize, recordLocation, record_id);

    if (recordSize == DELETED_RECORD_SIZE) {        // this is just a tombstone, record has been deleted
        return nullptr;
    }
    return new Dbt(this->address(recordLocation), recordSize);
}

void SlottedPage::put(RecordID record_id, const Dbt &data) {
    u16 oldSize;
    u16 oldLoc;
    get_header(oldSize, oldLoc, record_id); // gets the old size and location
    u16 newSize = data.get_size();
    if (newSize > oldSize) {                // indicates an increase in record size
        u16 sizeIncrease = data.get_size() - oldSize;
        if (!has_room(sizeIncrease - 4))    // checks if the block has room for the increase in size (minus the 4 byte header that already exists)
            throw DbBlockNoRoomError("not enough room for updated record");
        slide(oldLoc, oldLoc - sizeIncrease);
        memcpy(this->address(oldLoc - sizeIncrease), data.get_data(), newSize);
    }
    else {
        u16 sizeDecrease = oldSize - data.get_size();
        memcpy(this->address(oldLoc + sizeDecrease), data.get_data(), newSize);
        slide(oldLoc, oldLoc + sizeDecrease);
    }
    u16 size, newLoc;
    get_header(size, newLoc, record_id);
    put_header(record_id, newSize, newLoc);
}

void SlottedPage::del(RecordID record_id) {
    // set header size to -1 and set pointer to the same as the previous record
    u16 recordSize, recordLoc;
    get_header(recordSize, recordLoc, record_id);
    put_header(record_id, DELETED_RECORD_SIZE, recordLoc+recordSize);

    // remove record by sliding over previous records
    slide(recordLoc, recordLoc + recordSize);
}

RecordIDs *SlottedPage::ids(void) {
    // iterate through block and get all block ID's that don't have size = -1
    RecordIDs* idList = new RecordIDs();
    for (unsigned short i = 1; i <= this->num_records; i++) {
        u16 recordSize = get_n(i * 4);
        if (recordSize > 0) idList->push_back(i);
    }
    return idList;
} 

void SlottedPage::get_header(u_int16_t &size, u_int16_t &loc, RecordID id) {
    // sets the references to size and loc
    size = get_n(4 * id);
    loc = get_n((4 * id) + 2);
}

void SlottedPage::put_header(RecordID id, u_int16_t size, u_int16_t loc) {
    if (id == 0) { // called the put_header() version and using the default params
        size = this->num_records;
        loc = this->end_free;
    }
    put_n(4*id, size);
    put_n(4*id + 2, loc);
}

bool SlottedPage::has_room(u_int16_t size) {
    if (size > this->end_free) {                                // since these integers are unsigned, they can't handle negative values. If size > this_end free, there is no room in the block (even without the header)
        return false;
    }
    u16 currentHeaderLocation = 4 + (this->num_records * 4);    // current end of header section (each header entry = 4 bytes and beginning entry of 4 bytes)
    u16 newHeaderLocation = currentHeaderLocation + 4;          // new end of header section (with new header of 4 bytes)
    u16 newEndOfFreeSpace = this->end_free + 1 - size;          // where the new end of free space would be if record is added
    return newEndOfFreeSpace >= newHeaderLocation;              // checks to see if new record would collide with header
}

void SlottedPage::slide(u_int16_t start, u_int16_t end) {
    int shift = end - start;
    if (shift == 0) return;
    // write the data
    void* startCopyLocation = this->address(this->end_free + shift);
    void* dataStartLocation = this->address(this->end_free); 
    int size = start - this->end_free;
    memcpy(startCopyLocation, dataStartLocation, size); 

    // fix headers
    RecordIDs ids = *this->ids();
    for (int i = 0; i < ids.size(); i++) {
        u16 size, loc;
        get_header(size, loc, ids[i]);
        if (loc <= start) {
            loc += shift;
            put_header(ids[i], size, loc);
        }
    }
    this->end_free += shift;
    put_header();
}

u_int16_t SlottedPage::get_n(u_int16_t offset) {
    return *(u16*)this->address(offset);
}

void SlottedPage::put_n(u_int16_t offset, u_int16_t n) {
    *(u16*)this->address(offset) = n;
}

void *SlottedPage::address(u_int16_t offset) {
    return (void*)((char*)this->block.get_data() + offset);
}

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
    SlottedPage *blockPage = this->get_new();
    delete blockPage;
}

void HeapFile::drop(void) {
    this->close();
    this->closed = true;
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
HeapTable::HeapTable(Identifier table_name, ColumnNames column_names, ColumnAttributes column_attributes) : DbRelation(table_name, column_names, column_attributes), file(table_name) {}

void HeapTable::create(void) {
    this->file.create();
}

void HeapTable::create_if_not_exists(void) {
    try {
        this->open();
    } catch (DbRelationError const&) {
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

void HeapTable::update(const Handle handle, const ValueDict *new_values) {
    //Not in milestone2
}

void HeapTable::del(const Handle handle) {
    //Not in milestone2
}

ValueDict* HeapTable::project(Handle handle) {
    //Not in milestone2
}

ValueDict* HeapTable::project(Handle handle, const ColumnNames *column_names) {
    //Not in milestone2
}

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

ValueDict * HeapTable::unmarshal(Dbt *data){
    std::map<Identifier, Value> * row = {};
    char *bytes = new char[DbBlock::BLOCK_SZ];
    uint offset = 0;
    uint col_num = 0;
    for (auto const& column_name: this->column_names) {
        ColumnAttribute ca = this->column_attributes[col_num++];
        ValueDict::const_iterator column = row->find(column_name);
        Value value = column->second;
        if (ca.get_data_type() == ColumnAttribute::DataType::INT) {
            value.n = *(int32_t*) (bytes + offset);
            offset += sizeof(int32_t);
        } else if (ca.get_data_type() == ColumnAttribute::DataType::TEXT) {

        } else {
            throw DbRelationError("Only know how to marshal INT and TEXT");
        }
        (*row)[column_name] = value;
    }
    delete[] bytes;
    return row;
}

bool test_heap_storage() {
    ColumnNames column_names;
    column_names.push_back("a");
    column_names.push_back("b");
    ColumnAttributes column_attributes;
    ColumnAttribute ca(ColumnAttribute::INT);
    column_attributes.push_back(ca);
    ca.set_data_type(ColumnAttribute::TEXT);
    column_attributes.push_back(ca);
    HeapTable table1("_test_create_drop_cpp", column_names, column_attributes);
    table1.create();
    std::cout << "create ok" << std::endl;
    table1.drop();  // drop makes the object unusable because of BerkeleyDB restriction -- maybe want to fix this some day
    std::cout << "drop ok" << std::endl;

    HeapTable table("_test_data_cpp", column_names, column_attributes);
    table.create_if_not_exists();
    std::cout << "create_if_not_exists ok" << std::endl;

    ValueDict row;
    row["a"] = Value(12);
    row["b"] = Value("Hello!");
    std::cout << "try insert" << std::endl;
    table.insert(&row);
    std::cout << "insert ok" << std::endl;
    Handles* handles = table.select();
    std::cout << "select ok " << handles->size() << std::endl;
    ValueDict *result = table.project((*handles)[0]);
    std::cout << "project ok" << std::endl;
    Value value = (*result)["a"];
    if (value.n != 12)
        return false;
    value = (*result)["b"];
    if (value.s != "Hello!")
        return false;
    table.drop();

    return true;
}
