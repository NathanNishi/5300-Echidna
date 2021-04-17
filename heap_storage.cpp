#include "heap_storage.h"
#include <cstring>
#include <vector>

typedef u_int16_t u16;
using namespace std;
/*
SlottedPage class implementation
*/

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
    // check header for location and size of record
    u16 recordSizeLoc = record_id * 4;
    u16 recordLocationLoc = recordSizeLoc + 2;
    u16 size = get_n(recordSizeLoc);
    u16 loc = get_n(recordLocationLoc);
    
    Dbt* retDbt = new Dbt(this->address(loc), size);
    return retDbt;

    /*
    Maybe a more efficient way to do it:
    u16 size;
    u16 loc;
    get_header(size, loc, record_id);
    return Dbt(loc, size);
    */    
}

void SlottedPage::put(RecordID record_id, const Dbt &data) {
    // FIXME - TODO: implement
    // 1. check that updated size (if it is an increase) still fits
    u16 oldSize;
    u16 oldLoc;
    get_header(oldSize, oldLoc, record_id); // gets the old size and location
    if (data.get_size() > oldSize) {       // indicates an increase in record size
        u16 sizeIncrease = data.get_size() - oldSize;
        if (!has_room(sizeIncrease - 4))    // checks if the block has room for the increase in size (minus the 4 byte header that already exists)
            throw DbBlockNoRoomError("not enough room for updated record");
    }
    // 2. update record (slide)
    // 3. update header

}

void SlottedPage::del(RecordID record_id) {
    // FIXME - TODO: implement
    // set header size to -1 and set pointer to the same as the previous record
    // remove the record from the block (slide others?)
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
    u16 currentHeaderLocation = 4 + (this->num_records * 4);    // current end of header section (each header entry = 4 bytes and beginning entry of 4 bytes)
    u16 newHeaderLocation = currentHeaderLocation + 4;          // new end of header section (with new header of 4 bytes)
    u16 newEndOfFreeSpace = this->end_free - size;
    return newEndOfFreeSpace >= newHeaderLocation;              // FIXME - maybe just a > ? 
}

void SlottedPage::slide(u_int16_t start, u_int16_t end) {
    // FIXME - TODO: implement
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



/*
HeapFile class implementation
*/


/*
Testing implementation
*/
bool test_heap_storage() {

    // TESTING SLOTTED PAGE

    // Dbt for testing
    string data = "hello";
    Dbt block((void*)&data, sizeof(data));
    BlockID blockId = 1;

    // Create Slotted Page
    SlottedPage testSp(block, blockId, true); // FIXME!


    // Test has_room() function
    // cout << "Has room for 10 bytes? " << testSp.has_room(10) << endl;
    // cout << "Has room for 100 bytes? " << testSp.has_room(100) << endl;
    // cout << "Has room for 1000 bytes? " << testSp.has_room(1000) << endl;

    // Test add function
    string testData = "testingFunctions";
    Dbt block1((void*)&testData, sizeof(data));
    u16 recordId = testSp.add(&block1);

    // Test get function
    Dbt* retrieved = testSp.get(recordId); 
    cout << "Expect testingFunctions: " << *(string*)retrieved->get_data() << endl;
    delete retrieved;

    return true;
}

