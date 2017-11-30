#include "rm_test_util.h"

RC IDX_TEST(const string &tableName, const int nameLength, const string &name, const int age, const float height, const int salary)
{
    // Functions tested
    // 1. create Index
    // 2. Insert Tuple **
    // 3. Read Tuple **
    // 4. delete Index

    // NOTE: "**" signifies the new functions being tested in this test case.
    cout << endl << "***** In RM IDX Test Case *****" << endl;

    RID rid;
    int tupleSize = 0;
    void *tuple = malloc(200);
    void *returnedData = malloc(200);

    vector<Attribute> attrs;
    RC rc = rm->getAttributes(tableName, attrs);
    assert(rc == success && "RelationManager::getAttributes() should not fail.");

    // create index for EmpName
    rc = rm->createIndex(tableName, attrs[0].name);
    assert(rc == success && "RelationManager::createIndex() should not fail." );

    // create index for Age
    rc = rm->createIndex(tableName, attrs[1].name);
    assert(rc == success && "RelationManager::createIndex() should not fail." );

    // Initialize a NULL field indicator
    int nullAttributesIndicatorActualSize = getActualByteForNullsIndicator(attrs.size());
    unsigned char *nullsIndicator = (unsigned char *) malloc(nullAttributesIndicatorActualSize);
	memset(nullsIndicator, 0, nullAttributesIndicatorActualSize);

    // Insert a tuple into a table
    prepareTuple(attrs.size(), nullsIndicator, nameLength, name, age, height, salary, tuple, &tupleSize);
    cout << "The tuple to be inserted:" << endl;
    rc = rm->printTuple(attrs, tuple);
    cout << endl;

    rc = rm->insertTuple(tableName, tuple, rid);
    assert(rc == success && "RelationManager::insertTuple() should not fail.");

    // Given the rid, read the tuple from table
    rc = rm->readTuple(tableName, rid, returnedData);
    assert(rc == success && "RelationManager::readTuple() should not fail.");

    cout << "The returned tuple:" << endl;
    rc = rm->printTuple(attrs, returnedData);
    cout << endl;

    // start check index
    IndexManager *indexManagerPtr = IndexManager::instance();

    // check emp
    int keySize = nameLength + sizeof(int);
    void *key = malloc(keySize);
    memcpy( key, &nameLength, sizeof(int) );

    const char *empName = name.c_str();
    memcpy( (char*)key+sizeof(int), empName, nameLength );

    IX_ScanIterator ix_ScanIterator;
    IXFileHandle ixfileHandle;
    string indexFName = indexFileName(tableName, attrs[0].name);
    indexManagerPtr->openFile(indexFName, ixfileHandle);

    rc = indexManagerPtr->scan(ixfileHandle, attrs[0], key, key, true, true, ix_ScanIterator);
    assert(rc == success && "IndexManager::scan() should not fail.");

    void *data = malloc(PAGE_SIZE);
    RID idxRid;
    idxRid.pageNum = 1;
    int count = 0;
    while( ix_ScanIterator.getNextEntry(idxRid, data) == 0)
    {
        assert(rid.pageNum ==idxRid.pageNum && rid.slotNum == idxRid.slotNum && "index should exist in indexFile");
        count +=1;
    }
    assert(count == 1&& "indexScanfail");


    // delete record
    rm->deleteTuple(tableName,rid);
    ix_ScanIterator.close();
    count = 0;
    while( ix_ScanIterator.getNextEntry(idxRid, data) == 0)
    {
        count +=1;
    }
    assert(count == 0 && "indexScanfail");

    indexManagerPtr->closeFile(ixfileHandle);
    free(data);
    free(key);

    // delete table
    const string idxTable = "indexTable";
    rm->deleteTable(tableName);
    rm->printTable(idxTable);

    // Compare whether the two memory blocks are the same
    if(memcmp(tuple, returnedData, tupleSize) == 0)
    {
        cout << "**** RM IDX Test Case finished. The result will be examined. *****" << endl << endl;
        free(tuple);
        free(returnedData);
        free(nullsIndicator);
        return success;
    }
    else
    {
        cout << "**** [FAIL] RM IDX Test Case failed *****" << endl << endl;
        free(tuple);
        free(returnedData);
        free(nullsIndicator);
        return -1;
    }

}

int main()
{
    // Insert/Read Tuple
    RC rcmain = IDX_TEST("tbl_employee", 14, "Peter Anteater", 27, 6.2, 10000);

    return rcmain;
}
