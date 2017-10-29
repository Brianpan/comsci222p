#include <iostream>
#include <string>
#include <cassert>
#include <sys/stat.h>
#include <stdlib.h> 
#include <string.h>
#include <stdexcept>
#include <stdio.h> 

#include "pfm.h"
#include "rbfm.h"
#include "test_util.h"

using namespace std;

int RBFTest_scan(RecordBasedFileManager *rbfm) {
    // Functions tested
    // 1. Create Record-Based File
    // 2. Open Record-Based File
    // 3. Insert Record - NULL
    // 4. Read Record
    // 5. Close Record-Based File
    // 6. Destroy Record-Based File
    cout << endl << "***** In RBF Test Case scan *****" << endl;
   
    RC rc;
    string fileName = "test_scan";

    // Create a file named "test8b"
    rc = rbfm->createFile(fileName);
    assert(rc == success && "Creating the file should not fail.");

    rc = createFileShouldSucceed(fileName);
    assert(rc == success && "Creating the file failed.");

    // Open the file "test8b"
    FileHandle fileHandle;
    rc = rbfm->openFile(fileName, fileHandle);
    assert(rc == success && "Opening the file should not fail.");
   
    RID rid; 
    int recordSize = 0;
    void *record = malloc(100);
    void *returnedData = malloc(100);

    vector<Attribute> recordDescriptor;
    createRecordDescriptor(recordDescriptor);
    
    // NULL field indicator
    int nullFieldsIndicatorActualSize = getActualByteForNullsIndicator(recordDescriptor.size());
    unsigned char *nullsIndicator = (unsigned char *) malloc(nullFieldsIndicatorActualSize);
    memset(nullsIndicator, 0, nullFieldsIndicatorActualSize);

    // Setting the age & salary fields value as null
    nullsIndicator[0] = 80; // 01010000
    
    // Insert a record into a file
    prepareRecord(recordDescriptor.size(), nullsIndicator, 8, "Anteater", NULL, 177.8, NULL, record, &recordSize);
    cout << endl << "Inserting Data:" << endl;
    rbfm->printRecord(recordDescriptor, record);
    
    rc = rbfm->insertRecord(fileHandle, recordDescriptor, record, rid);
    assert(rc == success && "Inserting a record should not fail.");
    
    // insert second
    void *record2 = malloc(100);
    memset(nullsIndicator, 0, nullFieldsIndicatorActualSize);
    prepareRecord(recordDescriptor.size(), nullsIndicator, 9, "Anteater2", 25, 175.2, 6200, record2, &recordSize);
    cout << endl << "Inserting Data:" << endl;
    rc = rbfm->insertRecord(fileHandle, recordDescriptor, record2, rid);
    assert(rc == success && "Inserting a record should not fail.");
    rbfm->printRecord(recordDescriptor, record2);

    prepareRecord(recordDescriptor.size(), nullsIndicator, 9, "Anteater7", 25, 175.2, 6200, record2, &recordSize);
    cout << endl << "Inserting Data:" << endl;
    rc = rbfm->insertRecord(fileHandle, recordDescriptor, record2, rid);
    assert(rc == success && "Inserting a record should not fail.");
    rbfm->printRecord(recordDescriptor, record2);
    cout<<endl;


    // rbfm scan
    string conditionalAttribute = "EmpName";
    void *compVal = malloc(sizeof(12));
    string a ="Anteater2";
    int l = 9;
    memcpy(compVal, &l, 4);
    memcpy( (char*)compVal + 4, a.c_str(), 9 );
    vector<string> attributeNames;
    attributeNames.push_back("EmpName");
    attributeNames.push_back("Age");
    attributeNames.push_back("Height");
    attributeNames.push_back("Salary");
    RBFM_ScanIterator rbfm_ScanIterator;
    RC rf = rbfm->scan(fileHandle, recordDescriptor, conditionalAttribute, EQ_OP, compVal, attributeNames, rbfm_ScanIterator);

    rc = -1;
    while( rbfm_ScanIterator.getNextRecord(rid, returnedData) != -1 )
    {
        rbfm->printRecord( recordDescriptor, returnedData );
        rc = 0;
    }
    assert(rc==success && "getNextRecord should not fail.");

    rbfm_ScanIterator.close();
    
    cout << endl;

    // Close the file "test_scan"
    rc = rbfm->closeFile(fileHandle);
    assert(rc == success && "Closing the file should not fail.");

    // Destroy File
    rc = rbfm->destroyFile(fileName);
    assert(rc == success && "Destroying the file should not fail.");
    
    rc = destroyFileShouldSucceed(fileName);
    assert(rc == success  && "Destroying the file should not fail.");

    free(compVal);
    free(record);
    free(returnedData);
    
    cout << "RBF Test Case scan Finished! The result will be examined." << endl << endl;
    
    return 0;
}

int main()
{
    // To test the functionality of the paged file manager
    // PagedFileManager *pfm = PagedFileManager::instance();
    
    // To test the functionality of the record-based file manager 
    RecordBasedFileManager *rbfm = RecordBasedFileManager::instance(); 
     
    remove("test_scan");
       
    RC rcmain = RBFTest_scan(rbfm);
    return rcmain;
}
