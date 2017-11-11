#include <iostream>

#include <cstdlib>
#include <cstdio>
#include <cstring>

#include "ix.h"
#include "ix_test_util.h"

IndexManager *indexManager;

RC testCase_Search(const string &indexFileName)
{
    // Functions tested
    // 1. Create Index File **
    // 2. Open Index File **
    // 3. Create Index File -- when index file is already created **
    // 4. Open Index File ** -- when a file handle is already opened **
    // 5. Close Index File **
    // NOTE: "**" signifies the new functions being tested in this test case.
    cerr << endl << "***** In IX Test Case search *****" << endl;

    // create index file
    RC rc = indexManager->createFile(indexFileName);
    assert(rc == success && "indexManager::createFile() should not fail.");

    // open index file
    IXFileHandle ixfileHandle;
    rc = indexManager->openFile(indexFileName, ixfileHandle);
    assert(rc == success && "indexManager::openFile() should not fail.");


    void *tmpPage = malloc(PAGE_SIZE);
    int intermediateNodes = 6;
    int nodeValueStart = 11; 

    IDX_PAGE_POINTER_TYPE p0;
    for(int idx = 0; idx <= intermediateNodes; idx++)
    {
        p0 = nodeValueStart + idx*2;
        memcpy( (char*)tmpPage+getFixedKeyPointerOffset(idx), &p0, sizeof(IDX_PAGE_POINTER_TYPE) );
    }

    int nodeValue = nodeValueStart;
    for(int idx=0;idx<intermediateNodes;idx++)
    {
        nodeValue = nodeValueStart + idx*3;
        cout<<"Node Value"<<nodeValue<<endl;
        memcpy( (char*)tmpPage+getFixedKeyOffset(idx), &nodeValue, sizeof(int) );
    }
    int k = 27;

    INDEXPOINTER r0 = indexManager->searchFixedIntermediatePage<int>(k, tmpPage, 0, intermediateNodes);

    INDEXPOINTER a0;
    a0.pageNum = 23;
    a0.left = 0;
    a0.indexId = 6;
    assert( (a0.pageNum == r0.pageNum) && (a0.left == r0.left) && (a0.indexId == r0.indexId) && "Search 0 failed !" );
    
    k = 9;
    r0 = indexManager->searchFixedIntermediatePage<int>(k, tmpPage, 0, intermediateNodes);
    a0.pageNum = 11;
    a0.left = 1;
    a0.indexId = 0;
    assert( (a0.pageNum == r0.pageNum) && (a0.left == r0.left) && (a0.indexId == r0.indexId) && "Search 0 failed !" );

    k = 14;
    r0 = indexManager->searchFixedIntermediatePage<int>(k, tmpPage, 0, intermediateNodes);
    a0.pageNum = 15;
    a0.left = 0;
    a0.indexId = 2;
    assert( (a0.pageNum == r0.pageNum) && (a0.left == r0.left) && (a0.indexId == r0.indexId) && "Search 0 failed !" );

    // close index file
    rc = indexManager->closeFile(ixfileHandle);
    assert(rc == success && "indexManager::closeFile() should not fail.");

    free(tmpPage);
    return success;
}

int main()
{
    // Global Initialization
    indexManager = IndexManager::instance();

    const string indexFileName = "age_idx";
    remove("age_idx");

    RC rcmain = testCase_Search(indexFileName);
    if (rcmain == success) {
        cerr << "***** IX Test Case search finished. The result will be examined. *****" << endl;
        return success;
    } else {
        cerr << "***** [FAIL] IX Test Case search failed. *****" << endl;
        return fail;
    }
}

