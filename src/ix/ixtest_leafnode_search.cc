#include <iostream>

#include <cstdlib>
#include <cstdio>
#include <cstring>

#include "ix.h"
#include "ix_test_util.h"

IndexManager *indexManager;

RC testCase_Leafnode_Search(const string &indexFileName)
{
    // Functions tested

    cerr << endl << "***** In IX Test Case leaf node search *****" << endl;

    // create index file
    RC rc = indexManager->createFile(indexFileName);
    assert(rc == success && "indexManager::createFile() should not fail.");

    // open index file
    IXFileHandle ixfileHandle;
    rc = indexManager->openFile(indexFileName, ixfileHandle);
    assert(rc == success && "indexManager::openFile() should not fail.");


    void *tmpPage = malloc(PAGE_SIZE);
    int nodeValueStart = 11; 
    RecordMinLen slotCount = 5;
    
    LEAFNODE<int> leafNodes[slotCount];

    for(int idx = 0; idx <= slotCount; idx++)
    {
        leafNodes[idx].key = nodeValueStart + idx*3;
        leafNodes[idx].rid.pageNum = 0;
        leafNodes[idx].rid.slotNum = nodeValueStart + idx;
    }

    memcpy(tmpPage, leafNodes, slotCount*sizeof(LEAFNODE<int>));
    
    int k = 10;

    IDX_PAGE_POINTER_TYPE r0 = indexManager->searchFixedLeafNode<int>(k, tmpPage, slotCount);

    IDX_PAGE_POINTER_TYPE a0 = 0;
    assert( a0 == r0 && "Search 0 failed !" );
    cout<<"search 0 finish !"<<endl;

    k = 27;
    r0 = indexManager->searchFixedLeafNode<int>(k, tmpPage, slotCount);
    a0 = 5;
    assert( a0 == r0 && "Search 1 failed !" );
    cout<<"search 1 finish !"<<endl;
    
    k = 11;
    r0 = indexManager->searchFixedLeafNode<int>(k, tmpPage, slotCount);
    a0 = 1;
    assert( a0 == r0 && "Search 2 failed !" );

    k = 17;
    r0 = indexManager->searchFixedLeafNode<int>(k, tmpPage, slotCount);
    a0 = 3;
    assert( a0 == r0 && "Search 3 failed !" );

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

    RC rcmain = testCase_Leafnode_Search(indexFileName);
    if (rcmain == success) {
        cerr << "***** IX Test Case leaf node search finished. The result will be examined. *****" << endl;
        return success;
    } else {
        cerr << "***** [FAIL] IX Test Case leaf node search failed. *****" << endl;
        return fail;
    }
}

