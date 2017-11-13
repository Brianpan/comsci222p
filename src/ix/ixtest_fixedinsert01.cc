#include <iostream>

#include <cstdlib>
#include <cstdio>
#include <cstring>

#include "ix.h"
#include "ix_test_util.h"

IndexManager *indexManager;

RC testCase_Fixed_Insert01(const string &indexFileName)
{
    // Functions tested

    cerr << endl << "***** In IX Test Case fixed insert leaf node *****" << endl;

    // create index file
    RC rc = indexManager->createFile(indexFileName);
    assert(rc == success && "indexManager::createFile() should not fail.");

    // open index file
    IXFileHandle ixfileHandle;
    rc = indexManager->openFile(indexFileName, ixfileHandle);
    assert(rc == success && "indexManager::openFile() should not fail.");


    void *tmpPage = malloc(PAGE_SIZE);
    int nodeValueStart = 11; 
    RecordMinLen slotCount = 6;
    
    // prepare leafnodes
    LEAFNODE<int> leafNodes[slotCount];

    for(int idx = 0; idx <= slotCount; idx++)
    {
        leafNodes[idx].key = nodeValueStart + idx*3;
        leafNodes[idx].rid.pageNum = 0;
        leafNodes[idx].rid.slotNum = nodeValueStart + idx;
    }

    memcpy(tmpPage, leafNodes, slotCount*sizeof(LEAFNODE<int>));
    RecordMinLen freeSize = 0;
    RecordMinLen nodeType = LEAF_NODE;
    RecordMinLen slotInfos[3];
    IDX_PAGE_POINTER_TYPE rightPointer = 1;

    slotInfos[2] = freeSize;
    slotInfos[1] = slotCount;
    slotInfos[0] = nodeType;
    memcpy( (char*)tmpPage+getNodeTypeOffset(), slotInfos, sizeof(RecordMinLen)*3  );
    memcpy( (char*)tmpPage+getLeafNodeRightPointerOffset(), &rightPointer, sizeof(IDX_PAGE_POINTER_TYPE) );
    ixfileHandle.appendPage(tmpPage);
    ixfileHandle.treeHeight = 1;
    ixfileHandle.rootPageId = 0;

    // start insert
    void *newPage = malloc(PAGE_SIZE);
    int curPageId = ixfileHandle.getNumberOfPages() - 1;
    int newPageId;

    int k = 21;
    RID tmpRid;
    RC flag;
    tmpRid.pageNum = 1;
    tmpRid.slotNum = 0;
    flag = indexManager->insertFixedLengthEntry<int>( ixfileHandle, (void*)&k, tmpRid );
    assert(flag == success && "insert failed !");

    cout<<ixfileHandle.getNumberOfPages()<<endl;

    // print root node
    RecordMinLen rootSlotCount;
    ixfileHandle.readPage( 2, tmpPage );
    memcpy( &rootSlotCount, (char*)tmpPage+getIndexSlotCountOffset(), sizeof(RecordMinLen) );
    assert( rootSlotCount == 1 && "root Node failed !" );
    
    int rootValue;
    memcpy( &rootValue, (char*)tmpPage+getFixedKeyOffset(0), sizeof(int) );
    cout<<"rootValue:"<<rootValue<<endl;
    assert( rootValue == 20 && "root Node Value wrong !");


    // print two nodes
    ixfileHandle.readPage(0, tmpPage);
    cout<<"Left page : "<<endl;
    RecordMinLen curSlotCount;
    memcpy(&curSlotCount, (char*)tmpPage+getIndexSlotCountOffset(), sizeof(RecordMinLen) );
    LEAFNODE<int> curLeafNodes[curSlotCount];
    memcpy( curLeafNodes, tmpPage, sizeof(LEAFNODE<int>)*curSlotCount );
    for(int i; i<curSlotCount;i++)
    {
    	cout<<"Node: "<<curLeafNodes[i].key<<endl;
    }

    ixfileHandle.readPage(1, newPage);
    cout<<"Right page : "<<endl;
    RecordMinLen newSlotCount;
    memcpy(&newSlotCount, (char*)newPage+getIndexSlotCountOffset(), sizeof(RecordMinLen) );
    LEAFNODE<int> newLeafNodes[newSlotCount];
    memcpy( newLeafNodes, newPage, sizeof(LEAFNODE<int>)*newSlotCount );
    for(int i; i<newSlotCount;i++)
    {
    	cout<<"Node: "<<newLeafNodes[i].key<<endl;
    }

    // close index file
    rc = indexManager->closeFile(ixfileHandle);
    assert(rc == success && "indexManager::closeFile() should not fail.");

    free(tmpPage);
    free(newPage);
    return success;
}

int main()
{
    // Global Initialization
    indexManager = IndexManager::instance();

    const string indexFileName = "age_idx";
    remove("age_idx");

    RC rcmain = testCase_Fixed_Insert01(indexFileName);
    if (rcmain == success) {
        cerr << "***** IX Test Case fixed insert finished. The result will be examined. *****" << endl;
        return success;
    } else {
        cerr << "***** [FAIL] IX Test Case fixed insert failed. *****" << endl;
        return fail;
    }
}

