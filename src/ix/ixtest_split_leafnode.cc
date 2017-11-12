#include <iostream>

#include <cstdlib>
#include <cstdio>
#include <cstring>

#include "ix.h"
#include "ix_test_util.h"

IndexManager *indexManager;

RC testCase_Split_Leafnode(const string &indexFileName)
{
    // Functions tested

    cerr << endl << "***** In IX Test Case insert leaf node *****" << endl;

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
    RecordMinLen freeSize = PAGE_SIZE - getLeafNodeDirSize() - slotCount*sizeof(LEAFNODE<int>);
    RecordMinLen nodeType = LEAF_NODE;
    RecordMinLen slotInfos[3];
    IDX_PAGE_POINTER_TYPE rightPointer = 1;

    slotInfos[2] = freeSize;
    slotInfos[1] = slotCount;
    slotInfos[0] = nodeType;
    memcpy( (char*)tmpPage+getNodeTypeOffset(), slotInfos, sizeof(RecordMinLen)*3  );
    memcpy( (char*)tmpPage+getLeafNodeRightPointerOffset(), &rightPointer, sizeof(IDX_PAGE_POINTER_TYPE) );
    ixfileHandle.appendPage(tmpPage);

    // start insert
    void *newPage = malloc(PAGE_SIZE);
    int curPageId = ixfileHandle.getNumberOfPages() - 1;
    int newPageId;

    int k = 21;
    int upwardKey;
    RID tmpRid;
    RC flag;
    tmpRid.pageNum = 1;
    tmpRid.slotNum = 0;
    flag = indexManager->splitFixedLeafNode<int>( ixfileHandle, curPageId, newPageId, k, tmpRid, upwardKey, tmpPage, newPage);
    assert(flag == success && "split failed !");

    // print two nodes
    cout<<"Cur page : "<<endl;
    RecordMinLen curSlotCount;
    memcpy(&curSlotCount, (char*)tmpPage+getIndexSlotCountOffset(), sizeof(RecordMinLen) );
    LEAFNODE<int> curLeafNodes[curSlotCount];
    memcpy( curLeafNodes, tmpPage, sizeof(LEAFNODE<int>)*curSlotCount );
    for(int i; i<curSlotCount;i++)
    {
    	cout<<"Node: "<<curLeafNodes[i].key<<endl;
    }

    cout<<"New page : "<<endl;
    RecordMinLen newSlotCount;
    memcpy(&newSlotCount, (char*)newPage+getIndexSlotCountOffset(), sizeof(RecordMinLen) );
    LEAFNODE<int> newLeafNodes[newSlotCount];
    memcpy( newLeafNodes, newPage, sizeof(LEAFNODE<int>)*newSlotCount );
    for(int i; i<newSlotCount;i++)
    {
    	cout<<"Node: "<<newLeafNodes[i].key<<endl;
    }

    cout<<"upwardKey:"<<upwardKey<<endl;
    cout<<"New page id:"<<newPageId<<endl;

    assert(upwardKey == 20 && "wrong upwardKey !");

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

    RC rcmain = testCase_Split_Leafnode(indexFileName);
    if (rcmain == success) {
        cerr << "***** IX Test Case insert leaf node finished. The result will be examined. *****" << endl;
        return success;
    } else {
        cerr << "***** [FAIL] IX Test Case insert leaf node failed. *****" << endl;
        return fail;
    }
}

