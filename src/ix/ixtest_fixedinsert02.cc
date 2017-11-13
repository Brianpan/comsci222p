#include <iostream>

#include <cstdlib>
#include <cstdio>
#include <cstring>

#include "ix.h"
#include "ix_test_util.h"

IndexManager *indexManager;

RC testCase_Fixed_Insert02(const string &indexFileName)
{
    // Functions tested

    cerr << endl << "***** In IX Test Case fixed insert 02 leaf node *****" << endl;

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

    // insert a fake root node
    memset(tmpPage, 0, PAGE_SIZE);
    nodeValueStart += (slotCount-1)*3;
    IDX_PAGE_POINTER_TYPE p0;
    int rootNodes = 4;

    // left most pointer is the pointer before
    IDX_PAGE_POINTER_TYPE pointer0 = 0;
    memcpy( (char*)tmpPage+getFixedKeyPointerOffset(0), &pointer0, sizeof(int) );

    for(int idx = 1; idx <= rootNodes; idx++)
    {
        p0 = nodeValueStart + idx*2;
        memcpy( (char*)tmpPage+getFixedKeyPointerOffset(idx), &p0, sizeof(IDX_PAGE_POINTER_TYPE) );
    }

    int nodeValue = nodeValueStart;
    for(int idx=0;idx<rootNodes;idx++)
    {
        nodeValue = nodeValueStart + idx*3;
        cout<<"Node Value"<<nodeValue<<endl;
        memcpy( (char*)tmpPage+getFixedKeyOffset(idx), &nodeValue, sizeof(int) );
    }

    freeSize = 0; 
    nodeType = ROOT_NODE;
    slotInfos[3];

    slotInfos[2] = freeSize;
    slotInfos[1] = rootNodes;
    slotInfos[0] = nodeType;
    memcpy( (char*)tmpPage+getNodeTypeOffset(), slotInfos, sizeof(RecordMinLen)*3  );
    ixfileHandle.appendPage(tmpPage);
    ixfileHandle.treeHeight = 2;
    ixfileHandle.rootPageId = 1;

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
    assert(ixfileHandle.getNumberOfPages() == 5 && "split failed !");

    // print root node
    RecordMinLen rootSlotCount;
    ixfileHandle.readPage( 4, tmpPage );
    memcpy( &rootSlotCount, (char*)tmpPage+getIndexSlotCountOffset(), sizeof(RecordMinLen) );
    assert( rootSlotCount == 1 && "root Node failed !" );
    
    cout<< "Root info :"<<endl;
    int rootValue;
    memcpy( &rootValue, (char*)tmpPage+getFixedKeyOffset(0), sizeof(int) );
    cout<<"rootValue:"<<rootValue<<endl;
    assert( rootValue == 29 && "root Node Value wrong !");
    IDX_PAGE_POINTER_TYPE rootLeft, rootRight;
    memcpy(&rootLeft, (char*)tmpPage+getFixedKeyPointerOffset(0), sizeof(IDX_PAGE_POINTER_TYPE) );
    memcpy(&rootRight, (char*)tmpPage+getFixedKeyPointerOffset(1), sizeof(IDX_PAGE_POINTER_TYPE) );
    cout<<"Left pointer:"<<rootLeft<<" Right pointer:"<<rootRight<<endl;

    cout<<"Intermediate info :"<<endl;
    // two intermediate slots
    RecordMinLen intermediateSlotCount;
    cout<<"Inter Left page : "<<endl;
    ixfileHandle.readPage( 1, tmpPage );
    memcpy( &intermediateSlotCount, (char*)tmpPage+getIndexSlotCountOffset(), sizeof(RecordMinLen) );
    assert( intermediateSlotCount == 2 && "intermediate Node1 failed !" );
    int i0;
    for(int idx = 0; idx < intermediateSlotCount; idx++)
    {
        memcpy( &i0, (char*)tmpPage+getFixedKeyOffset(idx), sizeof(int) );
        cout<<"Node:"<<i0<<endl;
    }

    cout<<"Inter Right page : "<<endl;
    ixfileHandle.readPage( 3, tmpPage );
    memcpy( &intermediateSlotCount, (char*)tmpPage+getIndexSlotCountOffset(), sizeof(RecordMinLen) );
    assert( intermediateSlotCount == 2 && "intermediate Node1 failed !" );
    for(int idx = 0; idx < intermediateSlotCount; idx++)
    {
        memcpy( &i0, (char*)tmpPage+getFixedKeyOffset(idx), sizeof(int) );
        cout<<"Node:"<<i0<<endl;
    }

    cout<<"Leaf Info :"<<endl;
    // print two nodes
    ixfileHandle.readPage(0, tmpPage);
    cout<<"Leaf Left page : "<<endl;
    RecordMinLen curSlotCount;
    memcpy(&curSlotCount, (char*)tmpPage+getIndexSlotCountOffset(), sizeof(RecordMinLen) );
    LEAFNODE<int> curLeafNodes[curSlotCount];
    memcpy( curLeafNodes, tmpPage, sizeof(LEAFNODE<int>)*curSlotCount );
    for(int i; i<curSlotCount;i++)
    {
    	cout<<"Node: "<<curLeafNodes[i].key<<endl;
    }

    ixfileHandle.readPage(2, newPage);
    cout<<"Leaf Right page : "<<endl;
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

    RC rcmain = testCase_Fixed_Insert02(indexFileName);
    if (rcmain == success) {
        cerr << "***** IX Test Case fixed insert 02 finished. The result will be examined. *****" << endl;
        return success;
    } else {
        cerr << "***** [FAIL] IX Test Case fixed insert 02 failed. *****" << endl;
        return fail;
    }
}

