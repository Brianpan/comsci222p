#include <iostream>

#include <cstdlib>
#include <cstdio>
#include <cstring>

#include "ix.h"
#include "ix_test_util.h"

IndexManager *indexManager;

RC testCase_Split_Intermediatenode(const string &indexFileName)
{
    // Functions tested

    cerr << endl << "***** In IX Test Case split intermediate node *****" << endl;

    // create index file
    RC rc = indexManager->createFile(indexFileName);
    assert(rc == success && "indexManager::createFile() should not fail.");

    // open index file
    IXFileHandle ixfileHandle;
    rc = indexManager->openFile(indexFileName, ixfileHandle);
    assert(rc == success && "indexManager::openFile() should not fail.");


    void *tmpPage = malloc(PAGE_SIZE);
    
    // prepare intermediate nodes
    int intermediateNodes = 5;
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

    RecordMinLen freeSize = 10; 
    RecordMinLen nodeType = INTERMEDIATE_NODE;
    RecordMinLen slotInfos[3];

    slotInfos[2] = freeSize;
    slotInfos[1] = intermediateNodes;
    slotInfos[0] = nodeType;
    memcpy( (char*)tmpPage+getNodeTypeOffset(), slotInfos, sizeof(RecordMinLen)*3  );
    ixfileHandle.appendPage(tmpPage);

    // start insert
    void *newPage = malloc(PAGE_SIZE);
    int curPageId = ixfileHandle.getNumberOfPages() - 1;
    int insertIdx = 3;

    IDX_PAGE_POINTER_TYPE rightPointer = 99;
    int upwardKey = 19;
    RC flag;

    flag = indexManager->splitFixedIntermediateNode<int>( ixfileHandle, curPageId, insertIdx, upwardKey, rightPointer, tmpPage, newPage);
    assert(flag == success && "split failed !");

    // print tmpPage
    RecordMinLen slotCount;
    memcpy(&slotCount, (char*)tmpPage+getIndexSlotCountOffset(), sizeof(RecordMinLen) );
    cout<<slotCount<<endl;
    cout<<"UpwardKey"<<upwardKey<<endl;
    cout<<"--- Split left : ----"<<endl;
    for(int i=0; i<slotCount;i++)
    {
        int t;
        IDX_PAGE_POINTER_TYPE pointer;
        memcpy( &t, (char*)tmpPage+getFixedKeyOffset(i), sizeof(int) );
        memcpy( &pointer, (char*)tmpPage+getFixedKeyPointerOffset(i), sizeof(IDX_PAGE_POINTER_TYPE) );
        cout<<"tmpNode"<<t<<"left pointer"<<pointer<<endl;
    }
    
    // print newPage
    cout<<"--- Split right : ----"<<endl;
    memcpy(&slotCount, (char*)newPage+getIndexSlotCountOffset(), sizeof(RecordMinLen) );
    cout<<slotCount<<endl;
    for(int i=0; i<slotCount;i++)
    {
        int t;
        IDX_PAGE_POINTER_TYPE pointer;
        memcpy( &t, (char*)newPage+getFixedKeyOffset(i), sizeof(int) );
        memcpy( &pointer, (char*)newPage+getFixedKeyPointerOffset(i), sizeof(IDX_PAGE_POINTER_TYPE) );
        cout<<"tmpNode"<<t<<"left pointer"<<pointer<<endl;
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

    RC rcmain = testCase_Split_Intermediatenode(indexFileName);
    if (rcmain == success) {
        cerr << "***** IX Test Case split intermediate leaf node finished. The result will be examined. *****" << endl;
        return success;
    } else {
        cerr << "***** [FAIL] IX Test Case split intermediate node failed. *****" << endl;
        return fail;
    }
}

