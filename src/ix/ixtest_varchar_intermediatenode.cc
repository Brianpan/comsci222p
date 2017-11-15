#include <iostream>

#include <cstdlib>
#include <cstdio>
#include <cstring>

#include "ix.h"
#include "ix_test_util.h"

IndexManager *indexManager;

void prepareKeyAndRid(const unsigned count, const unsigned i, char* key, RID &rid){
    *(int *)key = count;
    for(unsigned j = 0; j < count; j++)
    {
        key[4 + j] = 'b' + i - 1;
    }
    rid.pageNum = i;
    rid.slotNum = i;
}

RC testCase_Varchar_Intermediatenode(const string &indexFileName)
{
    // Functions tested

    cerr << endl << "***** In IX Test Case testCase_Varchar_Intermediatenode *****" << endl;

    // create index file
    RC rc = indexManager->createFile(indexFileName);
    assert(rc == success && "indexManager::createFile() should not fail.");

    // open index file
    IXFileHandle ixfileHandle;
    rc = indexManager->openFile(indexFileName, ixfileHandle);
    assert(rc == success && "indexManager::openFile() should not fail.");


    void *tmpPage = malloc(PAGE_SIZE);
    int slotCount = 7;
    
    // insert root 
    // check createVarcharNewLeafNode
    int count = 5;
    void *key = malloc(9);
    RID rid;
    prepareKeyAndRid(count, 1, (char*)key, rid);
    rc = indexManager->createVarcharNewLeafNode(tmpPage, key, rid);
    assert(rc == success && "indexManager::createVarcharNewLeafNode." );
    free(key);

    INDEXSLOT slot;
    memcpy(&slot, (char*)tmpPage+getIndexSlotOffset(0), sizeof(INDEXSLOT) );
    assert( slot.pageOffset == 0 && slot.recordSize == 5 && "slot info fail");
    void *data=malloc(5);
    memcpy(data, (char*)tmpPage+slot.pageOffset, slot.recordSize);
    cout<<(char*)data<<endl;
    cout<<endl;
    free(data);

    for(int i=1;i<slotCount;i++)
    {
        int len = 5 + i;
        void *d = malloc(len+sizeof(int));
        prepareKeyAndRid(len, i, (char*) d, rid);
        indexManager-> insertVarcharLeafNode(d,rid,tmpPage,i);
    }
    
    
    RecordMinLen freeSize = 0;
    RecordMinLen NodeType = LEAF_NODE;
    RecordMinLen slotInfos[3];
    slotInfos[2] = freeSize;
    slotInfos[1] = 7;
    slotInfos[0] = NodeType;
    memcpy( (char*)tmpPage+getNodeTypeOffset(), slotInfos, 3*sizeof(RecordMinLen) );
    ixfileHandle.appendPage(tmpPage);

    // test split leaf node
    void *upwardKey;
    void *newPage = malloc(PAGE_SIZE);
    key = malloc(7);
    rid.pageNum = 12;
    rid.slotNum = 12;
    char c[3];
    c[0] = 'g';
    c[1] = 'g';
    c[2] = 'a';
    int csize = 3;
    memcpy(key, &csize, sizeof(int));
    memcpy((char*)key+sizeof(int), c, csize);

    int newPageId;
    rc = indexManager->splitVarcharLeafNode( ixfileHandle, 0, newPageId, key, rid, &upwardKey, tmpPage, newPage);
    assert( rc == success && "split leaf process failed" );

    // prepare new intermediate node
    void *iterPage = malloc(PAGE_SIZE);
    RecordMinLen iterFreeSize = 0;
    RecordMinLen iterslotCount = 4;
    RecordMinLen iterNodeType = INTERMEDIATE_NODE;
    slotInfos[2] = iterFreeSize;
    slotInfos[1] = iterslotCount;
    slotInfos[0] = iterNodeType;
    memcpy( (char*)iterPage+getNodeTypeOffset(), slotInfos, sizeof(RecordMinLen)*3 );

    // copy slots
    slot.pageOffset = sizeof(IDX_PAGE_POINTER_TYPE);
    slot.recordSize = 5;
    memcpy( (char*)iterPage+getIndexSlotOffset(0), &slot, sizeof(INDEXSLOT) );
    slot.pageOffset = sizeof(IDX_PAGE_POINTER_TYPE) + slot.pageOffset + slot.recordSize;
    slot.recordSize = 6;
    memcpy( (char*)iterPage+getIndexSlotOffset(1), &slot, sizeof(INDEXSLOT) );
    slot.pageOffset = sizeof(IDX_PAGE_POINTER_TYPE) + slot.pageOffset + slot.recordSize;
    slot.recordSize = 7;
    memcpy( (char*)iterPage+getIndexSlotOffset(2), &slot, sizeof(INDEXSLOT) );
    slot.pageOffset = sizeof(IDX_PAGE_POINTER_TYPE) + slot.pageOffset + slot.recordSize;
    slot.recordSize = 8;
    memcpy( (char*)iterPage+getIndexSlotOffset(3), &slot, sizeof(INDEXSLOT) );

    // copy data
    IDX_PAGE_POINTER_TYPE pointer;
    pointer = 10;
    memcpy( (char*)iterPage, &pointer, sizeof(IDX_PAGE_POINTER_TYPE) );
    data = malloc(5);
    for(unsigned j = 0; j < 5; j++)
    {
        *(char*)((char*)data +j) = 'c';
    }
    memcpy((char*)iterPage+sizeof(IDX_PAGE_POINTER_TYPE), data, 5);
    cout<<"d0"<<(char*)data<<endl;
    free(data);

    pointer = 0;
    memcpy( (char*)iterPage+5+sizeof(IDX_PAGE_POINTER_TYPE), &pointer, sizeof(IDX_PAGE_POINTER_TYPE) );
    data = malloc(6);
    for(unsigned j = 0; j < 6; j++)
    {
        *(char*)((char*)data +j) = 'd';
    }
    memcpy((char*)iterPage+5+2*sizeof(IDX_PAGE_POINTER_TYPE), data, 6);
    cout<<"d1"<<(char*)data<<endl;
    free(data);

    pointer = 11;
    memcpy( (char*)iterPage+11+2*sizeof(IDX_PAGE_POINTER_TYPE), &pointer, sizeof(IDX_PAGE_POINTER_TYPE) );
    data = malloc(7);
    for(unsigned j = 0; j < 7; j++)
    {
        *(char*)((char*)data +j) = 'e';
    }
    memcpy((char*)iterPage+11+3*sizeof(IDX_PAGE_POINTER_TYPE), data, 7);
    cout<<"d2"<<(char*)data<<endl;
    free(data);

    pointer = 12;
    memcpy( (char*)iterPage+18+3*sizeof(IDX_PAGE_POINTER_TYPE), &pointer, sizeof(IDX_PAGE_POINTER_TYPE) );
    data = malloc(8);
    for(unsigned j = 0; j < 8; j++)
    {
        *(char*)((char*)data +j) = 'f';
    }
    pointer = 15;
    memcpy((char*)iterPage+18+4*sizeof(IDX_PAGE_POINTER_TYPE), data, 8);
    memcpy( (char*)iterPage+26+4*sizeof(IDX_PAGE_POINTER_TYPE), &pointer, sizeof(IDX_PAGE_POINTER_TYPE) );
    cout<<"d3"<<(char*)data<<endl;
    free(data);

    ixfileHandle.appendPage(iterPage);
    int curPageId = ixfileHandle.getNumberOfPages() - 1;
    int tmp;
    memcpy(&tmp, upwardKey, sizeof(int));
    cout<<"Len"<<tmp<<(char*)((char*)upwardKey+4)<<endl;
    // check searchVarcharIntermediateNode
    INDEXPOINTER insertPointer = indexManager->searchVarcharIntermediateNode(upwardKey, curPageId, iterPage, 0, 4 );
    cout<<"indexID"<<insertPointer.indexId<<endl;
    assert(insertPointer.indexId == 2 && "searchVarcharIntermediateNode failed !");
    cout<<"searchVarcharIntermediateNode success!"<<endl;

    // split right page
    IDX_PAGE_POINTER_TYPE rightPointer = 1;
    rc = indexManager->splitVarcharIntermediateNode(ixfileHandle, curPageId, insertPointer.indexId, &upwardKey, rightPointer, iterPage, newPage);
    assert(rc == success && "splitVarcharIntermediateNode failed !" );

    int upwardKeyLen = getVarcharSize(upwardKey);
    data = malloc(upwardKeyLen);
    char a[upwardKeyLen];
    memcpy(a, (char*)upwardKey+sizeof(int), upwardKeyLen);
    cout<<(char*)a<<endl;
    assert(upwardKeyLen == 8 && "splitVarcharIntermediateNode validation failed !");


    // free
    free(data);
    free(upwardKey);
    free(tmpPage);
    free(newPage);
    free(iterPage);
    return success;
}

int main()
{
    // Global Initialization
    indexManager = IndexManager::instance();

    const string indexFileName = "student_idx";
    remove("student_idx");

    RC rcmain = testCase_Varchar_Intermediatenode(indexFileName);
    if (rcmain == success) {
        cerr << "***** IX Test Case testCase_Varchar_Intermediatenode finished. The result will be examined. *****" << endl;
        return success;
    } else {
        cerr << "***** [FAIL] IX Test Case testCase_Varchar_Intermediatenode failed. *****" << endl;
        return fail;
    }
}

