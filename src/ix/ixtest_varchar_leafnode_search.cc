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
        key[4 + j] = 'a' + i - 1;
    }
    rid.pageNum = i;
    rid.slotNum = i;
}

RC testCase_Varchar_Leafnode_Search(const string &indexFileName)
{
    // Functions tested

    cerr << endl << "***** In IX Test Case Varchar leaf node search *****" << endl;

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
    memcpy(&slot, (char*)tmpPage+getIndexLeafSlotOffset(0), sizeof(INDEXSLOT) );
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
    
    // insert another one in the middle
    key = malloc(7);
    rid.pageNum = 11;
    rid.slotNum = 11;
    char c[3];
    c[0] = 'z';
    c[1] = 'z';
    c[2] = 'a';
    int csize = 3;
    memcpy(key, &csize, sizeof(int));
    memcpy((char*)key+sizeof(int), c, 3);
    indexManager->insertVarcharLeafNode(key, rid, tmpPage, 7);
    free(key);

    memcpy(&slot, (char*)tmpPage+getIndexLeafSlotOffset(7), sizeof(INDEXSLOT) );
    data=malloc(slot.recordSize);
    memcpy(data, (char*)tmpPage+slot.pageOffset, slot.recordSize);
    cout<<(char*)data<<endl;
    free(data);
    cout<<"Insert Leaf Node checking finished !"<<endl;

    RecordMinLen freeSize = 0;
    RecordMinLen NodeType = LEAF_NODE;
    RecordMinLen slotInfos[3];
    slotInfos[2] = freeSize;
    slotInfos[1] = 8;
    slotInfos[0] = NodeType;
    memcpy( (char*)tmpPage+getNodeTypeOffset(), slotInfos, 3*sizeof(RecordMinLen) );
    ixfileHandle.appendPage(tmpPage);

    // test split leaf node
    void *upwardKey;
    void *newPage = malloc(PAGE_SIZE);
    key = malloc(7);
    rid.pageNum = 12;
    rid.slotNum = 12;
    c[0] = 'g';
    c[1] = 'g';
    c[2] = 'a';
    csize = 3;
    memcpy(key, &csize, sizeof(int));
    memcpy((char*)key+sizeof(int), c, csize);

    int newPageId;
    rc = indexManager->splitVarcharLeafNode( ixfileHandle, 0, newPageId, key, rid, &upwardKey, tmpPage, newPage);
    assert( rc == success && "split process failed" );

    int upwardKeySize = getVarcharSize(upwardKey);
    cout<<"upwardKeySize:"<<upwardKeySize<<endl;
    cout<<"newPageId:"<<newPageId<<endl;
    data = malloc(upwardKeySize);
    memcpy( data, upwardKey+sizeof(int), upwardKeySize );
    cout<<(char*)data<<endl;

    // check slotCount
    memcpy( &slotCount, (char*)tmpPage+getIndexSlotCountOffset(), sizeof(RecordMinLen) );
    assert(slotCount==4 && "slotCount not matched !");
    memcpy( &slotCount, (char*)newPage+getIndexSlotCountOffset(), sizeof(RecordMinLen) );
    assert(slotCount==5 && "new page slotCount not matched !");

    // check page rid
    // check new page
    memcpy(&slot, (char*)newPage+getIndexLeafSlotOffset(3), sizeof(INDEXSLOT) );
    RID trid;
    memcpy( &trid, (char*)newPage+slot.pageOffset+slot.recordSize, sizeof(RID) );
    cout<<slot.pageOffset<<","<<slot.recordSize<<endl;
    cout<<trid.pageNum<<","<<trid.slotNum<<endl;
    assert(trid.pageNum==rid.pageNum && trid.slotNum==rid.slotNum && "rid failed !");

    // check old page
    cout<<"Finish checking RID !"<<endl;

    // insert upward root
    int newRootPageId;
    void *newRootPage =malloc(PAGE_SIZE);
    rc = indexManager->insertVarcharRootPage(ixfileHandle, 0, newPageId, newRootPageId, upwardKey, newRootPage);
    assert(rc == success && "insertVarcharRootPage failed !");

    free(data);
    free(upwardKey);
    free(tmpPage);
    free(newPage);
    free(newRootPage);

    return success;
}

int main()
{
    // Global Initialization
    indexManager = IndexManager::instance();

    const string indexFileName = "student_idx";
    remove("student_idx");

    RC rcmain = testCase_Varchar_Leafnode_Search(indexFileName);
    if (rcmain == success) {
        cerr << "***** IX Test Case leaf node search finished. The result will be examined. *****" << endl;
        return success;
    } else {
        cerr << "***** [FAIL] IX Test Case leaf node search failed. *****" << endl;
        return fail;
    }
}

