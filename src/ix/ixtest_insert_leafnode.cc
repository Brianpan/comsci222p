#include <iostream>

#include <cstdlib>
#include <cstdio>
#include <cstring>

#include "ix.h"
#include "ix_test_util.h"

IndexManager *indexManager;

RC testCase_Insert_Leafnode(const string &indexFileName)
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
    RecordMinLen slotCount = 5;
    
    // prepare leafnodes
    LEAFNODE<int> leafNodes[slotCount];

    for(int idx = 0; idx <= slotCount; idx++)
    {
        leafNodes[idx].key = nodeValueStart + idx*3;
        leafNodes[idx].rid.pageNum = 0;
        leafNodes[idx].rid.slotNum = nodeValueStart + idx;
    }

    memcpy(tmpPage, leafNodes, slotCount*sizeof(LEAFNODE<int>));
    
    RC r0;
    LEAFNODE<int> tmpLeafNode;
    // insert case 0
    int k = 10;
    RID tmpRid;
    tmpRid.pageNum = 1;
    tmpRid.slotNum = 0;
    r0 = indexManager->insertLeafNode<int>(k, tmpRid, tmpPage, slotCount);
    slotCount += 1;
    assert( r0 == success && "Insert 0 failed !" );

    memcpy( &tmpLeafNode, (char*)tmpPage, sizeof(LEAFNODE<int>) );
    LEAFNODE<int> nodes[slotCount];
    memcpy(nodes, tmpPage, sizeof(LEAFNODE<int>)*slotCount);
    for(int i=0;i<slotCount;i++)
    {
    	cout<<"Node:"<<nodes[i].key<<endl;
    }
    assert( tmpLeafNode.key == 10 && "Insert check 0 failed !" );
    cout<<"Insert 0 finish !"<<endl;

    // test 2
    k = 18;
    tmpRid.slotNum = 1;
    r0 = indexManager->insertLeafNode<int>(k, tmpRid, tmpPage, slotCount);
    slotCount += 1;
    assert( r0==success && "Insert 1 failed !" );
    LEAFNODE<int> nodes2[slotCount];
    memcpy(nodes2, tmpPage, sizeof(LEAFNODE<int>)*slotCount);
    for(int i=0;i<slotCount;i++)
    {
        	cout<<"Node:"<<nodes2[i].key<<endl;
    }
    memcpy( &tmpLeafNode, (char*)tmpPage+sizeof(LEAFNODE<int>)*4, sizeof(LEAFNODE<int>) );
    assert( tmpLeafNode.key == 18 && "Insert check 1 failed !" );
    cout<<"Insert 1 finish !"<<endl;

    // test 3
    k = 24;
    tmpRid.slotNum = 2;
    r0 = indexManager->insertLeafNode<int>(k, tmpRid, tmpPage, slotCount);
    slotCount += 1;
    assert( r0==success && "Insert 2 failed !" );
    LEAFNODE<int> nodes3[slotCount];
    memcpy(nodes3, tmpPage, sizeof(LEAFNODE<int>)*slotCount);
    for(int i=0;i<slotCount;i++)
    {
         cout<<"Node:"<<nodes3[i].key<<endl;
    }
    memcpy( &tmpLeafNode, (char*)tmpPage+sizeof(LEAFNODE<int>)*7, sizeof(LEAFNODE<int>) );
    assert( tmpLeafNode.key == 24 && "Insert check 2 failed !" );

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

    RC rcmain = testCase_Insert_Leafnode(indexFileName);
    if (rcmain == success) {
        cerr << "***** IX Test Case insert leaf node finished. The result will be examined. *****" << endl;
        return success;
    } else {
        cerr << "***** [FAIL] IX Test Case insert leaf node failed. *****" << endl;
        return fail;
    }
}

