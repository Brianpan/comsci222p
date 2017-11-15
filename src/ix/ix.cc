
#include "ix.h"

IndexManager* IndexManager::_index_manager = 0;

IndexManager* IndexManager::instance()
{
    if(!_index_manager)
        _index_manager = new IndexManager();

    return _index_manager;
}

IndexManager::IndexManager()
{
}

IndexManager::~IndexManager()
{
}

RC IndexManager::createFile(const string &fileName)
{
    if( isFileExist(fileName) )
    {
        return -1;
    }

    // create file
    fstream fp;
    fp.open(fileName, ios::out);
    if( !fp ){
        return -1;
    }

    fp.close();
    return 0;
}

RC IndexManager::destroyFile(const string &fileName)
{
    if( !isFileExist(fileName) )
    {
        return -1;
    }

    remove( fileName.c_str() );

    return 0;
}

RC IndexManager::openFile(const string &fileName, IXFileHandle &ixfileHandle)
{
    if(ixfileHandle._handler != NULL)
    {
        return -1;
    }

    if( !isFileExist(fileName) )
    {
            return -1;
    }

    // open file
    // for here we should init
    ixfileHandle._handler= new fstream();
    ixfileHandle._handler->open(fileName);
    ixfileHandle.fetchFileData();

    return 0;
}

RC IndexManager::closeFile(IXFileHandle &ixfileHandle)
{
    if(ixfileHandle._handler == NULL)
    {
        return -1;
    }

    // save pages to disk

    // close file
    ixfileHandle.saveCounter();
    ixfileHandle._handler->close();

    delete ixfileHandle._handler;
    ixfileHandle._handler = NULL;
    return 0;
}




RC IndexManager::insertEntry(IXFileHandle &ixfileHandle, const Attribute &attribute, const void *key, const RID &rid)
{
    AttrType idxAttrType = attribute.type;
    int rootPageId = ixfileHandle.rootPageId;


    if( idxAttrType == TypeReal )
    {
        if( rootPageId == -1 )
        {
            ixfileHandle.indexType = REAL_TYPE;
        }
        else
        {
            if( ixfileHandle.indexType != REAL_TYPE )
            {
                return -1;
            }
        }
        return insertFixedLengthEntry<float>( ixfileHandle, key, rid );
    }
    else if( idxAttrType == TypeInt)
    {
    	if( rootPageId == -1 )
        {
            ixfileHandle.indexType = INT_TYPE;
        }
        else
        {
            if( ixfileHandle.indexType != INT_TYPE )
            {
                return -1;
            }
        }

        return insertFixedLengthEntry<int>( ixfileHandle, key, rid );
    }
    else
    {
        if( rootPageId == -1 )
        {
            ixfileHandle.indexType = CHAR_TYPE;
        }
        else
        {
            if( ixfileHandle.indexType != CHAR_TYPE )
            {
                return -1;
            }
        }

        return insertVarLengthEntry( ixfileHandle, key, rid );
    }
    
}

template<class T>
RC IndexManager::insertFixedLengthEntry(IXFileHandle &ixfileHandle, const void *key, const RID &rid){
    RC success = -1;

    T keyValue;
    memcpy( &keyValue, key, sizeof(T) );


    int rootPageId = ixfileHandle.rootPageId;
    void *tmpPage = malloc(PAGE_SIZE);

    // empty tree
    if( rootPageId == -1 )
    {
        // insert leaf Node
        memset( tmpPage, 0, PAGE_SIZE );
        if( createFixedNewLeafNode(tmpPage, key, rid) == 0 )
        {
            ixfileHandle.appendPage(tmpPage);
            // update rootPageId
            ixfileHandle.rootPageId = 0;
            ixfileHandle.treeHeight = 1;
            success = 0;
        }
    }
    else
    {
        ixfileHandle.readPage( rootPageId, tmpPage );
        vector<INDEXPOINTER> traversePointerList;
        int tmpPageId = rootPageId;
        int traverseCallback = traverseFixedLengthNode<T>(ixfileHandle, keyValue, tmpPageId, tmpPage, traversePointerList);

        if(  traverseCallback == 0 )
        {
            // after traverFixedLengthNode we are in the leaf nodes
            int leafPageId = tmpPageId;

        	RecordMinLen freeSize;
            memcpy( &freeSize, (char*)tmpPage+getIndexRestSizeOffset(), sizeof(RecordMinLen) );
            int leftSize = freeSize-getFixedKeySize();
            // should not split
            if( leftSize >= 0 )
            {
                RecordMinLen slotCount;
                memcpy( &slotCount, (char*)tmpPage+getIndexSlotCountOffset(), sizeof(RecordMinLen) );
                // start get inserted position
                if( insertLeafNode<T>(keyValue, rid, tmpPage, slotCount) != 0 )
                {
                    success = -1;
                    free(tmpPage);
                    return success;
                }

                // maybe we can save some memcpy
                freeSize = freeSize - getFixedKeySize();
                memcpy( (char*)tmpPage+getIndexRestSizeOffset(), &freeSize, sizeof(RecordMinLen) );
                slotCount += 1;
                memcpy( (char*)tmpPage+getIndexSlotCountOffset(), &slotCount, sizeof(RecordMinLen) );
                ixfileHandle.writePage(leafPageId, tmpPage);
                success = 0;
            }
            // should split
            else
            {
                void *newPage = malloc(PAGE_SIZE);
                // case 1: height = 1
                if( ixfileHandle.treeHeight == 1 )
                {
                    T upwardKey;
                    int newPageId;
                    if( splitFixedLeafNode<T>( ixfileHandle, leafPageId, newPageId, keyValue, rid, upwardKey, tmpPage, newPage) == 0 )
                    {
                        // insert new root
                        int newRootPageId;
                        if( insertRootPage<T>(ixfileHandle, leafPageId, newPageId, newRootPageId, upwardKey, tmpPage) == 0)
                        {
                            ixfileHandle.rootPageId = newRootPageId;
                            ixfileHandle.treeHeight += 1;
                            success = 0;
                        }
                    }

                }
                // case 2: height > 1
                else
                {
                    T upwardKey;
                    int newPageId;
                    if( splitFixedLeafNode<T>( ixfileHandle, leafPageId, newPageId, keyValue, rid, upwardKey, tmpPage, newPage) == 0 )
                    {
                        IDX_PAGE_POINTER_TYPE leftPointer = leafPageId;
                        IDX_PAGE_POINTER_TYPE rightPointer = newPageId;

                        // traverse back
                        while( traversePointerList.size() > 0 )
                        {
                            INDEXPOINTER curParentPointer = traversePointerList.back();
                            IDX_PAGE_POINTER_TYPE parentPageNum = curParentPointer.curPageNum;
                            ixfileHandle.readPage( parentPageNum, tmpPage );

                            RecordMinLen freeSize;
                            memcpy( &freeSize, (char*)tmpPage+getIndexRestSizeOffset(), sizeof(RecordMinLen) );
                            RecordMinLen nodeType;
                            memcpy( &nodeType, (char*)tmpPage+getNodeTypeOffset(), sizeof(RecordMinLen) );
                            RecordMinLen slotCount;
                            memcpy( &slotCount, (char*)tmpPage+getIndexSlotCountOffset(), sizeof(RecordMinLen) );

                            int parentIdx = curParentPointer.indexId;
                            bool isLeft = curParentPointer.left;
                            int leftSize = freeSize - getInterNodeSize();
                            // can fit in parent node
                            if( leftSize >= 0 )
                            {   unsigned moveSize = (slotCount - parentIdx)*( getFixedIndexSize() + sizeof(IDX_PAGE_POINTER_TYPE) );
                                if( moveSize > 0 )
                                {    
                                    memmove( (char*)tmpPage+getFixedKeyPointerOffset(parentIdx+1)+sizeof(IDX_PAGE_POINTER_TYPE), (char*)tmpPage+getFixedKeyPointerOffset(parentIdx)+sizeof(IDX_PAGE_POINTER_TYPE), moveSize );
                                }
                                // insert new key
                                memcpy( (char*)tmpPage+getFixedKeyPointerOffset(parentIdx)+sizeof(IDX_PAGE_POINTER_TYPE), &upwardKey, getFixedIndexSize() );
                                memcpy( (char*)tmpPage+getFixedKeyPointerOffset(parentIdx)+sizeof(IDX_PAGE_POINTER_TYPE)+getFixedIndexSize(), &rightPointer, sizeof(IDX_PAGE_POINTER_TYPE) );
                                
                                // write page info
                                freeSize = leftSize;
                                slotCount += 1;
                                memcpy( (char*)tmpPage+getIndexRestSizeOffset(), &freeSize, sizeof(RecordMinLen) );
                                memcpy( (char*)tmpPage+getIndexSlotCountOffset(), &slotCount, sizeof(RecordMinLen) );

                                ixfileHandle.writePage( parentPageNum, tmpPage );
                                success = 0;
                                break;
                            }
                            // should continue to split and go up
                            else
                            {
                                if( nodeType == ROOT_NODE )
                                {
                                    if( splitFixedIntermediateNode<T>(ixfileHandle, parentPageNum, parentIdx, upwardKey, rightPointer, tmpPage, newPage) != 0 )
                                    {
                                        success = -1;
                                        break;
                                    }

                                    leftPointer = parentPageNum;
                                    // insert new root node
                                    int newRootPageId;
                                    if( insertRootPage<T>(ixfileHandle, leftPointer, rightPointer, newRootPageId, upwardKey, tmpPage) != 0)
                                    {
                                        success = -1;
                                        break;
                                    }

                                    ixfileHandle.rootPageId = newRootPageId;
                                    ixfileHandle.treeHeight += 1;
                                    success = 0;
                                    break;
                                }
                                else
                                {
                                    if( splitFixedIntermediateNode<T>(ixfileHandle, parentPageNum, parentIdx, upwardKey, rightPointer, tmpPage, newPage) != 0 )
                                    {
                                        success = -1;
                                        break;
                                    }
                                    leftPointer = parentPageNum;
                                }
                            }
                            // pop pointer out
                            traversePointerList.pop_back();
                        }
                    } 
                }
                free(newPage);
            }

        }
        // this case should insert new leaf node
        else if( traverseCallback == 1 )
        {
            void *newPage = malloc(PAGE_SIZE);
            createFixedNewLeafNode(newPage, key, rid);
            ixfileHandle.appendPage(newPage);
            IDX_PAGE_POINTER_TYPE leafPageNum = ixfileHandle.getNumberOfPages() - 1;
            // update intermediate node
            INDEXPOINTER parentPointer = traversePointerList.back();
            // update node
            updateParentPointer( ixfileHandle, parentPointer, leafPageNum );
            // end update intermediate node
            free(newPage);

            success = 0;
        }
        else
        {
            success = -1;
        }    
    }

    // free
    free(tmpPage);
    
    return success;
}

// create empty leaf node
RC IndexManager::createFixedNewLeafNode(void *data, const void *key, const RID &rid){
    unsigned auxSlotSize = 3*sizeof(RecordMinLen);
    RecordMinLen freeSize = PAGE_SIZE - auxSlotSize -getFixedKeySize() - sizeof(IDX_PAGE_POINTER_TYPE);
    RecordMinLen slotCount = 1;
    RecordMinLen NodeType = LEAF_NODE;
    RecordMinLen slotInfos[3];
    slotInfos[2] = freeSize;
    slotInfos[1] = slotCount;
    slotInfos[0] = NodeType;

    IDX_PAGE_POINTER_TYPE rightPointer = -1;

    memcpy( (char*)data+getNodeTypeOffset(), slotInfos, auxSlotSize );
    memcpy( (char*)data+getLeafNodeRightPointerOffset(), &rightPointer, sizeof(IDX_PAGE_POINTER_TYPE) );
    memcpy( data, key, getFixedIndexSize() );
    memcpy( data+getFixedIndexSize(), &rid, sizeof(RID) );
    return 0;
}

// traverse to which leaf node to insert
template<class T>
int IndexManager::traverseFixedLengthNode(IXFileHandle &ixfileHandle, T keyValue, int &curPageId, void *idxPage, vector<INDEXPOINTER> &traversePointerList){
    int status = 0;
    RecordMinLen slotCount;
    memcpy( &slotCount, (char*)idxPage+getIndexSlotCountOffset(), sizeof(RecordMinLen) );

    RecordMinLen nodeType;
    memcpy( &nodeType, (char*)idxPage+getNodeTypeOffset(), sizeof(RecordMinLen) );

    while( nodeType != LEAF_NODE )
    {
    	INDEXPOINTER childPointer;
        childPointer = searchFixedIntermediateNode<T>(keyValue, curPageId, idxPage, 0, slotCount);


        traversePointerList.push_back(childPointer);

        IDX_PAGE_POINTER_TYPE pageNum = childPointer.pageNum;
        
        // empty page no leaf node
        if(pageNum == -1)
        {
            status = 1;
            break;
        }
        else
        {
            if( ixfileHandle.readPage( pageNum, idxPage ) != 0 )
            {
                return -1;
            }
            curPageId = pageNum; 
            memcpy( &nodeType, (char*)idxPage+getNodeTypeOffset(), sizeof(RecordMinLen) );
            
        }
    }

    return status;
}

template<class T>
INDEXPOINTER IndexManager::searchFixedIntermediateNode(T keyValue, int curPageId, const void *idxPage, RecordMinLen head, RecordMinLen tail){
    
    // node should larger or equal than keyValue
    T compareLeft;
    T compareRight;
    RecordMinLen mid;
    RecordMinLen slotCount = tail;
    INDEXPOINTER idxPointer;
    IDX_PAGE_POINTER_TYPE pageNum;
    bool isMatch = false;

    while( (tail - head) > 1 )
    {
    	mid = (head + tail)/2;
    	memcpy( &compareLeft, (char*)idxPage+getFixedKeyOffset(mid), sizeof(T) );
    	// to check it is fit or not
    	if( compareKey<T>(keyValue, compareLeft) )
    	{
    		if( (mid+1) != tail )
    		{
    			memcpy( &compareRight, (char*)idxPage+getFixedKeyOffset(mid+1), sizeof(T) );
    			if( !compareKey<T>(keyValue, compareRight) )
    			{
    				isMatch = true;
    				memcpy( &pageNum, (char*)idxPage+getFixedKeyPointerOffset(mid+1), sizeof(IDX_PAGE_POINTER_TYPE) );
    				idxPointer.pageNum = pageNum;
                    idxPointer.indexId = mid + 1;
                    idxPointer.left = 0;
    				break;
    			}
    		}
    		if( mid == (tail - 1) )
    		{
    		    head = mid;
    		    break;
    		}
            head = mid + 1;
    	}
        else
        {
            tail = mid;
        }
    }

    // tail - head == 1 & not matched yet
    if( isMatch == false )
    {
        // check only 1 element case
        memcpy( &compareLeft, (char*)idxPage+getFixedKeyOffset(head), sizeof(T) );
        if(keyValue >= compareLeft)
        {
            memcpy( &pageNum, (char*)idxPage+getFixedKeyPointerOffset(head+1), sizeof(IDX_PAGE_POINTER_TYPE) );
            idxPointer.pageNum = pageNum;
            idxPointer.indexId = head+1;
            idxPointer.left = 0;
        }
        else
        {
            memcpy( &pageNum, (char*)idxPage+getFixedKeyPointerOffset(head), sizeof(IDX_PAGE_POINTER_TYPE) );
            idxPointer.pageNum = pageNum;
            idxPointer.indexId = head;
            idxPointer.left = 1;
        }

    }

    idxPointer.curPageNum = curPageId;
    return idxPointer;
}

RC IndexManager::updateParentPointer( IXFileHandle &ixfileHandle, INDEXPOINTER indexPointer, IDX_PAGE_POINTER_TYPE pageNum ){
    RC success = 0;
    IDX_PAGE_POINTER_TYPE parentPageNum = indexPointer.curPageNum;
    int parentIndexId = indexPointer.indexId;

    void *tmpPage = malloc(PAGE_SIZE);
    if( ixfileHandle.readPage(parentPageNum, tmpPage) != 0 )
    {
        free(tmpPage);
        return -1;
    }

    memcpy( (char*)tmpPage+getFixedKeyPointerOffset(parentIndexId), &pageNum, sizeof(IDX_PAGE_POINTER_TYPE) );

    if( ixfileHandle.writePage(parentPageNum, tmpPage) != 0 )
    {
        success = -1;
    }

    free(tmpPage);
    return success;
}

// insert node into enough leaf node
template<class T>
RC IndexManager::insertLeafNode(T keyValue, const RID &rid, void *data, RecordMinLen slotCount){

    LEAFNODE<T> leafNode;
    leafNode.key = keyValue;
    leafNode.rid = rid;

    IDX_PAGE_POINTER_TYPE insertIdx = searchFixedLeafNode(keyValue, data, slotCount);
    unsigned toMoveSize = (slotCount - insertIdx)*sizeof(LEAFNODE<T>);
    if(toMoveSize > 0)
    {
        unsigned moveDestPosition = (insertIdx+1)*sizeof(LEAFNODE<T>);
        memmove( (char*)data+moveDestPosition, (char*)data+insertIdx*sizeof(LEAFNODE<T>), toMoveSize );
    }
    memcpy( (char*)data+insertIdx*sizeof(LEAFNODE<T>), &keyValue, sizeof(LEAFNODE<T>) );
    return 0;
}

template<class T>
IDX_PAGE_POINTER_TYPE IndexManager::searchFixedLeafNode(T keyValue, void *data, RecordMinLen slotCount){
    LEAFNODE<T> dataList[slotCount];
    memcpy( dataList, data, sizeof(LEAFNODE<T>)*slotCount );
    int head = 0;
    int tail = slotCount;
    int mid;
    bool isMatch = false;
    // return idx
    IDX_PAGE_POINTER_TYPE idx;
    T compareLeft;
    T compareRight;

    while( (tail - head) > 1 )
    {
        mid = (head+tail)/2;
        compareLeft = dataList[mid].key;
        //
        if( compareKey<T>(keyValue, compareLeft) )
        {
            if( (mid+1) != tail )
            {
                compareRight = dataList[mid+1].key;
                if( !compareKey<T>(keyValue, compareRight) )
                {
                    isMatch = true;
                    idx = mid+1;
                    break;
                }
            }
            if( (mid+1) == tail)
            {
                head = mid;
                break;
            }
            head = mid + 1;
        }
        else
        {
            tail = mid;
        }
    }
    // tail - head == 1 & not matched yet
    if( isMatch == false )
    {
        compareLeft = dataList[head].key;
        if( compareKey<T>(keyValue, compareLeft) )
        {
            idx = head+1;
        }
        else{
            idx = head;
        }
    }
    return idx;
}

template<class T>
bool IndexManager::compareKey(T keyValue, T toCompareValue){
    return (keyValue >= toCompareValue);
}

template<class T>
RC IndexManager::splitFixedLeafNode(IXFileHandle &ixfileHandle, int curPageId, int &newPageId, T keyValue, const RID &rid, T &upwardKey, void *curPage, void *newPage){
    RecordMinLen slotCount;
    memcpy( &slotCount, (char*)curPage+getIndexSlotCountOffset(), sizeof(RecordMinLen) );
    
    // copy all data from curPage 
    RecordMinLen mid = slotCount/2;
    LEAFNODE<T> dataList[slotCount];
    memcpy( dataList, curPage, sizeof(LEAFNODE<T>)*slotCount );

    LEAFNODE<T> midNode = dataList[mid];
    
    // update upward key
    upwardKey = midNode.key;

    bool insertCurPage = true;
    if( keyValue >= midNode.key )
    {
        insertCurPage = false;
    }

    // new page slot count
    RecordMinLen newSlotCount = (slotCount - mid);
    int moveSize = newSlotCount*sizeof(LEAFNODE<T>);
    RecordMinLen newFreeSize = PAGE_SIZE - getLeafNodeDirSize() - moveSize;
    RecordMinLen NodeType = LEAF_NODE;

    RecordMinLen slotInfos[3];
    slotInfos[2] = newFreeSize;
    slotInfos[1] = newSlotCount;
    slotInfos[0] = NodeType;
    // move slotinfos
    memcpy( (char*)newPage+getNodeTypeOffset(), slotInfos, 3*sizeof(RecordMinLen) );
    // copy pointer
    memcpy( (char*)newPage+getLeafNodeRightPointerOffset(), (char*)curPage+getLeafNodeRightPointerOffset(), sizeof(IDX_PAGE_POINTER_TYPE) );

    // move data
    if(moveSize > 0)
    {
        memcpy( newPage, (char*)dataList+mid*sizeof(LEAFNODE<T>), moveSize );

        // clear data in cur page
        memset( (char*)curPage+mid*sizeof(LEAFNODE<T>), 0, moveSize);
    }
    // copy new page id to curPage
    // not yet append 
    IDX_PAGE_POINTER_TYPE curPageRightPointer = ixfileHandle.getNumberOfPages();
    memcpy( (char*)curPage+getLeafNodeRightPointerOffset(), &curPageRightPointer, sizeof(IDX_PAGE_POINTER_TYPE) );

    // update curPage
    slotCount = mid;
    RecordMinLen freeSize;
    memcpy( &freeSize, (char*)curPage+getIndexRestSizeOffset(), sizeof(RecordMinLen) );
    freeSize += moveSize;

    if(insertCurPage)
    {
        insertLeafNode<T>(keyValue, rid, curPage, slotCount);
        slotCount += 1;
        freeSize -= sizeof(LEAFNODE<T>);

    }
    else
    {
        insertLeafNode<T>(keyValue, rid, newPage, slotCount);
        newSlotCount += 1;
        newFreeSize -= sizeof(LEAFNODE<T>);
        slotInfos[2] = newFreeSize;
        slotInfos[1] = newSlotCount;
        // move slotinfos
        memcpy( (char*)newPage+getNodeTypeOffset(), slotInfos, 3*sizeof(RecordMinLen) );
    }
    // update curPage
    slotInfos[2] = freeSize;
    slotInfos[1] = slotCount;
    memcpy( (char*)curPage+getNodeTypeOffset(), slotInfos, 3*sizeof(RecordMinLen) );

    // write page
    ixfileHandle.appendPage(newPage);
    ixfileHandle.writePage( curPageId, curPage );
    newPageId = curPageRightPointer;
    return 0;
}

// insert new root page
template<class T>
RC IndexManager::insertRootPage(IXFileHandle &ixfileHandle, int leftPagePointer, int rightPagePointer, int &newRootPageId, T upwardKey, void *newRootPage){
    RecordMinLen freeSize = PAGE_SIZE - getAuxSlotsSize() - getFixedIndexSize() - 2*sizeof(IDX_PAGE_POINTER_TYPE);
    RecordMinLen slotCount = 1;
    RecordMinLen nodeType = ROOT_NODE;

    // update aux slots
    memset(newRootPage, 0, PAGE_SIZE);
    RecordMinLen slotInfos[3];
    slotInfos[0] = nodeType;
    slotInfos[1] = slotCount;
    slotInfos[2] = freeSize;
    memcpy( (char*)newRootPage+getNodeTypeOffset(), slotInfos, getAuxSlotsSize() );

    IDX_PAGE_POINTER_TYPE leftPointer = leftPagePointer;
    IDX_PAGE_POINTER_TYPE rightPointer = rightPagePointer;
    memcpy( (char*)newRootPage, &leftPointer, sizeof(IDX_PAGE_POINTER_TYPE) );
    memcpy( (char*)newRootPage+getFixedKeyInsertOffset(0), &upwardKey, getFixedIndexSize() );
    memcpy( (char*)newRootPage+getFixedKeyPointerOffset(1), &rightPointer, sizeof(IDX_PAGE_POINTER_TYPE) );

    ixfileHandle.appendPage(newRootPage);
    newRootPageId = ixfileHandle.getNumberOfPages() - 1;

    return 0;
}

template<class T>
RC IndexManager::splitFixedIntermediateNode(IXFileHandle &ixfileHandle, int curPageId, int insertIdx, T &upwardKey, IDX_PAGE_POINTER_TYPE &rightPointer, void *curPage, void *newPage){
    RecordMinLen freeSize;
    RecordMinLen slotCount;
    memcpy( &freeSize, (char*)curPage+getIndexRestSizeOffset(), sizeof(RecordMinLen) );
    memcpy( &slotCount, (char*)curPage+getIndexSlotCountOffset(),sizeof(RecordMinLen) );
    
    IDX_PAGE_POINTER_TYPE upwardRightPointer = rightPointer;
    unsigned tmpDataSize = (slotCount+1)*getInterNodeSize() + sizeof(IDX_PAGE_POINTER_TYPE);
    void *tmpData = malloc(tmpDataSize);

    memcpy( tmpData, curPage, (tmpDataSize -getInterNodeSize()) );

    // move tmpData
    int moveSize = (slotCount - insertIdx)*getInterNodeSize();
    //
    if( moveSize > 0)
    {
        memmove( (char*)tmpData+getFixedKeyPointerOffset(insertIdx+1)+sizeof(IDX_PAGE_POINTER_TYPE), (char*)tmpData+getFixedKeyPointerOffset(insertIdx)+sizeof(IDX_PAGE_POINTER_TYPE), moveSize );
    }
    // insert new key
    memcpy( (char*)tmpData+getFixedKeyPointerOffset(insertIdx)+sizeof(IDX_PAGE_POINTER_TYPE), &upwardKey, getFixedIndexSize() );
    memcpy( (char*)tmpData+getFixedKeyPointerOffset(insertIdx)+sizeof(IDX_PAGE_POINTER_TYPE)+getFixedIndexSize(), &upwardRightPointer, sizeof(IDX_PAGE_POINTER_TYPE) );

    RecordMinLen mid = (slotCount+1)/2;
    
    // get mid index
    T midIndex;
    memcpy( &midIndex, (char*)tmpData+getFixedKeyOffset(mid), getFixedIndexSize() );
    
    // update upwardKey
    upwardKey = midIndex;

    // insert new page
    memset( newPage, 0, PAGE_SIZE );
    // minus all mid
    RecordMinLen newSlotCount = slotCount - mid;
    RecordMinLen newIndexDataSize = newSlotCount*getInterNodeSize() + sizeof(IDX_PAGE_POINTER_TYPE);
    RecordMinLen newFreeSize = PAGE_SIZE - newIndexDataSize - getAuxSlotsSize();
    RecordMinLen newNodeType = INTERMEDIATE_NODE;
    RecordMinLen slotInfos[3];
    slotInfos[0] = newNodeType;
    slotInfos[1] = newSlotCount;
    slotInfos[2] = newFreeSize;
    memcpy( (char*)newPage+getNodeTypeOffset(), slotInfos, getAuxSlotsSize() );
    memcpy( (char*)newPage, (char*)tmpData+getFixedKeyOffset(mid)+getFixedIndexSize(), newIndexDataSize );
    ixfileHandle.appendPage(newPage);

    // update right pointer
    rightPointer = ixfileHandle.getNumberOfPages() - 1;

    // update old page
    freeSize += newSlotCount*getInterNodeSize();
    slotCount -= newSlotCount;
    slotInfos[1] = slotCount;
    slotInfos[2] = freeSize;
    memcpy( (char*)curPage+getNodeTypeOffset(), slotInfos, getAuxSlotsSize() );
    RecordMinLen curIndexDataSize = slotCount*getInterNodeSize() + sizeof(IDX_PAGE_POINTER_TYPE);
    memcpy( curPage, tmpData, curIndexDataSize );
    RecordMinLen resetSize = getNodeTypeOffset() - curIndexDataSize;
    memset( (char*)curPage+curIndexDataSize, 0,  resetSize );

    ixfileHandle.writePage( curPageId, curPage );

    free(tmpData);
    return 0;
}

RC IndexManager::insertVarLengthEntry(IXFileHandle &ixfileHandle, const void *key, const RID &rid){
    RC success = -1;

    int rootPageId = ixfileHandle.rootPageId;
    void *tmpPage = malloc(PAGE_SIZE);

    // empty tree
    if( rootPageId == -1 )
    {
        // insert leaf Node
        memset( tmpPage, 0, PAGE_SIZE );
        if( createVarcharNewLeafNode(tmpPage, key, rid) == 0 )
        {
            ixfileHandle.appendPage(tmpPage);
            // update rootPageId
            ixfileHandle.rootPageId = 0;
            ixfileHandle.treeHeight = 1;
            success = 0;
        }
    }
    else
    {
        ixfileHandle.readPage( rootPageId, tmpPage );
        vector<INDEXPOINTER> traversePointerList;
        int tmpPageId = rootPageId;
        int traverseCallback = traverseVarcharNode(ixfileHandle, key, tmpPageId, tmpPage, traversePointerList);

        if(  traverseCallback == 0 )
        {
            // after traverseVarcharNode we are in the leaf nodes
            int leafPageId = tmpPageId;

            RecordMinLen freeSize;
            memcpy( &freeSize, (char*)tmpPage+getIndexRestSizeOffset(), sizeof(RecordMinLen) );
            int leftSize = freeSize-getVarcharTotalKeySize(key);
            
            // should not split
            if( leftSize >= 0 )
            {
                RecordMinLen slotCount;
                memcpy( &slotCount, (char*)tmpPage+getIndexSlotCountOffset(), sizeof(RecordMinLen) );
                // start get inserted position
                if( insertVarcharLeafNode(key, rid, tmpPage, slotCount) != 0 )
                {
                    success = -1;
                    free(tmpPage);
                    return success;
                }

                // maybe we can save some memcpy
                freeSize = freeSize - getVarcharSize(key) - sizeof(RID) - sizeof(INDEXSLOT);
                memcpy( (char*)tmpPage+getIndexRestSizeOffset(), &freeSize, sizeof(RecordMinLen) );
                slotCount += 1;
                memcpy( (char*)tmpPage+getIndexSlotCountOffset(), &slotCount, sizeof(RecordMinLen) );
                ixfileHandle.writePage(leafPageId, tmpPage);
                success = 0;
            }

            // should split
            else
            {
                void *newPage = malloc(PAGE_SIZE);
                // case 1: height = 1
                if( ixfileHandle.treeHeight == 1 )
                {
                    void *upwardKey;
                    int newPageId;
                    if( splitVarcharLeafNode( ixfileHandle, leafPageId, newPageId, key, rid, &upwardKey, tmpPage, newPage) == 0 )
                    {
                        // insert new root
                        int newRootPageId;
                        if( insertVarcharRootPage(ixfileHandle, leafPageId, newPageId, newRootPageId, upwardKey, tmpPage) == 0 )
                        {
                            ixfileHandle.rootPageId = newRootPageId;
                            ixfileHandle.treeHeight += 1;
                            success = 0;
                        }
                    }
                    free(upwardKey);
                }
                // case 2: height > 1
                // else
                {
                    void *upwardKey;
                    int newPageId;
                    if( splitVarcharLeafNode( ixfileHandle, leafPageId, newPageId, key, rid, &upwardKey, tmpPage, newPage) == 0 )
                    {
                        IDX_PAGE_POINTER_TYPE leftPointer = leafPageId;
                        IDX_PAGE_POINTER_TYPE rightPointer = newPageId;

                        // traverse back
                        while( traversePointerList.size() > 0 )
                        {
                            INDEXPOINTER curParentPointer = traversePointerList.back();
                            IDX_PAGE_POINTER_TYPE parentPageNum = curParentPointer.curPageNum;
                            ixfileHandle.readPage( parentPageNum, tmpPage );

                            int charLen = getVarcharSize(upwardKey);
                            RecordMinLen freeSize;
                            memcpy( &freeSize, (char*)tmpPage+getIndexRestSizeOffset(), sizeof(RecordMinLen) );
                            RecordMinLen nodeType;
                            memcpy( &nodeType, (char*)tmpPage+getNodeTypeOffset(), sizeof(RecordMinLen) );
                            RecordMinLen slotCount;
                            memcpy( &slotCount, (char*)tmpPage+getIndexSlotCountOffset(), sizeof(RecordMinLen) );

                            int parentIdx = curParentPointer.indexId;
                            bool isLeft = curParentPointer.left;
                            // left varchar size
                            int leftSize = freeSize - sizeof(INDEXSLOT) - charLen - sizeof(IDX_PAGE_POINTER_TYPE);
                            
                            // can fit in parent node
                            if( leftSize >= 0 )
                            {   
                                int slotDiff = slotCount - parentIdx;

                                unsigned moveStart;
                                INDEXSLOT lastSlot;
                                memcpy( &lastSlot, (char*)tmpPage+getIndexSlotOffset(slotCount-1), sizeof(INDEXSLOT) );
                                INDEXSLOT insertSlot;
                                if( slotDiff > 0 )
                                {    
                                    INDEXSLOT tmpSlot;
                                    memcpy( &tmpSlot, (char*)tmpPage+getIndexSlotOffset(parentIdx), sizeof(INDEXSLOT) );
                                    moveStart = tmpSlot.pageOffset;
                                    unsigned moveDest = moveStart + sizeof(IDX_PAGE_POINTER_TYPE) + charLen;
                                    unsigned moveSize = lastSlot.pageOffset + lastSlot.recordSize + sizeof(IDX_PAGE_POINTER_TYPE) - moveStart;

                                    memmove( (char*)tmpPage+moveDest, (char*)tmpPage+moveStart, moveSize );
                                    // insert slot
                                    insertSlot.pageOffset = moveStart;
                                    insertSlot.recordSize = charLen;

                                    for(int i = parentIdx ; i < slotCount; i++)
                                    {
                                        memcpy( &tmpSlot, (char*)tmpPage+getIndexSlotOffset(i), sizeof(INDEXSLOT) );
                                        tmpSlot.pageOffset += charLen + sizeof(IDX_PAGE_POINTER_TYPE);
                                        memcpy( (char*)tmpPage+getIndexSlotOffset(i), &tmpSlot, sizeof(INDEXSLOT) );
                                    }
                                    // notice memmove is reverse
                                    unsigned slotMoveSize = (slotCount - parentIdx)*sizeof(INDEXSLOT); 
                                    memmove( (char*)tmpPage+getIndexSlotOffset(slotCount), (char*)tmpPage+getIndexSlotOffset(slotCount-1), slotMoveSize );
                                }
                                else
                                {
                                    moveStart = lastSlot.pageOffset + lastSlot.recordSize + sizeof(IDX_PAGE_POINTER_TYPE);
                                    // insert slot
                                    insertSlot.pageOffset = moveStart;
                                    insertSlot.recordSize = charLen;
                                }
                                // insert slot
                                memcpy( (char*)tmpPage+getIndexSlotOffset(parentIdx), &insertSlot, sizeof(INDEXSLOT) );

                                // insert new key
                                memcpy( (char*)tmpPage+moveStart, (char*)upwardKey+sizeof(int), charLen );
                                memcpy( (char*)tmpPage+moveStart+charLen, &rightPointer, sizeof(IDX_PAGE_POINTER_TYPE) );

                                // write page info
                                freeSize = leftSize;
                                slotCount += 1;
                                memcpy( (char*)tmpPage+getIndexRestSizeOffset(), &freeSize, sizeof(RecordMinLen) );
                                memcpy( (char*)tmpPage+getIndexSlotCountOffset(), &slotCount, sizeof(RecordMinLen) );

                                ixfileHandle.writePage( parentPageNum, tmpPage );
                                success = 0;
                                break;
                            }
                            // should continue to split and go up
                            else
                            {
                                if( nodeType == ROOT_NODE )
                                {
                                    if( splitVarcharIntermediateNode(ixfileHandle, parentPageNum, parentIdx, &upwardKey, rightPointer, tmpPage, newPage) != 0 )
                                    {
                                        success = -1;
                                        break;
                                    }

                                    leftPointer = parentPageNum;
                                    // insert new root node
                                    int newRootPageId;
                                    if( insertVarcharRootPage(ixfileHandle, leftPointer, rightPointer, newRootPageId, upwardKey, tmpPage) != 0)
                                    {
                                        success = -1;
                                        break;
                                    }

                                    ixfileHandle.rootPageId = newRootPageId;
                                    ixfileHandle.treeHeight += 1;
                                    success = 0;
                                    break;
                                }
                                else
                                {
                                    if( splitVarcharIntermediateNode(ixfileHandle, parentPageNum, parentIdx, &upwardKey, rightPointer, tmpPage, newPage) != 0 )
                                    {
                                        success = -1;
                                        break;
                                    }
                                    leftPointer = parentPageNum;
                                }
                            }
                            // pop pointer out
                            traversePointerList.pop_back();
                        }
                    } 
                }
                free(newPage);
            }

        }
        // this case should insert new leaf node
        else if( traverseCallback == 1 )
        {
            void *newPage = malloc(PAGE_SIZE);
            createVarcharNewLeafNode(newPage, key, rid);
            ixfileHandle.appendPage(newPage);
            IDX_PAGE_POINTER_TYPE leafPageNum = ixfileHandle.getNumberOfPages() - 1;
            // update intermediate node
            INDEXPOINTER parentPointer = traversePointerList.back();
            // update node
            updateVarcharParentPointer( ixfileHandle, parentPointer, leafPageNum );
            // end update intermediate node
            free(newPage);

            success = 0;
        }
        else
        {
            success = -1;
        }    
    }

    // free
    free(tmpPage);
    
    return success;
}


// varchar insert start 
// create empty leaf node
RC IndexManager::createVarcharNewLeafNode(void *data, const void *key, const RID &rid){
    
    unsigned auxSlotSize = 3*sizeof(RecordMinLen);
    int charLen = getVarcharSize(key);

    RecordMinLen freeSize = PAGE_SIZE - auxSlotSize - sizeof(INDEXSLOT) - charLen - sizeof(IDX_PAGE_POINTER_TYPE);
    RecordMinLen slotCount = 1;
    RecordMinLen NodeType = LEAF_NODE;
    RecordMinLen slotInfos[3];
    slotInfos[2] = freeSize;
    slotInfos[1] = slotCount;
    slotInfos[0] = NodeType;

    IDX_PAGE_POINTER_TYPE rightPointer = -1;

    memcpy( (char*)data+getNodeTypeOffset(), slotInfos, auxSlotSize );
    memcpy( (char*)data+getLeafNodeRightPointerOffset(), &rightPointer, sizeof(IDX_PAGE_POINTER_TYPE) );
    // should not copy char length
    memcpy( data, (char*)key+sizeof(int), charLen );
    memcpy( data+charLen, &rid, sizeof(RID) );
    
    INDEXSLOT slot;
    slot.pageOffset = 0;
    slot.recordSize = charLen;
    memcpy( (char*)data+getIndexSlotOffset(0), &slot, sizeof(INDEXSLOT) );
    return 0;
}

int IndexManager::traverseVarcharNode(IXFileHandle &ixfileHandle, const void *key, int &curPageId, void *idxPage, vector<INDEXPOINTER> &traversePointerList){
    int status = 0;
    RecordMinLen slotCount;
    memcpy( &slotCount, (char*)idxPage+getIndexSlotCountOffset(), sizeof(RecordMinLen) );

    RecordMinLen nodeType;
    memcpy( &nodeType, (char*)idxPage+getNodeTypeOffset(), sizeof(RecordMinLen) );

    while( nodeType != LEAF_NODE )
    {
        INDEXPOINTER childPointer;
        childPointer = searchVarcharIntermediateNode(key, curPageId, idxPage, 0, slotCount);


        traversePointerList.push_back(childPointer);

        IDX_PAGE_POINTER_TYPE pageNum = childPointer.pageNum;
        
        // empty page no leaf node
        if(pageNum == -1)
        {
            status = 1;
            break;
        }
        else
        {
            if( ixfileHandle.readPage( pageNum, idxPage ) != 0 )
            {
                return -1;
            }
            curPageId = pageNum; 
            memcpy( &nodeType, (char*)idxPage+getNodeTypeOffset(), sizeof(RecordMinLen) );
            
        }
    }

    return status;

}

INDEXPOINTER IndexManager::searchVarcharIntermediateNode(const void* key, int curPageId, const void *idxPage, RecordMinLen head, RecordMinLen tail){
    
    // node should larger or equal than keyValue
    void* compareLeft = malloc(PAGE_SIZE);
    void* compareRight = malloc(PAGE_SIZE);
    RecordMinLen mid;
    RecordMinLen slotCount = tail;
    INDEXPOINTER idxPointer;
    IDX_PAGE_POINTER_TYPE pageNum;
    bool isMatch = false;

    // copy keyData
    int charLen = getVarcharSize(key);
    void *keyData = malloc(charLen);
    memcpy( keyData, (char*)key+sizeof(int), charLen );

    while( (tail - head) > 1 )
    {
        mid = (head + tail)/2;
        fetchVarcharIntermediateNodeData( compareLeft, mid, (void*) idxPage);
        // to check it is fit or not
        if( compareVarcharKey(keyData, compareLeft) )
        {
            if( (mid+1) != tail )
            {
                fetchVarcharIntermediateNodeData( compareRight, mid+1, idxPage);

                if( !compareVarcharKey(keyData, compareRight) )
                {
                    isMatch = true;
                    memcpy( &pageNum, (char*)idxPage+getVarcharKeyPointerOffset(mid+1, idxPage), sizeof(IDX_PAGE_POINTER_TYPE) );
                    idxPointer.pageNum = pageNum;
                    idxPointer.indexId = mid + 1;
                    idxPointer.left = 0;
                    break;
                }
            }
            if( mid == (tail - 1) )
            {
                head = mid;
                break;
            }
            head = mid + 1;
        }
        else
        {
            tail = mid;
        }
    }

    // tail - head == 1 & not matched yet
    if( isMatch == false )
    {
        // check only 1 element case
        fetchVarcharIntermediateNodeData( compareLeft, head, idxPage );
        if( compareVarcharKey(keyData, compareLeft) )
        {
            memcpy( &pageNum, (char*)idxPage+getVarcharKeyPointerOffset(head+1, idxPage), sizeof(IDX_PAGE_POINTER_TYPE) );
            idxPointer.pageNum = pageNum;
            idxPointer.indexId = head+1;
            idxPointer.left = 0;
        }
        else
        {
            memcpy( &pageNum, (char*)idxPage+getVarcharKeyPointerOffset(head, idxPage), sizeof(IDX_PAGE_POINTER_TYPE) );
            idxPointer.pageNum = pageNum;
            idxPointer.indexId = head;
            idxPointer.left = 1;
        }

    }

    idxPointer.curPageNum = curPageId;

    // free
    free(compareLeft);
    free(compareRight);

    return idxPointer;
}

bool IndexManager::compareVarcharKey(const void *key, const void* toCompareValue){
    return ( strcmp((const char*)key, (const char*)toCompareValue) >= 0 );
}

RC IndexManager::updateVarcharParentPointer( IXFileHandle &ixfileHandle, INDEXPOINTER indexPointer, IDX_PAGE_POINTER_TYPE pageNum ){
    RC success = 0;
    IDX_PAGE_POINTER_TYPE parentPageNum = indexPointer.curPageNum;
    int parentIndexId = indexPointer.indexId;

    void *tmpPage = malloc(PAGE_SIZE);
    if( ixfileHandle.readPage(parentPageNum, tmpPage) != 0 )
    {
        free(tmpPage);
        return -1;
    }

    memcpy( (char*)tmpPage+getVarcharKeyPointerOffset(parentIndexId, tmpPage), &pageNum, sizeof(IDX_PAGE_POINTER_TYPE) );

    if( ixfileHandle.writePage(parentPageNum, tmpPage) != 0 )
    {
        success = -1;
    }

    free(tmpPage);
    return success;
}

RC IndexManager::insertVarcharLeafNode(const void *key, const RID &rid, void *data, RecordMinLen slotCount){

    int varcharSize = getVarcharSize(key);
    int dataSize = varcharSize + sizeof(RID);

    IDX_PAGE_POINTER_TYPE insertIdx = searchVarcharLeafNode(key, data, slotCount);
    unsigned toMoveSlot = slotCount - insertIdx;
    RecordMinLen pageOffset;
    
    // get last leaf record
    INDEXSLOT lastSlot;
    memcpy( &lastSlot, (char*)data+getIndexSlotOffset(slotCount-1), sizeof(INDEXSLOT) );
    
    if(toMoveSlot > 0)
    {
        INDEXSLOT slot;
        memcpy( &slot, (char*)data+getIndexSlotOffset(insertIdx), sizeof(INDEXSLOT) );
        pageOffset = slot.pageOffset;
        RecordMinLen recordSize = slot.recordSize;

        unsigned toMoveSize = lastSlot.pageOffset + lastSlot.recordSize + sizeof(RID) - pageOffset;
        unsigned moveDestPosition = pageOffset + dataSize;
        memmove( (char*)data+moveDestPosition, (char*)data+pageOffset, toMoveSize );
        
        // move slots larger than inserted point
        updateVarcharRestSlots( insertIdx, dataSize, slotCount, data );
    }
    // insert after last leaf node
    else
    {
        pageOffset = lastSlot.pageOffset+lastSlot.recordSize+sizeof(IDX_PAGE_POINTER_TYPE);
    }

    // copy data notice varchar len
    memcpy( (char*)data+pageOffset, (char*)key+sizeof(int), varcharSize );
    memcpy( (char*)data+pageOffset+varcharSize, &rid, sizeof(RID) );
    
    // insert slot
    INDEXSLOT insertSlot;
    insertSlot.pageOffset = pageOffset;
    insertSlot.recordSize = varcharSize;
    memcpy( (char*)data+getIndexSlotOffset(insertIdx), &insertSlot, sizeof(INDEXSLOT) );

    return 0;
}

void IndexManager::updateVarcharRestSlots( IDX_PAGE_POINTER_TYPE insertIdx, unsigned dataSize, RecordMinLen slotCount, void *data)
{
    INDEXSLOT slot;

    // update slots pageOffset
    for(int idx = insertIdx; idx < slotCount; idx++)
    {
        memcpy( &slot, (char*)data+getIndexSlotOffset(insertIdx), sizeof(INDEXSLOT) );
        slot.pageOffset += dataSize;
        memcpy( (char*)data+getIndexSlotOffset(insertIdx), &slot, sizeof(INDEXSLOT) );
    }

    // shift slots for one slot size
    unsigned movedSlotSize = (slotCount - insertIdx)*sizeof(INDEXSLOT);
    
    //! notice is reverse
    memmove( (char*)data+getIndexSlotOffset(slotCount), (char*)data+getIndexSlotOffset(slotCount-1), movedSlotSize );
}

IDX_PAGE_POINTER_TYPE IndexManager::searchVarcharLeafNode(const void *key, void *data, RecordMinLen slotCount){
    int charLen = getVarcharSize(key);
    int head = 0;
    int tail = slotCount;
    int mid;
    bool isMatch = false;
    // // return idx
    IDX_PAGE_POINTER_TYPE idx;
    void *compareLeft = malloc(PAGE_SIZE);
    void *compareRight = malloc(PAGE_SIZE);

    void *keyData = malloc(charLen);
    memcpy( keyData, (char*)key+sizeof(int), charLen );

    INDEXSLOT slot;

    while( (tail - head) > 1 )
    {
        mid = (head+tail)/2;    
        memcpy( &slot, (char*)data+getIndexSlotOffset(mid), sizeof(INDEXSLOT) );
        memcpy( compareLeft, (char*)data+slot.pageOffset, slot.recordSize );
        //
        if( compareVarcharKey(keyData, compareLeft) )
        {
            if( (mid+1) != tail )
            {
                memcpy( &slot, (char*)data+getIndexSlotOffset(mid+1), sizeof(INDEXSLOT) );
                memcpy( compareRight, (char*)data+slot.pageOffset, slot.recordSize );
                
                if( !compareVarcharKey(keyData, compareRight) )
                {
                    isMatch = true;
                    idx = mid+1;
                    break;
                }
            }
            if( (mid+1) == tail)
            {
                head = mid;
                break;
            }
            head = mid + 1;
        }
        else
        {
            tail = mid;
        }
    }
    // tail - head == 1 & not matched yet
    if( isMatch == false )
    {
        memcpy( &slot, (char*)data+getIndexSlotOffset(head), sizeof(INDEXSLOT) );
        memcpy( compareLeft, (char*)data+slot.pageOffset, slot.recordSize );

        if( compareVarcharKey(keyData, compareLeft) )
        {
            idx = head+1;
        }
        else{
            idx = head;
        }
    }

    free(compareLeft);
    free(compareRight);
    return idx;
}

// split varchar leaf node
RC IndexManager::splitVarcharLeafNode(IXFileHandle &ixfileHandle, int curPageId, int &newPageId, const void *key, const RID &rid, void **upwardKey, void *curPage, void *newPage){
    RecordMinLen slotCount;
    memcpy( &slotCount, (char*)curPage+getIndexSlotCountOffset(), sizeof(RecordMinLen) );
    
    // copy all data from curPage 
    RecordMinLen mid = slotCount/2;
    
    INDEXSLOT slot;
    memcpy( &slot, (char*)curPage+getIndexSlotOffset(mid), sizeof(INDEXSLOT) );
    
    // get slot info
    int charLen = getVarcharSize(key);
    void *keyValue = malloc(charLen);
    memcpy(keyValue, (char*)key+sizeof(int), charLen);

    // update upward key
    // use double pointer
    // upward key should include size
    *upwardKey = malloc(sizeof(int)+slot.recordSize);
    int rSize = slot.recordSize;
    memcpy( *upwardKey, &rSize, sizeof(int) );
    memcpy( (char*)*upwardKey+sizeof(int), (char*)curPage+slot.pageOffset, slot.recordSize );

    bool insertCurPage = true;
    if( compareVarcharKey(keyValue, *upwardKey) )
    {
        insertCurPage = false;
    }

    // new page slot count
    RecordMinLen newSlotCount = (slotCount - mid);
    // should move data
    int moveSize = 0;
    if( newSlotCount > 0 )
    {
        // calculate moveSize
        INDEXSLOT lastSlot;
        memcpy( &lastSlot, (char*)curPage+getIndexSlotOffset(slotCount-1), sizeof(INDEXSLOT) );
        moveSize = lastSlot.pageOffset + lastSlot.recordSize + sizeof(RID) - slot.pageOffset;
    }

    RecordMinLen newFreeSize = PAGE_SIZE - getLeafNodeDirSize() - moveSize;
    RecordMinLen NodeType = LEAF_NODE;

    RecordMinLen slotInfos[3];
    slotInfos[2] = newFreeSize;
    slotInfos[1] = newSlotCount;
    slotInfos[0] = NodeType;
    // move slotinfos
    memcpy( (char*)newPage+getNodeTypeOffset(), slotInfos, 3*sizeof(RecordMinLen) );
    // copy pointer
    memcpy( (char*)newPage+getLeafNodeRightPointerOffset(), (char*)curPage+getLeafNodeRightPointerOffset(), sizeof(IDX_PAGE_POINTER_TYPE) );

    // move data
    if(moveSize > 0)
    {
        memcpy( newPage, (char*)curPage+slot.pageOffset, moveSize );
        // clear data in cur page
        memset( (char*)curPage+slot.pageOffset, 0, moveSize);
        
        // move slots
        memcpy( (char*)newPage+getIndexSlotOffset(newSlotCount-1) , (char*)curPage+getIndexSlotOffset(slotCount-1), sizeof(INDEXSLOT)*newSlotCount );
        // update pageOffset
        INDEXSLOT tmpSlot;
        for(int i=0; i<newSlotCount;i++)
        {
            memcpy( &tmpSlot, (char*)newPage+getIndexSlotOffset(i), sizeof(INDEXSLOT) );
            ////here
            tmpSlot.pageOffset -= slot.pageOffset;
            memcpy( (char*)newPage+getIndexSlotOffset(i), &tmpSlot, sizeof(INDEXSLOT) );
        }

    }

    // copy new page id to curPage
    // not yet append 
    IDX_PAGE_POINTER_TYPE curPageRightPointer = ixfileHandle.getNumberOfPages();
    memcpy( (char*)curPage+getLeafNodeRightPointerOffset(), &curPageRightPointer, sizeof(IDX_PAGE_POINTER_TYPE) );

    // update curPage
    slotCount = mid;
    RecordMinLen freeSize;
    memcpy( &freeSize, (char*)curPage+getIndexRestSizeOffset(), sizeof(RecordMinLen) );
    
    // should minus the slot removed 
    freeSize += moveSize + sizeof(INDEXSLOT)*newSlotCount;

    if(insertCurPage)
    {
        insertVarcharLeafNode(key, rid, curPage, slotCount);
        slotCount += 1;
        freeSize -= charLen + sizeof(INDEXSLOT);

    }
    else
    {
        insertVarcharLeafNode(key, rid, newPage, slotCount);
        newSlotCount += 1;
        newFreeSize -= charLen + sizeof(INDEXSLOT);
        slotInfos[2] = newFreeSize;
        slotInfos[1] = newSlotCount;
        // move slotinfos
        memcpy( (char*)newPage+getNodeTypeOffset(), slotInfos, 3*sizeof(RecordMinLen) );
    }
    // update curPage
    slotInfos[2] = freeSize;
    slotInfos[1] = slotCount;
    memcpy( (char*)curPage+getNodeTypeOffset(), slotInfos, 3*sizeof(RecordMinLen) );

    // write page
    ixfileHandle.appendPage(newPage);
    ixfileHandle.writePage( curPageId, curPage );
    newPageId = curPageRightPointer;

    free(keyValue);
    return 0;
}

RC IndexManager::insertVarcharRootPage(IXFileHandle &ixfileHandle, int leftPagePointer, int rightPagePointer, int &newRootPageId, void *upwardKey, void *newRootPage){
    int charLen = getVarcharSize(upwardKey);
    void *keyValue = malloc(charLen);
    memcpy( keyValue, (char*)upwardKey+sizeof(int), charLen );

    RecordMinLen freeSize = PAGE_SIZE - getAuxSlotsSize() - charLen - sizeof(int) - 2*sizeof(IDX_PAGE_POINTER_TYPE);
    RecordMinLen slotCount = 1;
    RecordMinLen nodeType = ROOT_NODE;

    // update aux slots
    memset(newRootPage, 0, PAGE_SIZE);
    RecordMinLen slotInfos[3];
    slotInfos[0] = nodeType;
    slotInfos[1] = slotCount;
    slotInfos[2] = freeSize;
    memcpy( (char*)newRootPage+getNodeTypeOffset(), slotInfos, getAuxSlotsSize() );

    IDX_PAGE_POINTER_TYPE leftPointer = leftPagePointer;
    IDX_PAGE_POINTER_TYPE rightPointer = rightPagePointer;
    memcpy( (char*)newRootPage, &leftPointer, sizeof(IDX_PAGE_POINTER_TYPE) );
    memcpy( (char*)newRootPage+sizeof(IDX_PAGE_POINTER_TYPE), keyValue, charLen );
    memcpy( (char*)newRootPage+sizeof(IDX_PAGE_POINTER_TYPE)+charLen, &rightPointer, sizeof(IDX_PAGE_POINTER_TYPE) );

    // insert slot
    INDEXSLOT slot;
    slot.pageOffset = sizeof(IDX_PAGE_POINTER_TYPE);
    slot.recordSize = (RecordMinLen) charLen;
    memcpy( (char*)newRootPage+getIndexSlotOffset(0), &slot, sizeof(INDEXSLOT) );

    ixfileHandle.appendPage(newRootPage);
    newRootPageId = ixfileHandle.getNumberOfPages() - 1;

    free(keyValue);
    return 0;
}

RC IndexManager::splitVarcharIntermediateNode(IXFileHandle &ixfileHandle, int curPageId, int insertIdx, void **upwardKey, IDX_PAGE_POINTER_TYPE &rightPointer, void *curPage, void *newPage){
    RecordMinLen freeSize;
    RecordMinLen slotCount;
    memcpy( &freeSize, (char*)curPage+getIndexRestSizeOffset(), sizeof(RecordMinLen) );
    memcpy( &slotCount, (char*)curPage+getIndexSlotCountOffset(),sizeof(RecordMinLen) );
    
    int insertKeyLen = getVarcharSize(*upwardKey);
    void *insertData = malloc(insertKeyLen);
    memcpy( insertData, (char*)(*upwardKey)+sizeof(int), insertKeyLen );

    IDX_PAGE_POINTER_TYPE upwardRightPointer = rightPointer;

    // this is merged data
    INDEXSLOT lastSlot;
    memcpy( &lastSlot, (char*)curPage+getIndexSlotOffset(slotCount-1), sizeof(INDEXSLOT) );

    unsigned tmpDataSize = lastSlot.pageOffset + lastSlot.recordSize + insertKeyLen + 2*sizeof(IDX_PAGE_POINTER_TYPE);
    void *tmpData = malloc(tmpDataSize);

    memcpy( tmpData, curPage, (tmpDataSize - insertKeyLen - sizeof(IDX_PAGE_POINTER_TYPE)) );

    // move tmpData
    int slotDiff = slotCount - insertIdx;

    unsigned moveStart;
    
    INDEXSLOT insertSlot;

    if( slotDiff > 0 )
    {    
        INDEXSLOT tmpSlot;
        memcpy( &tmpSlot, (char*)tmpData+getIndexSlotOffset(insertIdx), sizeof(INDEXSLOT) );
        moveStart = tmpSlot.pageOffset;
        unsigned moveDest = moveStart + sizeof(IDX_PAGE_POINTER_TYPE) + insertKeyLen;
        
        
        unsigned moveSize = lastSlot.pageOffset + lastSlot.recordSize + sizeof(IDX_PAGE_POINTER_TYPE) - moveStart;
        memmove( (char*)tmpData+moveDest, (char*)tmpData+moveStart, moveSize );

        // insert slot
        insertSlot.pageOffset = moveStart;
        insertSlot.recordSize = insertKeyLen;

        for(int i = insertIdx ; i < slotCount; i++)
        {
            memcpy( &tmpSlot, (char*)tmpData+getIndexSlotOffset(i), sizeof(INDEXSLOT) );
            tmpSlot.pageOffset += insertKeyLen + sizeof(IDX_PAGE_POINTER_TYPE);
            memcpy( (char*)tmpData+getIndexSlotOffset(i), &tmpSlot, sizeof(INDEXSLOT) );
        }

        // notice memmove is reverse
        unsigned slotMoveSize = (slotCount - insertKeyLen)*sizeof(INDEXSLOT);
        memmove( (char*)tmpData+getIndexSlotOffset(slotCount), (char*)tmpData+getIndexSlotOffset(slotCount-1), slotMoveSize );
    }
    else
    {
        moveStart = lastSlot.pageOffset + lastSlot.recordSize + sizeof(IDX_PAGE_POINTER_TYPE);
        insertSlot.pageOffset = moveStart;
        insertSlot.recordSize = insertKeyLen;

    }
    // insert new key
    memcpy( (char*)tmpData+moveStart, (char*)upwardKey+sizeof(int), insertKeyLen );
    memcpy( (char*)tmpData+moveStart+insertKeyLen, &rightPointer, sizeof(IDX_PAGE_POINTER_TYPE) );
    
    // should also insert slot
    memcpy( (char*)tmpData+getIndexSlotOffset(insertIdx), &insertSlot, sizeof(INDEXSLOT) );

    RecordMinLen mid = (slotCount+1)/2;
    
    // get mid index
    INDEXSLOT midSlot;
    memcpy( &midSlot, (char*)tmpData+getIndexSlotOffset(mid), sizeof(INDEXSLOT) );

    // free current upwardKey
    free(*upwardKey);
    int newUpwardKeySize = sizeof(int) + midSlot.recordSize;
    
    // update upwardKey
    *upwardKey = malloc(newUpwardKeySize);
    int newUpwardDataSize = midSlot.recordSize;
    memcpy( (char*)(*upwardKey), &newUpwardKeySize, sizeof(int) );
    memcpy( (char*)(*upwardKey)+sizeof(int), (char*)tmpData+midSlot.pageOffset, newUpwardDataSize );

    // insert new page
    memset( newPage, 0, PAGE_SIZE );
    
    // minus all mid
    RecordMinLen newSlotCount = slotCount - mid;
    
    INDEXSLOT nextSlot;
    memcpy( &nextSlot, (char*)tmpData+getIndexSlotOffset(mid+1), sizeof(INDEXSLOT) );
    memcpy( &lastSlot, (char*)tmpData+getIndexSlotOffset(slotCount), sizeof(INDEXSLOT) );

    RecordMinLen newIndexDataSize = lastSlot.pageOffset + lastSlot.recordSize - nextSlot.pageOffset + sizeof(IDX_PAGE_POINTER_TYPE);
    
    RecordMinLen newFreeSize = PAGE_SIZE - newIndexDataSize - getAuxSlotsSize() - newSlotCount*sizeof(IDX_PAGE_POINTER_TYPE);
    RecordMinLen newNodeType = INTERMEDIATE_NODE;
    RecordMinLen slotInfos[3];
    slotInfos[0] = newNodeType;
    slotInfos[1] = newSlotCount;
    slotInfos[2] = newFreeSize;
    memcpy( (char*)newPage+getNodeTypeOffset(), slotInfos, getAuxSlotsSize() );
    // move data should skip mid node
    memcpy( (char*)newPage, (char*)tmpData+nextSlot.pageOffset, newIndexDataSize );
    // update slot
    memcpy( (char*)newPage+getIndexSlotOffset(newSlotCount-1), (char*)tmpData+getIndexSlotOffset(slotCount), sizeof(INDEXSLOT)*newSlotCount );
    INDEXSLOT tmpSlot;
    for(int i =0; i<newSlotCount;i++)
    {
        memcpy( &tmpSlot, (char*)newPage+getIndexSlotOffset(i), sizeof(INDEXSLOT) );
        tmpSlot.pageOffset -= nextSlot.pageOffset - sizeof(IDX_PAGE_POINTER_TYPE);
        memcpy( (char*)newPage+getIndexSlotOffset(i), &tmpSlot, sizeof(INDEXSLOT) );
    }

    ixfileHandle.appendPage(newPage);

    // update right pointer
    rightPointer = ixfileHandle.getNumberOfPages() - 1;

    // update old page
    slotCount -= newSlotCount;
    RecordMinLen curIndexDataSize = tmpDataSize - newIndexDataSize - midSlot.recordSize;
    freeSize = slotCount*sizeof(INDEXSLOT) + curIndexDataSize;
    slotInfos[1] = slotCount;
    slotInfos[2] = freeSize;
    memcpy( (char*)curPage+getNodeTypeOffset(), slotInfos, getAuxSlotsSize() );
    memcpy( curPage, tmpData, curIndexDataSize );
    RecordMinLen resetSize = getNodeTypeOffset() - curIndexDataSize;
    memset( (char*)curPage+curIndexDataSize, 0,  resetSize );

    ixfileHandle.writePage( curPageId, curPage );

    // free
    free(insertData);
    free(tmpData);
    return 0;
}
// varchar insert end

RC IndexManager::deleteEntry(IXFileHandle &ixfileHandle, const Attribute &attribute, const void *key, const RID &rid)
{
    return -1;
}


RC IndexManager::scan(IXFileHandle &ixfileHandle,
        const Attribute &attribute,
        const void      *lowKey,
        const void      *highKey,
        bool			lowKeyInclusive,
        bool        	highKeyInclusive,
        IX_ScanIterator &ix_ScanIterator)
{
    return -1;
}

void IndexManager::printBtree(IXFileHandle &ixfileHandle, const Attribute &attribute) const {
}


//// start scanIterator ////
IX_ScanIterator::IX_ScanIterator()
{
}

IX_ScanIterator::~IX_ScanIterator()
{
}

RC IX_ScanIterator::getNextEntry(RID &rid, void *key)
{
    return -1;
}

RC IX_ScanIterator::close()
{
    return -1;
}
//// end scanIterator ////


IXFileHandle::IXFileHandle()
{
    ixReadPageCounter = 0;
    ixWritePageCounter = 0;
    ixAppendPageCounter = 0;
    pageCounter = 0;
    treeHeight = 0;
    rootPageId = -1;
}

IXFileHandle::~IXFileHandle()
{
}

RC IXFileHandle::collectCounterValues(unsigned &readPageCount, unsigned &writePageCount, unsigned &appendPageCount)
{
    readPageCount = ixReadPageCounter;
    writePageCount = ixWritePageCounter;
    appendPageCount = ixAppendPageCounter;
    return 0;
}

unsigned IXFileHandle::getNumberOfPages()
{
    return pageCounter;
}


void IXFileHandle::fetchFileData()
{

    // check if there is record exists
    // if so load them
    //the order of integer is pageCounter, readPageCounter, writePageCounter, appendPageCounter end with #
    int hiddenAttrCount = 7;

    _handler->seekg(0,  ios_base::beg);
    unsigned counter[hiddenAttrCount];

    _handler->seekg(0, ios_base::end);
    ios_base::streampos end_pos = _handler->tellg();
    if(end_pos >= PAGE_SIZE)
    {
        _handler->seekg(0, ios_base::beg);
        void* firstPage = malloc(PAGE_SIZE);
        _handler->read( (char*) firstPage, PAGE_SIZE );

        memcpy( (void*)counter, firstPage, sizeof(unsigned)*hiddenAttrCount );
        // free memory
        free(firstPage);
    }
    // init the hidden page
    else
    {
        _handler->seekg(0, ios_base::beg);
        memset( counter, 0, sizeof(unsigned)*hiddenAttrCount );

        void *blankPage = malloc(PAGE_SIZE);
        memcpy( blankPage, (void*)counter, sizeof(unsigned)*hiddenAttrCount );
        memset( (char*)blankPage+sizeof(unsigned)*hiddenAttrCount, 0, PAGE_SIZE-sizeof(unsigned)*hiddenAttrCount );
        _handler->write( (char*) blankPage, PAGE_SIZE );
        free(blankPage);
    }

    pageCounter = counter[0];
    ixReadPageCounter = counter[1];
    ixWritePageCounter = counter[2];
    ixAppendPageCounter = counter[3];
    treeHeight = counter[4];
    // fetch should minus 
    rootPageId = counter[5] - 1;
    indexType = counter[6];
}

void IXFileHandle::saveCounter()
{
    //http://www.cplusplus.com/forum/beginner/30644/
    // back to the first of file pointer
    _handler->clear();
    _handler->seekg(0, ios_base::beg);

    int hiddenAttrCount = 7;
    unsigned counter[hiddenAttrCount];
    counter[0] = pageCounter;
    counter[1] = ixReadPageCounter;
    counter[2] = ixWritePageCounter;
    counter[3] = ixAppendPageCounter;
    counter[4] = treeHeight;
    // save should add one
    counter[5] = rootPageId + 1;
    counter[6] = indexType;
    _handler->write( (char*)counter , sizeof(unsigned)*hiddenAttrCount );
    return;
}

RC IXFileHandle::readPage(PageNum pageNum, void *data)
{
    if( (int)pageNum >= getNumberOfPages() )
    {
        return -1;
    }

    // read from specific position
    // first 4096 are private page
    int page_offset = -(getNumberOfPages() - pageNum)*PAGE_SIZE;
    _handler->seekg(page_offset, ios_base::end);
    _handler->read( (char*) data, PAGE_SIZE );

    ixReadPageCounter += 1;
    return 0;
}


RC IXFileHandle::writePage(PageNum pageNum, const void *data)
{
    if(pageNum >= pageCounter)
    {
//      cout<<"pageNum not existed!"<<endl;
        return -1;
    }
    int page_offset = (pageNum+1)*PAGE_SIZE;
    _handler->clear();
    _handler->seekg(page_offset, ios_base::beg);
    _handler->write((char*) data, PAGE_SIZE);

    ixWritePageCounter += 1;
    return 0;
}


RC IXFileHandle::appendPage(const void *data)
{
    if(_handler == NULL)
    {
        return -1;
    }
    _handler->seekg(0, ios_base::end);
    _handler->write( (char*) data, PAGE_SIZE );

    // add counter
    ixAppendPageCounter += 1;
    pageCounter += 1;

    return 0;
}

// accessary

// get aux slots size
inline unsigned getAuxSlotsSize(){
    return 3*sizeof(RecordMinLen);
}


inline unsigned getIndexSlotOffset(int slotNum) {
    return ( PAGE_SIZE - 3*sizeof(RecordMinLen) - sizeof(INDEXSLOT)*(slotNum + 1) );
}

inline unsigned getIndexRestSizeOffset() {
    return ( PAGE_SIZE - 1*sizeof(RecordMinLen) );
}

inline unsigned getIndexSlotCountOffset() {
    return ( PAGE_SIZE - 2*sizeof(RecordMinLen) );
}

inline unsigned getNodeTypeOffset() {
    return ( PAGE_SIZE - 3*sizeof(RecordMinLen) );
}

inline unsigned getLeafNodeRightPointerOffset() {
    return ( PAGE_SIZE - 3*sizeof(RecordMinLen)-sizeof(IDX_PAGE_POINTER_TYPE) );
}

// the size of each index value of fixed size data type
inline unsigned getFixedIndexSize(){
    return (4);
}
// get intermediate index/pointer pair size
inline unsigned getInterNodeSize(){
    return ( getFixedIndexSize() + sizeof(IDX_PAGE_POINTER_TYPE) );
}
// intermediate node
inline unsigned getFixedKeyInsertOffset( unsigned idx ){
    return ( sizeof(IDX_PAGE_POINTER_TYPE) + idx*( sizeof(IDX_PAGE_POINTER_TYPE) + getFixedIndexSize() ) ); 
}

// intermediate node 
inline unsigned getFixedKeyOffset( unsigned idx ){
    return (  (idx+1)*( sizeof(IDX_PAGE_POINTER_TYPE) + getFixedIndexSize() ) - getFixedIndexSize() ); 
}
// use pointer index offset |p0| k0 |p1| k1 |p2|
inline unsigned getFixedKeyPointerOffset( unsigned idx ){
	return (  idx*( sizeof(IDX_PAGE_POINTER_TYPE) + getFixedIndexSize() ) );
}
// the size of each insert fixed key
inline unsigned getFixedKeySize(){
    return ( 4 + sizeof(RID) );
}

// the dir size of leaf node
inline unsigned getLeafNodeDirSize(){
    return (3*sizeof(RecordMinLen) + sizeof(IDX_PAGE_POINTER_TYPE));
}

inline int getVarcharSize(const void *key){
    int len;
    memcpy( &len, key, sizeof(int) );
    return len;
}

unsigned getVarcharKeySize(const void *key)
{
    return ( getVarcharSize(key) + sizeof(int) );
}
unsigned getVarcharTotalKeySize(const void *key)
{
    return ( getVarcharKeySize(key) + sizeof(INDEXSLOT) );
}

void fetchVarcharIntermediateNodeData( void *key, int idx, const void *data ){
    INDEXSLOT slot;
    memcpy( &slot, (char*)data+getIndexSlotOffset(idx), sizeof(INDEXSLOT) );
    RecordMinLen pageOffset = slot.pageOffset;
    RecordMinLen recordSize = slot.recordSize;

    memcpy( key, (char*)data+pageOffset, recordSize );
}

// use pointer index offset |p0| k0 |p1| k1 |p2|
unsigned getVarcharKeyPointerOffset( unsigned idx, const void *data ){
    if( idx == 0 )
    {
        return 0;
    }
    INDEXSLOT slot;
    memcpy( &slot, (char*)data+getIndexSlotOffset(idx-1), sizeof(INDEXSLOT) );
    RecordMinLen pageOffset = slot.pageOffset;
    RecordMinLen recordSize = slot.recordSize;

    return (  pageOffset + recordSize );
}
