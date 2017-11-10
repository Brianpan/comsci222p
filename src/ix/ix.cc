
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
    if( idxAttrType == TypeReal || idxAttrType == TypeInt )
    {
        return insertFixedLengthEntry( ixfileHandle, idxAttrType, key, rid );
    }
    else
    {
        return insertVarLengthEntry( ixfileHandle, key, rid );
    }
    
}

RC IndexManager::insertFixedLengthEntry(IXFileHandle &ixfileHandle, AttrType idxAttrType, const void *key, const RID &rid){
    RC success = -1;

    if(idxAttrType == TypeReal)
    {
        float keyValue;
        memcpy( &keyValue, key, sizeof(float) );
    }
    else
    {
        int keyValue;
        memcpy( &keyValue, key, sizeof(int) );
    }

    int rootPageId = ixfileHandle.rootPageId;
    void *tmpPage = malloc(PAGE_SIZE);

    // empty tree
    if( rootPageId == -1 )
    {
        // 4 is sizeof(int)/sizeof(float)
        unsigned auxSlotSize = 3*sizeof(RecordMinLen);
        RecordMinLen freeSize = PAGE_SIZE - auxSlotSize - 4 - 2*sizeof(IDX_PAGE_POINTER_TYPE);
        RecordMinLen slotCount = 1;
        RecordMinLen NodeType = ROOT_NODE;
        RecordMinLen slotInfos[3];
        slotInfos[0] = freeSize;
        slotInfos[1] = slotCount;
        slotInfos[2] = NodeType;
        mecpy( (char*)tmpPage+getNodeTypeOffset(), slotInfos, auxSlotSize );

        // insert first Node
        IDX_PAGE_POINTER_TYPE leftPointer = NO_POINTER;
        // right pointer is page 1
        IDX_PAGE_POINTER_TYPE rightPointer = 1;
        memcpy( (char*)tmpPage, &leftPointer, sizeof(IDX_PAGE_POINTER_TYPE) );
        memcpy( (char*)tmpPage+getFixedKeyInsertOffset(0), &keyValue, 4 );
        memcpy( (char*)tmpPage+getFixedKeyInsertOffset(0)+4, &rightPointer, sizeof(IDX_PAGE_POINTER_TYPE) );
        
        // update rootPageId
        ixfileHandle.rootPageId = 0;
        // append page
        ixfileHandle.appendPage(tmpPage);

        // insert leaf Node
        memset( tmpPage, 0, PAGE_SIZE );
        if( createFixedNewLeafNode(tmpPage, key) == 0 )
        {
            ixfileHandle.appendPage(tmpPage);
            success = 0;
        }
       
        // free
        free(tmpPage);
    }
    else
    {
        ixfileHandle.readPage( rootPageId, tmpPage );
        vector<IDX_PAGE_POINTER_TYPE> traversePointerList;
        
    }

    return success;
}

// create empty leaf node
RC IndexManager::createFixedNewLeafNode(void *data, const void *key){
    IDX_PAGE_POINTER_TYPE rightPointer = -1;
    memcpy( (char*)data+getLeafNodeRightPointerOffset(), &rightPointer, sizeof(IDX_PAGE_POINTER_TYPE) );
    memcpy( data, key, 4 );
    return 0;
}

RC IndexManger::insertVarLengthEntry(IXFileHandle &ixfileHandle, const coid *key, const RID &rid){
    return -1;
}

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
    int hiddenAttrCount = 6;

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
}

void IXFileHandle::saveCounter()
{
    //http://www.cplusplus.com/forum/beginner/30644/
    // back to the first of file pointer
    _handler->clear();
    _handler->seekg(0, ios_base::beg);

    int hiddenAttrCount = 6;
    unsigned counter[hiddenAttrCount];
    counter[0] = pageCounter;
    counter[1] = ixReadPageCounter;
    counter[2] = ixWritePageCounter;
    counter[3] = ixAppendPageCounter;
    counter[4] = treeHeight;
    // save should add one
    counter[5] = rootPageId + 1;

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

inline unsigned getFixedKeyInsertOffset( unsigned idx ){
    return ( sizeof(IDX_PAGE_POINTER_TYPE) + idx*( sizeof(IDX_PAGE_POINTER_TYPE) + 4 ) ); 
}