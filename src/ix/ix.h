#ifndef _ix_h_
#define _ix_h_

#include <vector>
#include <string>

#include "../rbf/rbfm.h"

# define IX_EOF (-1)  // end of the index scan
# define IDX_PAGE_POINTER_TYPE int

# define ROOT_NODE 0
# define INTERMEDIATE_NODE 1
# define LEAF_NODE 2
# define NO_POINTER -1

# define REAL_TYPE 0
# define INT_TYPE 1
# define CHAR_TYPE 2

typedef struct {
    RecordMinLen pageOffset;
    RecordMinLen recordSize;
} INDEXSLOT;

typedef struct {
    IDX_PAGE_POINTER_TYPE pageNum;
    IDX_PAGE_POINTER_TYPE curPageNum;
    int indexId;
    bool left = 0;
} INDEXPOINTER;

template<typename T>
struct LEAFNODE{
    T key;
    RID rid;
};

class IX_ScanIterator;
class IXFileHandle;

class IndexManager {

    public:
        static IndexManager* instance();

        // Create an index file.
        RC createFile(const string &fileName);

        // Delete an index file.
        RC destroyFile(const string &fileName);

        // Open an index and return an ixfileHandle.
        RC openFile(const string &fileName, IXFileHandle &ixfileHandle);

        // Close an ixfileHandle for an index.
        RC closeFile(IXFileHandle &ixfileHandle);

        // Insert an entry into the given index that is indicated by the given ixfileHandle.
        RC insertEntry(IXFileHandle &ixfileHandle, const Attribute &attribute, const void *key, const RID &rid);
        template<class T>
        RC insertFixedLengthEntry(IXFileHandle &ixfileHandle, const void *key, const RID &rid);
        RC insertVarLengthEntry(IXFileHandle &ixfileHandle, const void *key, const RID &rid);

        // Delete an entry from the given index that is indicated by the given ixfileHandle.
        RC deleteEntry(IXFileHandle &ixfileHandle, const Attribute &attribute, const void *key, const RID &rid);

        // Initialize and IX_ScanIterator to support a range search
        RC scan(IXFileHandle &ixfileHandle,
                const Attribute &attribute,
                const void *lowKey,
                const void *highKey,
                bool lowKeyInclusive,
                bool highKeyInclusive,
                IX_ScanIterator &ix_ScanIterator);

        // Print the B+ tree in pre-order (in a JSON record format)
        void printBtree(IXFileHandle &ixfileHandle, const Attribute &attribute);
        void printVarcharBtree(IXFileHandle &ixfileHandle, int pageId, int indent);
        template<class T>
        void printFixedBtree(IXFileHandle &ixfileHandle, int pageId, int indent);

        template<class T>
        INDEXPOINTER searchFixedIntermediateNode(T keyValue, int curPageId, const void *idxPage, RecordMinLen head, RecordMinLen tail);
        template<class T>
        RC insertLeafNode(T keyValue, const RID &rid, void *data, RecordMinLen slotCount);
        template<class T>
        IDX_PAGE_POINTER_TYPE searchFixedLeafNode(T keyValue, void *data, RecordMinLen slotCount);

        template<class T>
        RC splitFixedLeafNode(IXFileHandle &ixfileHandle, int curPageId, int &newPageId, T keyValue, const RID &rid, T &upwardKey, void *curPage, void *newPage);

        template<class T>
        RC insertRootPage(IXFileHandle &ixfileHandle, int leftPagePointer, int rightPagePointer, int &newRootPageId, T upwardKey, void *newRootPage);

        template<class T>
        RC splitFixedIntermediateNode(IXFileHandle &ixfileHandle, int curPageId, int insertIdx, T &upwardKey, IDX_PAGE_POINTER_TYPE &rightPointer, void *curPage, void *newPage);

        // here are for varchar insert
        RC createVarcharNewLeafNode(void *data, const void *key, const RID &rid);
        INDEXPOINTER searchVarcharIntermediateNode(const void* key, int curPageId, const void *idxPage, RecordMinLen head, RecordMinLen tail);

        RC insertVarcharLeafNode(const void *key, const RID &rid, void *data, RecordMinLen slotCount);
        void updateVarcharRestSlots(IDX_PAGE_POINTER_TYPE insertIdx, unsigned dataSize, RecordMinLen slotCount, void *data);

        IDX_PAGE_POINTER_TYPE searchVarcharLeafNode(const void *key, void *data, RecordMinLen slotCount);

        RC splitVarcharLeafNode(IXFileHandle &ixfileHandle, int curPageId, int &newPageId, const void *key, const RID &rid, void **upwardKey, void *curPage, void *newPage);
        RC insertVarcharRootPage(IXFileHandle &ixfileHandle, int leftPagePointer, int rightPagePointer, int &newRootPageId, void *upwardKey, void *newRootPage);

        RC splitVarcharIntermediateNode(IXFileHandle &ixfileHandle, int curPageId, int insertIdx, void **upwardKey, IDX_PAGE_POINTER_TYPE &rightPointer, void *curPage, void *newPage);

        template<class T>
        bool compareKey(T keyValue, T toCompareValue);

        bool compareVarcharKey(const void *key, const void *toCompareValue);

    protected:
        IndexManager();
        ~IndexManager();

    private:
        static IndexManager *_index_manager;
        bool _fileExist = false;

        RC createFixedNewLeafNode(void *data, const void *key, const RID &rid);
        template<class T>
        int traverseFixedLengthNode(IXFileHandle &ixfileHandle, T keyValue, int &curPageId, void *idxPage, vector<INDEXPOINTER> &traversePointerList);

        int traverseVarcharNode(IXFileHandle &ixfileHandle, const void *key, int &curPageId, void *idxPage, vector<INDEXPOINTER> &traversePointerList);

        RC updateParentPointer( IXFileHandle &ixfileHandle, INDEXPOINTER indexPointer, IDX_PAGE_POINTER_TYPE pageNum );
        RC updateVarcharParentPointer( IXFileHandle &ixfileHandle, INDEXPOINTER indexPointer, IDX_PAGE_POINTER_TYPE pageNum );


};


class IX_ScanIterator {
    public:

		// Constructor
        IX_ScanIterator();

        // Destructor
        ~IX_ScanIterator();

        // Get next matching entry
        RC getNextEntry(RID &rid, void *key);

        // Terminate index scan
        RC close();

        IXFileHandle *_ixfileHandlePtr;
        void *_highKey;
        void *_lowKey;
        Attribute _attribute;
        bool _lowKeyInclusive;
        bool _highKeyInclusive;
        void *_tmpPage;

        IndexManager *_indexManager;


        // note current interslot
        bool _isStart;
        int _pageId;
        int _slotNum;
};



class IXFileHandle {
    public:

        // variables to keep counter for each operation
        unsigned ixReadPageCounter;
        unsigned ixWritePageCounter;
        unsigned ixAppendPageCounter;
        unsigned pageCounter;
        unsigned treeHeight;
        int rootPageId;
        unsigned indexType;

        fstream* _handler = NULL;

        // Constructor
        IXFileHandle();

        // Destructor
        ~IXFileHandle();

        // Read/Write/Append
        RC readPage(PageNum pageNum, void *data);
        RC writePage(PageNum pageNum, const void *data);
        RC appendPage(const void *data);

    	// Put the current counter values of associated PF FileHandles into variables
    	RC collectCounterValues(unsigned &readPageCount, unsigned &writePageCount, unsigned &appendPageCount);

        unsigned getNumberOfPages();
        void fetchFileData();
        void saveCounter();
};

// aux slots size
inline unsigned getAuxSlotsSize();

inline unsigned getIndexLeafSlotOffset(int slotNum);
inline unsigned getIndexSlotOffset(int slotNum);
inline unsigned getIndexRestSizeOffset();
inline unsigned getIndexSlotCountOffset();
inline unsigned getNodeTypeOffset();

inline unsigned getLeafNodeRightPointerOffset();


// the size of each index value of fixed size data type
inline unsigned getFixedIndexSize();

// get intermediate (index,pointer) pair size
inline unsigned getInterNodeSize();

// for tree offset
inline unsigned getFixedKeyInsertOffset( unsigned idx );

// intermediate node
inline unsigned getFixedKeyOffset( unsigned idx );

// use pointer index offset |p0| k0 |p1| k1 |p2|
inline unsigned getFixedKeyPointerOffset( unsigned idx );

// the size of each insert fixed key
inline unsigned getFixedKeySize();

// the dir size of leaf node
inline unsigned getLeafNodeDirSize();

inline unsigned getFixedLeafNodeOffset(unsigned idx);

// varchar insert accessary functions
inline int getVarcharSize(const void *key);
void fetchVarcharIntermediateNodeData( void *key, int idx, const void *data );
unsigned getVarcharKeyPointerOffset( unsigned idx, const void *data );

// leaf node key size
unsigned getVarcharKeySize(const void *key);
// with sizeof(INDEXSLOT)
unsigned getVarcharTotalKeySize(const void *key);

// print indent
inline void printIndent(int indent);
inline void printRid(RID rid);

#endif
