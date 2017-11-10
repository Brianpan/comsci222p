#ifndef _ix_h_
#define _ix_h_

#include <vector>
#include <string>

#include "../rbf/rbfm.h"

# define IX_EOF (-1)  // end of the index scan
# define IDX_PAGE_POINTER_TYPE int;

# define ROOT_NODE 0
# define INTERMEDIATE_NODE 1
# define LEAF_NODE 2
# define NO_POINTER -1

typedef struct {
    RecordMinLen pageOffset;
    RecordMinLen recordSize;
} INDEXSLOT;

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
        RC insertFixedLengthEntry(IXFileHandle &ixfileHandle, AttrType idxAttrType, const void *key, const RID &rid);
        RC insertVarLengthEntry(IXFileHandle &ixfileHandle, const coid *key, const RID &rid);

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
        void printBtree(IXFileHandle &ixfileHandle, const Attribute &attribute) const;

    protected:
        IndexManager();
        ~IndexManager();

    private:
        static IndexManager *_index_manager;

        RC createFixedNewLeafNode(void *data, const void *key);
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

inline unsigned getIndexSlotOffset(int slotNum);
inline unsigned getIndexRestSizeOffset();
inline unsigned getIndexSlotCountOffset();
inline unsigned getNodeTypeOffset();

inline unsigned getLeafNodeRightPointerOffset();
// for tree offset
inline unsigned getFixedKeyInsertOffset( unsigned idx );
#endif
