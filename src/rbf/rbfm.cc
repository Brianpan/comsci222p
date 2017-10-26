#include "rbfm.h"

RecordBasedFileManager* RecordBasedFileManager::_rbf_manager = 0;

RecordBasedFileManager* RecordBasedFileManager::instance()
{
    if(!_rbf_manager)
        _rbf_manager = new RecordBasedFileManager();

    return _rbf_manager;
}

RecordBasedFileManager::RecordBasedFileManager()
{
	 _pf_manager = PagedFileManager::instance();
}

RecordBasedFileManager::~RecordBasedFileManager()
{
}

RC RecordBasedFileManager::createFile(const string &fileName) {
   return _pf_manager->createFile(fileName);

}

RC RecordBasedFileManager::destroyFile(const string &fileName) {
	return _pf_manager->destroyFile(fileName);
}

RC RecordBasedFileManager::openFile(const string &fileName, FileHandle &fileHandle) {
    return _pf_manager->openFile(fileName, fileHandle);
}

RC RecordBasedFileManager::closeFile(FileHandle &fileHandle) {
	return _pf_manager->closeFile(fileHandle);
}

RC RecordBasedFileManager::insertRecord(FileHandle &fileHandle, const vector<Attribute> &recordDescriptor, const void *data, RID &rid) {
    // check which page to insert into first
	bool shouldAppendPage = true;
	int pageNum;

	//prepare record
	int recordSize = recordDescriptor.size();
	RecordMinLen recordAttrCount = recordSize;

	// memcpy data to recordData
	unsigned int localOffset = 0;
	void *recordData = NULL;
	// use pointer to pointer for malloc in function
	if( prepareRecord( recordDescriptor, localOffset, &recordData, data ) != 0 )
	{
		return -1;
	}

	// find a page or append page
	// store tmpPage
	void *tmpPage = malloc(PAGE_SIZE);

	int page_id = fileHandle.getNumberOfPages() - 1;
	if( page_id >= 0 && fileHandle.readPage(page_id, tmpPage) == 0 )
	{
		//get the last bool
		RecordMinLen remainSize;
		memcpy( (void*) &remainSize, (void*)tmpPage + getRestSizeOffset() , sizeof(RecordMinLen) );
		// this page is full continue
		// should add slot size to record size
		if( remainSize >= localOffset + sizeof(DIRECTORYSLOT) )
		{
			// select pageNum
			shouldAppendPage = false;
			pageNum = page_id;
		}
	}

	// check page
	if( (page_id -1) >=0 && shouldAppendPage )
	{
		for( int i=0; i<page_id;i++ )
		{
			if( fileHandle.readPage(i, tmpPage) == 0 )
			{
				RecordMinLen remainSize;
				memcpy( (void*) &remainSize, (void*)tmpPage + getRestSizeOffset() , sizeof(RecordMinLen) );
				if( remainSize >= localOffset + sizeof(DIRECTORYSLOT) )
				{
					// select pageNum
					shouldAppendPage = false;
					pageNum = i;
					break;
				}
			}
		}
	}

	bool shouldUpdatePage = false;
	// insert data into new page
	if( shouldAppendPage )
	{
		// init empty page
		memset(tmpPage, 0, PAGE_SIZE);
		// create slot
		DIRECTORYSLOT *slot = (DIRECTORYSLOT *) malloc(sizeof(DIRECTORYSLOT));
		slot->recordSize = localOffset;
		slot->pageOffset = 0;
		// add slot type
		slot->slotType = Normal;
		// create for storing rest of the bytes
		// 3 * 2bytes (1 for note rest of bytes, one for number of slot, one for deletedPointer)
		unsigned int directorySize = getDirectorySize();
		RecordMinLen restSize = PAGE_SIZE - directorySize - localOffset;
		RecordMinLen slotSize = 1;
		RecordMinLen deletedPointer = -1;
		void *directory = malloc(directorySize);
		int tmpOffset = 0;
		memcpy( directory + tmpOffset, (void*)slot, sizeof(DIRECTORYSLOT) );
		tmpOffset += sizeof(DIRECTORYSLOT);
		memcpy( directory + tmpOffset,  &deletedPointer, sizeof(RecordMinLen) );
		tmpOffset += sizeof(RecordMinLen);
		memcpy(directory + tmpOffset, &slotSize, sizeof(RecordMinLen) );
		tmpOffset += sizeof(RecordMinLen);
		memcpy( directory + tmpOffset,  &restSize, sizeof(RecordMinLen) );

		//copy directory to tmpPage
		memcpy( tmpPage + (PAGE_SIZE-directorySize), directory, directorySize );
		// copy record to the front of tmpPage
		memcpy( tmpPage, recordData, localOffset );


		// append page
		fileHandle.appendPage(tmpPage);
		rid.pageNum = fileHandle.getNumberOfPages() -1;
		rid.slotNum = 0;

		free(slot);
		free(directory);
	}
	// insert data into existed page
	else
	{
		RecordMinLen restSize;
		RecordMinLen slotSize;
		int tmpOffset;
		// get curr restSize
		memcpy( (void*) &restSize, tmpPage + getRestSizeOffset(), sizeof(RecordMinLen) );
		memcpy( (void*) &slotSize, tmpPage + getSlotCountOffset(), sizeof(RecordMinLen) );

		DIRECTORYSLOT lastSlot;
		memcpy( (void*) &lastSlot, tmpPage + getSlotOffset(slotSize-1), sizeof(DIRECTORYSLOT) );

		//!!! get curr slotSize
		RecordMinLen deletedPointer;
		memcpy( &deletedPointer, tmpPage + getDeletedPointerOffset(), sizeof(RecordMinLen) );
		if( deletedPointer != -1 )
		{
			// use deleted slot
			DIRECTORYSLOT tmpSlot;
			rid.pageNum = pageNum;
			rid.slotNum = deletedPointer;
			shouldUpdatePage = true;

			// update deletedPointer
			int newSlotNum = slotSize - deletedPointer - 1;
			bool hasDeletedNode = false;
			if( newSlotNum > 0 )
			{
				int pivot = 0;
				DIRECTORYSLOT *afterCurSlots = new DIRECTORYSLOT[newSlotNum];
				memcpy( afterCurSlots, tmpPage + getSlotOffset(deletedPointer+1), sizeof(DIRECTORYSLOT)*newSlotNum );
				while( pivot < newSlotNum )
				{
					if( afterCurSlots[pivot].slotType == Deleted )
					{
						deletedPointer = pivot + 1 + deletedPointer;
						hasDeletedNode = true;
						break;
					}
					pivot++;
				}

				delete []afterCurSlots;
			}
			if( !hasDeletedNode )
			{
				deletedPointer = -1;
			}

			memcpy( tmpPage + getDeletedPointerOffset(), &deletedPointer, sizeof(RecordMinLen) );
		}
		else
		{
			// insert new slot
			DIRECTORYSLOT *slot = (DIRECTORYSLOT *) malloc(sizeof(DIRECTORYSLOT));
			slot->recordSize = localOffset;
			slot->pageOffset = lastSlot.pageOffset + lastSlot.recordSize;
			slot->slotType = Normal;

			slotSize += 1;
			restSize -= (localOffset + sizeof(DIRECTORYSLOT) );
			memcpy( tmpPage + getSlotOffset(slotSize-1), slot, sizeof(DIRECTORYSLOT) );
			// copy recordData to page
			memcpy( tmpPage + slot->pageOffset, recordData, localOffset );

			// free memory
			free(slot);

			rid.pageNum = pageNum;
			rid.slotNum = slotSize - 1;
		}

		// update value back
		memcpy( tmpPage + getRestSizeOffset(), (void*) &restSize, sizeof(RecordMinLen) );
		memcpy( tmpPage + getSlotCountOffset(), (void*) &slotSize, sizeof(RecordMinLen) );

		// write to page
		fileHandle.writePage(pageNum, tmpPage);
	}

	//free pointer and return
	free(tmpPage);
	free(recordData);
	// update data when insert in deleted node
	if( shouldUpdatePage )
	{
		updateRecord( fileHandle, recordDescriptor, data, rid );
	}
	return 0;
}

RC RecordBasedFileManager::readRecord(FileHandle &fileHandle, const vector<Attribute> &recordDescriptor, const RID &rid, void *data) {
    unsigned pageNum = rid.pageNum;
    unsigned slotNum = rid.slotNum;
    int recordSize = recordDescriptor.size();
    // read page first
    void* tmpPage = malloc(PAGE_SIZE);
    if( fileHandle.readPage(pageNum, tmpPage) == -1 )
    {
    	return  -1;
    }
    // get record offset
    DIRECTORYSLOT slot;
    memcpy( (void*) &slot, tmpPage + getSlotOffset(slotNum), sizeof(DIRECTORYSLOT) );


    RecordMinLen pageOffset =  slot.pageOffset;
    RecordMinLen tmpSize = slot.recordSize;
    SlotType slotType = slot.slotType;

    // record deleted
    if( slotType == Deleted ) return -1;

    RID tmpRid;
    while( slotType == MasterPointer || slotType == SlavePointer )
    {
    	memcpy( &tmpRid, tmpPage + slot.pageOffset, sizeof(RID) );
    	pageNum = tmpRid.pageNum;
    	slotNum = tmpRid.slotNum;
    	if( fileHandle.readPage(pageNum, tmpPage) == -1 )
    	{
    	    free(tmpPage);
    		return -1;
    	}
    	memcpy( (void*) &slot, tmpPage + getSlotOffset(slotNum), sizeof(DIRECTORYSLOT) );
    	pageOffset = slot.pageOffset;
    	tmpSize = slot.recordSize;
    	slotType = slot.slotType;
    }

    int nullBytes= getActualBytesForNullsIndicator( recordSize );
    unsigned char* nullIndicator = (unsigned char*) malloc(nullBytes);
    // move to NullBytes position
    memcpy( (void*) nullIndicator, (void*)tmpPage + pageOffset + sizeof(RecordMinLen), nullBytes );

    unsigned int recordOffset = sizeof(RecordMinLen) + nullBytes + recordSize*sizeof(RecordMinLen);
    unsigned int recordActualSize = tmpSize - recordOffset;
    void* recordData = malloc(recordActualSize);
    memcpy( recordData, (void*)tmpPage+pageOffset+recordOffset, recordActualSize );

    // copy to dest (void*)data
    memcpy( data, nullIndicator, nullBytes);
    memcpy( data + nullBytes, recordData, recordActualSize );

    // free memory
    free(nullIndicator);
    free(recordData);
    free(tmpPage);
	return 0;
}

RC RecordBasedFileManager::printRecord(const vector<Attribute> &recordDescriptor, const void *data) {
    int recordSize = recordDescriptor.size();
	int nullBytes = getActualBytesForNullsIndicator( recordSize );
    unsigned char* nullIndicator = (unsigned char*) malloc(nullBytes);
    memcpy( nullIndicator, data, nullBytes );
    // loop record
    unsigned int offset = nullBytes;
    string columnName;
    AttrType columnType;
    AttrLength columnLength;
    unsigned int shiftBit;
    bool isNull = false;
    for(int i = 0; i<recordSize;i++)
    {
    	columnName = recordDescriptor[i].name;
    	columnType = recordDescriptor[i].type;
    	columnLength = recordDescriptor[i].length;
    	cout<<columnName<<":"<<"	";
    	shiftBit = 8*nullBytes - i - 1;
    	isNull = nullIndicator[0] & ( 1 << shiftBit );
    	if( !isNull )
    	{
    		if( columnType == TypeVarChar )
    		{
    			int charCount;
    			memcpy((void *) &charCount, (char*)data + offset, sizeof(int));
    			offset += sizeof(int);
    			if(charCount > 0)
    			{
    				char charVal[charCount];
    				memcpy(charVal, (char*)data+offset, charCount);
    				offset += sizeof(char)*charCount;
    				for(int q=0; q<charCount ; q++)
    				{
    					cout<<charVal[q];
    				}
    				cout<<'	';
    			}
    			else{
    				cout<<'	';
    			}
    		}else
    		{

    			if(columnType == TypeReal)
    			{
    				float realVal;
    				memcpy( (void*)&realVal, (char*)data + offset, sizeof(float) );
    				offset += sizeof(float);
    				cout<<realVal<<'	';
    			}else
    			{
    				int intVal;
    				memcpy( (void*)&intVal, (char*)data + offset, sizeof(int) );
    				offset += sizeof(int);
    				cout<<intVal<<'	';
    			}
    		}
    	}
    	else{
    		cout<<"NULL"<<'	';
    	}
    }

    cout<<endl;
    free(nullIndicator);
    return 0;
}

// accessory functions
// Calculate actual bytes for nulls-indicator for the given field counts
inline int getActualBytesForNullsIndicator(int fieldCount) {
    return ceil((double) fieldCount / CHAR_BIT);
}

inline unsigned getSlotOffset(int slotNum) {
	return ( PAGE_SIZE - 3*sizeof(RecordMinLen) - sizeof(DIRECTORYSLOT)*(slotNum + 1) );
}

inline unsigned getRestSizeOffset() {
	return ( PAGE_SIZE - 1*sizeof(RecordMinLen) );
}

inline unsigned getSlotCountOffset() {
	return ( PAGE_SIZE - 2*sizeof(RecordMinLen) );
}

inline unsigned getDeletedPointerOffset() {
	return ( PAGE_SIZE - 3*sizeof(RecordMinLen) );
}

inline unsigned getDirectorySize() {
	return ( sizeof(DIRECTORYSLOT) + 3*sizeof(RecordMinLen) );
}

inline unsigned getNullBytesOffset() {
	return sizeof(RecordMinLen);
}
///////////////////////
//Project 2 functions//
///////////////////////
RC RecordBasedFileManager::prepareRecord( const vector<Attribute> &recordDescriptor, unsigned int &localOffset, void **recordData, const void *data ) {
	int recordSize = recordDescriptor.size();
	RecordMinLen recordAttrCount = recordSize;
	// copy NULL check addr
	int nullBytes= getActualBytesForNullsIndicator( recordSize );
	unsigned char* nullIndicator = (unsigned char*) malloc(nullBytes);
	memcpy( (void*) nullIndicator, (void*) data, nullBytes );

	// malloc address offset pointer
	unsigned int recordAddrPointerSize = sizeof(RecordMinLen)*recordSize;
	void *recordAddrPointer = malloc( recordAddrPointerSize );
	memset( recordAddrPointer, 0, recordAddrPointerSize );

	// offset of record before insert (column count + recordAddrPointer + nullIndicator)
	RecordMinLen recordOffset = sizeof(RecordMinLen)*( 1+recordAddrPointerSize ) + nullBytes;
	// used for fetching data
	unsigned int dataOffset = nullBytes;
	// the data of record size
	unsigned int recordActualSize = 0;

	for( int i = 0; i<recordSize;i++ )
	{
		AttrType columnType = recordDescriptor[i].type;
		AttrLength columnLength =recordDescriptor[i].length;

		int shiftBit = 8*nullBytes - i - 1;
		bool isNull = nullIndicator[0] & ( 1<< shiftBit );
		// NULL case
		if( isNull )
		{
			memcpy( (char*)recordAddrPointer + i*sizeof(RecordMinLen), (void*) &recordOffset, sizeof(RecordMinLen) );
			continue;
		}

		// not NULL case
		switch (columnType)
		{
			case TypeVarChar:
			{
				int charCount;
				memcpy( (void*)&charCount, (char*) data + dataOffset, sizeof(int) );
				// varchar over length
//				if( charCount > columnLength )
//				{
//					free(recordAddrPointer);
//					free(nullIndicator);
//					return -1;
//					charCount = columnLength;
//				}
				RecordMinLen varcharRecordSize = charCount*sizeof(char) + sizeof(int);
				recordOffset += varcharRecordSize;
				recordActualSize += varcharRecordSize;
				// set recordAddrPointer
				// each time shift sizeof(RecordMinLen)
				memcpy( (char*)recordAddrPointer + i*sizeof(RecordMinLen), (void*) &recordOffset, sizeof(RecordMinLen) );

				dataOffset += varcharRecordSize;
				break;
			}
			case TypeInt:
			{
				recordOffset += sizeof(int);
				recordActualSize += sizeof(int);

				memcpy( (char*)recordAddrPointer + i*sizeof(RecordMinLen), (void*) &recordOffset, sizeof(RecordMinLen) );
				dataOffset += sizeof(int);
				break;
			}
			case TypeReal:
			{
				recordOffset += sizeof(float);
				recordActualSize += sizeof(float);

				memcpy( (char*) recordAddrPointer + i*sizeof(RecordMinLen), (void*) &recordOffset, sizeof(RecordMinLen) );
				dataOffset += sizeof(float);
				break;
			}
		}
	}
	// memcpy data to recordData
	// copy data to recordData
	*recordData = malloc(recordOffset);
	memcpy( (char*) *recordData, &recordAttrCount, sizeof(RecordMinLen) );
	localOffset += sizeof(RecordMinLen);
	memcpy( (char*) *recordData + localOffset, nullIndicator, nullBytes );
	localOffset += nullBytes;
	memcpy( (char*) *recordData + localOffset, (char*)recordAddrPointer, recordAddrPointerSize );
	localOffset += recordAddrPointerSize;

	memcpy( (char*) *recordData + localOffset, (char*) data + nullBytes, recordActualSize );
	localOffset += recordActualSize;


	// free
	free(recordAddrPointer);
	free(nullIndicator);
	
	return 0;
}

RC RecordBasedFileManager::deleteRecord(FileHandle &fileHandle, const vector<Attribute> &recordDescriptor, const RID &rid) {
	int maxPage = fileHandle.getNumberOfPages() - 1;
	int pageNum = rid.pageNum;
	int slotNum = rid.slotNum;

	void *tmpPage = malloc(PAGE_SIZE);

	if( pageNum > maxPage || fileHandle.readPage(pageNum, tmpPage) != 0 )
	{
		free(tmpPage);
		return -1;
	}

	// get slot to delete
	DIRECTORYSLOT tmpSlot;
	memcpy( &tmpSlot, (char*)tmpPage + getSlotOffset(slotNum), sizeof(DIRECTORYSLOT) );
	RecordMinLen deletedRecordSize = tmpSlot.recordSize;
	RecordMinLen deletedRecordOffset = tmpSlot.pageOffset;
	SlotType deletedRecordSlotType = tmpSlot.slotType;

	// get slot num
	RecordMinLen slotCount, newSlotCount;

	memcpy( &slotCount, (char*) tmpPage + getSlotCountOffset(), sizeof(RecordMinLen) );
	newSlotCount = slotCount - 1;
	memcpy( (char*) tmpPage + getSlotCountOffset(), &newSlotCount, sizeof(RecordMinLen) );

	// get restSize
	RecordMinLen restSize;
	memcpy( &restSize, (char*) tmpPage + getRestSizeOffset(), sizeof(RecordMinLen) );
	restSize += deletedRecordSize;
	memcpy( (char*) tmpPage + getRestSizeOffset(), &restSize, sizeof(RecordMinLen) );

	// calculate total size between slotNum+1 -> slotNum End
	// not the last slot
	if( slotNum != (slotCount-1) )
	{
		DIRECTORYSLOT nextSlot, lastSlot;
		memcpy( &nextSlot, (char*)tmpPage + getSlotOffset(slotNum+1), sizeof(DIRECTORYSLOT) );
		memcpy( &lastSlot, (char*)tmpPage + getSlotOffset(slotCount-1), sizeof(DIRECTORYSLOT) );
		RecordMinLen shiftedLen = lastSlot.pageOffset + lastSlot.recordSize - nextSlot.pageOffset;

		// from here add deletedRecordSize to memset 0
		RecordMinLen shiftedResetOffset = lastSlot.pageOffset + lastSlot.recordSize - deletedRecordSize;

		// shift pageOffset
		memmove( (char*)tmpPage + deletedRecordOffset, (char*)tmpPage + nextSlot.pageOffset, shiftedLen );
		memset( (char*)tmpPage + shiftedResetOffset, 0, deletedRecordSize );
		// update slots after slotNum
		RecordMinLen slotIter = slotNum + 1;
		while( slotIter < slotCount )
		{
			DIRECTORYSLOT s;
			memcpy( &s, (char*)tmpPage + getSlotOffset(slotIter), sizeof(DIRECTORYSLOT) );
			s.pageOffset -= deletedRecordSize;
			memcpy( (char*)tmpPage + getSlotOffset(slotIter), &s, sizeof(DIRECTORYSLOT) );
			slotIter += 1;
		}
	}

	// get rid
	bool shouldTreverseDeleteNode = false;
	RID treverseRid;
	if( deletedRecordSlotType == MasterPointer || deletedRecordSlotType == SlavePointer )
	{
		shouldTreverseDeleteNode = true;
		memcpy( &treverseRid, (char*)tmpPage + deletedRecordOffset, sizeof(RID) );
	}

	// clear slot and save
	tmpSlot.slotType = Deleted;
	tmpSlot.pageOffset = -1;
	tmpSlot.recordSize = 0;
	memcpy( (char*)tmpPage + getSlotOffset(slotNum), &tmpSlot, sizeof(DIRECTORYSLOT) );

	// update deletedPointer
	RecordMinLen deletedPointer;
	memcpy( &deletedPointer, tmpPage + getDeletedPointerOffset(), sizeof(RecordMinLen) );
	RecordMinLen sNum = slotNum;
	if( deletedPointer == -1 || deletedPointer > sNum )
	{
		memcpy( tmpPage + getDeletedPointerOffset(), &sNum, sizeof(RecordMinLen) );
	}

	// write page
	fileHandle.writePage(pageNum, tmpPage);

	// free
	free(tmpPage);

	// if the record is pointer, it should delete recursively
	if( shouldTreverseDeleteNode )
	{
		deleteRecord( fileHandle, recordDescriptor, treverseRid );
	}
	return 0;
}

// update slotType
RC RecordBasedFileManager::updateSlotType(FileHandle &fileHandle, RID &rid, SlotType slotType){
	int pageNum = rid.pageNum;
	int slotNum = rid.slotNum;

	void *tmpPage = malloc(PAGE_SIZE);

	if( fileHandle.readPage(pageNum, tmpPage) != 0 )
	{
		return -1;
	}
	DIRECTORYSLOT curSlot;
	memcpy( &curSlot, tmpPage + getSlotOffset(slotNum), sizeof(DIRECTORYSLOT) );
	curSlot.slotType = slotType;
	memcpy( tmpPage + getSlotOffset(slotNum), &curSlot, sizeof(DIRECTORYSLOT) );

	// free
	free(tmpPage);

	return 0;
}

RC RecordBasedFileManager::updateRecord(FileHandle &fileHandle, const vector<Attribute> &recordDescriptor, const void *data, const RID &rid) {
	int maxPage = fileHandle.getNumberOfPages() - 1;
	int pageNum = rid.pageNum;
	int slotNum = rid.slotNum;

	void *tmpPage = malloc(PAGE_SIZE);

	// definition
	RecordMinLen restSize;
	RecordMinLen slotCount;
	DIRECTORYSLOT curSlot;

	// fetch the slot that stores the data
	do{
		if( pageNum > maxPage || fileHandle.readPage(pageNum, tmpPage) != 0 )
		{
			free(tmpPage);
			return -1;
		}
		// get slot num
		memcpy( &slotCount, (char*) tmpPage + getSlotCountOffset(), sizeof(RecordMinLen) );
		memcpy( &restSize, tmpPage + getRestSizeOffset(), sizeof(RecordMinLen) );
		// Copy slot to dest
		memcpy( &curSlot, tmpPage + getSlotOffset(slotNum), sizeof(DIRECTORYSLOT) );
		// Get pageNum, slotNum
		RID tmpRid;
		if( curSlot.slotType == MasterPointer || curSlot.slotType == SlavePointer )
		{
			memcpy( &tmpRid, tmpPage + curSlot.pageOffset, curSlot.recordSize );
			pageNum = tmpRid.pageNum;
			slotNum = tmpRid.slotNum;
		}

	}while( curSlot.slotType == MasterPointer || curSlot.slotType == SlavePointer );
	
	// memcpy data to recordData
	unsigned int recordFullSize = 0;
	void *recordData = NULL;
	SlotType curSlotType = curSlot.slotType;

	// use pointer to pointer for malloc in function
	if( prepareRecord( recordDescriptor, recordFullSize, &recordData, data ) != 0 )
	{
		free(tmpPage);
		return -1;
	}

	if( pageNum > maxPage || fileHandle.readPage(pageNum, tmpPage) != 0 )
	{
		free(tmpPage);
		free(recordData);
		return -1;
	}
	// get slot
	RecordMinLen originalRecordSize = curSlot.recordSize;
	//  which can shift right
	int recordDiff = recordFullSize - originalRecordSize;

	// used for copyData at the end
	unsigned copySize;
	RID slaveRid;
	bool updateType = true;
	// update curSlot
	if( restSize >= recordDiff )
	{
		// update slot
		curSlot.recordSize = recordFullSize;
		curSlot.slotType = Normal;
		copySize = recordFullSize;
		restSize -= recordDiff;
		if( curSlotType == Deleted )
		{
			RecordMinLen offset = 0;
			if( slotNum != (slotCount -1) )
			{
				DIRECTORYSLOT nextSlot;
				memcpy( &nextSlot, (char*)tmpPage + getSlotOffset(slotNum+1), sizeof(DIRECTORYSLOT) );
				offset = nextSlot.pageOffset;
			}
			curSlot.pageOffset = offset;
		}
	}
	// should insert data in other page
	else{
		updateType = false;
		curSlot.recordSize = sizeof(RID);
		if( curSlotType == Normal || curSlotType == Deleted )
		{
			curSlot.slotType = MasterPointer;
			// Deleted page offset is next slot's offset if existing
			if( curSlotType == Deleted )
			{
				RecordMinLen offset = 0;
				if( slotNum != (slotCount -1) )
				{
					DIRECTORYSLOT nextSlot;
					memcpy( &nextSlot, (char*)tmpPage + getSlotOffset(slotNum+1), sizeof(DIRECTORYSLOT) );
					offset = nextSlot.pageOffset;
				}
				curSlot.pageOffset = offset;
			}
		}
		// DataPointer
		else
		{
			curSlot.slotType = SlavePointer;
		}

		recordDiff = sizeof(RID) - originalRecordSize;
		if( insertRecord(fileHandle, recordDescriptor, data, slaveRid) != 0 )
		{
			free(tmpPage);
			free(recordData);
			return -1;
		}
		// update slaveRid's record SlotType
		updateSlotType( fileHandle, slaveRid, DataPointer );

		copySize = sizeof(RID);
		restSize += originalRecordSize - sizeof(RID);
	}
	memcpy( tmpPage + getSlotOffset(slotNum), &curSlot, sizeof(DIRECTORYSLOT) );

	// move slot
	// not the last record in a page
	if( slotNum != (slotCount -1) )
	{
		// get next slot
		// get last slot
		DIRECTORYSLOT nextSlot, lastSlot;
		memcpy( &nextSlot, (char*)tmpPage + getSlotOffset(slotNum+1), sizeof(DIRECTORYSLOT) );
		memcpy( &lastSlot, (char*)tmpPage + getSlotOffset(slotCount-1), sizeof(DIRECTORYSLOT) );
		// move resizeDiff steps
		RecordMinLen shiftedLen = lastSlot.pageOffset + lastSlot.recordSize - nextSlot.pageOffset;
		memmove( tmpPage + nextSlot.pageOffset + recordDiff, tmpPage + nextSlot.pageOffset, shiftedLen );

		// add recordDiff to pageOffset
		int slotIter = slotNum + 1;
		DIRECTORYSLOT s;
		while(slotIter < slotCount)
		{
			memcpy( &s, (char*)tmpPage + getSlotOffset(slotIter), sizeof(DIRECTORYSLOT) );
			s.pageOffset += recordDiff;
			memcpy( (char*)tmpPage + getSlotOffset(slotIter), &s, sizeof(DIRECTORYSLOT) );
			slotIter += 1;
		}
	}

	// copy new record to curSlot.pageOffset
	if( updateType )
	{
		memcpy( tmpPage + curSlot.pageOffset, recordData, copySize );
	}
	else
	{
		memcpy( tmpPage + curSlot.pageOffset, &slaveRid, copySize );
	}

	memcpy( tmpPage + getRestSizeOffset(), &restSize, sizeof(RecordMinLen) );
	// write back
	fileHandle.writePage(pageNum, tmpPage);

	// free
	free(recordData);
	free(tmpPage);
	return 0;
}

// for scanning
RC RecordBasedFileManager::scan(FileHandle &fileHandle,
	      const vector<Attribute> &recordDescriptor,
	      const string &conditionAttribute,
	      const CompOp compOp,                  // comparision type such as "<" and "="
	      const void *value,                    // used in the comparison
	      const vector<string> &attributeNames, // a list of projected attributes
	      RBFM_ScanIterator &rbfm_ScanIterator){

	unsigned recordSize = recordDescriptor.size();
	string recordName[recordSize];

	rbfm_ScanIterator._recordDescriptor = recordDescriptor;
	rbfm_ScanIterator._conditionAttribute = conditionAttribute;
	rbfm_ScanIterator._compOp = compOp;
	rbfm_ScanIterator._value = (void*)value;
	rbfm_ScanIterator._attributeNames = attributeNames;
	rbfm_ScanIterator._fileHandle = fileHandle;
	return 0;
}


// RBFM_ScanIterator

RBFM_ScanIterator::RBFM_ScanIterator(){
	_isFirstIter = true;
	_value = 0;
	_tmpPage = malloc(PAGE_SIZE);
}

RBFM_ScanIterator::~RBFM_ScanIterator(){
	free(_tmpPage);
}

RC RBFM_ScanIterator::getNextRecord(RID &rid, void *data){
	// check rbfm pointer
	unsigned pageNum = 0;
	unsigned slotNum = 0;
	int maxPage = _fileHandle.getNumberOfPages() - 1;

	if( !_isFirstIter )
	{
		pageNum = _cursor.pageNum;
		slotNum = _cursor.slotNum + 1;

		if( pageNum > maxPage || _fileHandle.readPage(pageNum, _tmpPage) != 0 )
		{
			return -1;
		}
	}


	RecordMinLen curTotalSlot;

	memcpy( &curTotalSlot, _tmpPage + getSlotCountOffset(), sizeof(RecordMinLen) );

	if( slotNum >= curTotalSlot )
	{
		// no more data to search for
		if( maxPage == pageNum )
		{
			return -1;
		}
		// treverse next page
		else
		{
			pageNum += 1;
			slotNum = 0;
			if( _fileHandle.readPage(pageNum, _tmpPage) != 0 )
			{
				return -1;
			}
		}

	}
	unsigned recordMaxSize = getRecordSize( _recordDescriptor );
	void *tmpData = malloc(recordMaxSize);
	RID tmpRid;
	DIRECTORYSLOT tmpSlot;
	// treverse records in page
	unsigned curSlotId = slotNum;
	unsigned curPageId = pageNum;
	bool notScan = true;
	while(notScan && curTotalSlot > 0)
	{
		if(curPageId >  maxPage)
		{
			break;
		}
		// read new page
		if( curPageId != pageNum )
		{
			_fileHandle.readPage(curPageId, _tmpPage);
			memcpy( &curTotalSlot, _tmpPage + getSlotCountOffset(), sizeof(RecordMinLen) );
			while( curTotalSlot == 0 && curPageId < maxPage )
			{
				curPageId += 1;
				_fileHandle.readPage(curPageId, _tmpPage);
				memcpy( &curTotalSlot, _tmpPage + getSlotCountOffset(), sizeof(RecordMinLen) );
			}
			// in the maxpage still has nothing to read
			if(curTotalSlot == 0)
			{
				break;
			}
			pageNum = curPageId;
		}

		tmpRid.pageNum = curPageId;
		tmpRid.slotNum = curSlotId;
		memcpy(&tmpSlot, _tmpPage+getSlotOffset(curSlotId), sizeof(DIRECTORYSLOT) );
		SlotType slotType = tmpSlot.slotType;
		// treverse only when the slotType is Normal or MasterPointer
		if( slotType == Normal || slotType == MasterPointer )
		{
			if( checkRecord(tmpRid, tmpData) == 0 )
			{
				notScan = false;
				break;
			}
		}

		// iteration params updating
		if( (curSlotId + 1) < curTotalSlot )
		{
			curSlotId += 1;
		}
		else{
			curPageId += 1;
			curSlotId = 0;
		}
	}

	free(tmpData);
	return 0;
}



RC RBFM_ScanIterator::checkRecord(const RID rid, void* data) {
	// readRecord failed
	if( readFullRecord( rid, data ) != 0 )
	{
		return -1;
	}

	// if no condition return 0
	if( _compOp == NO_OP )
	{
		return 0;
	}

	int recordSize = _recordDescriptor.size();
	int nullBytes = getActualBytesForNullsIndicator( recordSize );
	unsigned char* nullIndicator = (unsigned char*) malloc(nullBytes);
	memcpy( nullIndicator, data+getNullBytesOffset(), nullBytes );
	// loop record
	unsigned int offset = nullBytes;
	string columnName;
	AttrType columnType;
	AttrLength columnLength;
	unsigned int shiftBit;
	bool isNull = false;


	// which one is dest column
	unsigned columnPivot = 0;

	for( int i=0;i<recordSize;i++ )
	{
		columnName = _recordDescriptor[i].name;
		columnType = _recordDescriptor[i].type;
		columnLength = _recordDescriptor[i].length;

		shiftBit = 8*nullBytes - i - 1;
		isNull = nullIndicator[0] & ( 1 << shiftBit );
		if( columnName == _conditionAttribute )
		{
			columnPivot = i;
			break;
		}
	}
	shiftBit = 8*nullBytes - columnPivot - 1;
	isNull = nullIndicator[0] & ( 1 << shiftBit );
	RC success = -1;

	if( isNull )
	{

		if( _compOp == EQ_OP)
		{
			if( _value == NULL )
			{
				success = 0;
			}
		}
		if( _compOp == NE_OP )
		{
			if( _value != NULL )
			{
				success = 0;
			}
		}
		free(nullIndicator);
		return success;
	}

	// not NULL case
	void *destColumnData;

	RecordMinLen addressPointerOffset = getNullBytesOffset() + nullBytes + columnPivot*sizeof(RecordMinLen);
	RecordMinLen addressColumnEndOffset;
	memcpy( &addressColumnEndOffset, data+addressPointerOffset, sizeof(RecordMinLen) );

	RecordMinLen addressColumnStartOffset = getNullBytesOffset() + nullBytes +recordSize*sizeof(RecordMinLen);
	if(columnPivot != 0)
	{
		unsigned addressPrevPointerOffset = getNullBytesOffset() + nullBytes + (columnPivot-1)*sizeof(RecordMinLen);
		memcpy( &addressColumnStartOffset, data+addressPrevPointerOffset, sizeof(RecordMinLen) );
	}
	unsigned destColumnSize = addressColumnEndOffset - addressColumnStartOffset;

	// copy destColumnData
	destColumnData = malloc(destColumnSize);
	memcpy( destColumnData, data+addressColumnStartOffset, destColumnSize );

	// prepare data for varchar
	int compLen;
	int destLen;
	void *compColumn;
	void *destColumn;
	if( columnType == TypeVarChar )
	{
		memcpy( &compLen, _value, sizeof(int) );
		memcpy( &destLen, destColumnData, sizeof(int) );
		compColumn = malloc(compLen);
		destColumn = malloc(destLen);

		memcpy( compColumn, _value+sizeof(int), compLen );
		memcpy( destColumn, destColumnData+sizeof(int), destLen );
	}
	else
	{
		int allocateSize = columnType == TypeInt ? sizeof(int) : sizeof(float);

		compLen = allocateSize;
		destLen = allocateSize;

		compColumn = malloc(compLen);
		destColumn = malloc(destLen);

		memcpy( compColumn, _value, compLen );
		memcpy( destColumn, destColumnData, destLen );
	}

	bool compBool = false; 
	switch( _compOp )
	{
		case EQ_OP:
		{
			if( compLen != destLen )
			{
				success = -1;
				break;
			}
			if( memcmp(destColumn, compColumn, compLen) == 0)
			{
				success = 0;
				break;
			}
			break;
		}
		case LT_OP:
		{
			if( columnType == TypeVarChar )
			{
				if ( strcmp( (char*)destColumn, (char*)compColumn ) < 0 )
				{
					success = 0;
					break;
				}
			}
			else
			{
				if( columnType == TypeInt )
				{
					compBool = ( (int*)destColumn < (int*)compColumn );
				}
				else
				{
					compBool = ( (float*)destColumn < (float*)compColumn );
				}
				if( compBool )
				{
					success = 0;
					break;
				}
			}
			break;
		}
		case LE_OP:
		{
			if( columnType == TypeVarChar )
			{
				if ( strcmp( (char*)destColumn, (char*)compColumn ) <= 0 )
				{
					success = 0;
					break;
				}
			}
			else
			{
				if( columnType == TypeInt )
				{
					compBool = ( (int*)destColumn <= (int*)compColumn );
				}
				else
				{
					compBool = ( (float*)destColumn <= (float*)compColumn );
				}
				if( compBool )
				{
					success = 0;
					break;
				}
			}
			break;
		}
		case GT_OP:
		{
			if( columnType == TypeVarChar )
			{
				if ( strcmp( (char*)destColumn, (char*)compColumn ) > 0 )
				{
					success = 0;
					break;
				}
			}
			else
			{
				if( columnType == TypeInt )
				{
					compBool = ( (int*)destColumn > (int*)compColumn );
				}
				else
				{
					compBool = ( (float*)destColumn > (float*)compColumn );
				}
				if( compBool )
				{
					success = 0;
					break;
				}
			}
			break;
		}
		case GE_OP:
		{
			if( columnType == TypeVarChar )
			{
				if ( strcmp( (char*)destColumn, (char*)compColumn ) >= 0 )
				{
					success = 0;
					break;
				}
			}
			else
			{
				if( columnType == TypeInt )
				{
					compBool = ( (int*)destColumn >= (int*)compColumn );
				}
				else
				{
					compBool = ( (float*)destColumn >= (float*)compColumn );
				}
				if( compBool )
				{
					success = 0;
					break;
				}
			}
			break;
		}
		case NE_OP:
		{
			if( columnType == TypeVarChar )
			{
				if( compLen != destLen )
				{
					success = 0;
					break;
				}
				if ( strcmp( (char*)destColumn, (char*)compColumn ) != 0 )
				{
					success = 0;
					break;
				}
			}
			else
			{
				if( columnType == TypeInt )
				{
					compBool = ( (int*)destColumn != (int*)compColumn );
				}
				else
				{
					compBool = ( (float*)destColumn != (float*)compColumn );
				}
				if( compBool )
				{
					success = 0;
					break;
				}
			}
			break;
		}
	}


	free(compColumn);
	free(destColumn);

	free(destColumnData);
	free(nullIndicator);
	return success;
}

RC RBFM_ScanIterator::readFullRecord(const RID &rid, void *data) {
    unsigned pageNum = rid.pageNum;
    unsigned slotNum = rid.slotNum;
    int recordSize = _recordDescriptor.size();
    // read page first
    void* tmpPage = malloc(PAGE_SIZE);
    if( _fileHandle.readPage(pageNum, tmpPage) == -1 )
    {
    	return  -1;
    }
    // get record offset
    DIRECTORYSLOT slot;
    memcpy( (void*) &slot, tmpPage + getSlotOffset(slotNum), sizeof(DIRECTORYSLOT) );


    RecordMinLen pageOffset =  slot.pageOffset;
    RecordMinLen tmpSize = slot.recordSize;
    SlotType slotType = slot.slotType;

    // record deleted
    if( slotType == Deleted ) return -1;

    RID tmpRid;
    while( slotType == MasterPointer || slotType == SlavePointer )
    {
    	memcpy( &tmpRid, tmpPage + slot.pageOffset, sizeof(RID) );
    	pageNum = tmpRid.pageNum;
    	slotNum = tmpRid.slotNum;
    	if( _fileHandle.readPage(pageNum, tmpPage) == -1 )
    	{
    	    free(tmpPage);
    		return -1;
    	}
    	memcpy( (void*) &slot, tmpPage + getSlotOffset(slotNum), sizeof(DIRECTORYSLOT) );
    	pageOffset = slot.pageOffset;
    	tmpSize = slot.recordSize;
    	slotType = slot.slotType;
    }

    // copy to dest
    memcpy( data, tmpPage+pageOffset, tmpSize );

    // free memory
    free(tmpPage);
	return 0;
}

unsigned getRecordSize(const vector<Attribute> &recordDescriptor) {
    unsigned recordMaxSize = 0;
	int recordSize = recordDescriptor.size();
	int nullBytes = getActualBytesForNullsIndicator( recordSize );
	unsigned int recordAddrPointerSize = sizeof(RecordMinLen)*recordSize;
	//!!! may change when dealing with bonus
	// column count + recordAddrPointer + nullIndicator
	recordMaxSize += nullBytes + sizeof(RecordMinLen)*( 1+recordAddrPointerSize );
	// loop record
    string columnName;
    AttrType columnType;
    AttrLength columnLength;

    for(int i = 0; i<recordSize;i++)
    {
    	columnName = recordDescriptor[i].name;
    	columnType = recordDescriptor[i].type;
    	columnLength = recordDescriptor[i].length;

    	if( columnType == TypeVarChar )
    	{
    			recordMaxSize += columnLength + sizeof(int);
    	}
    	else
    	{
    		recordMaxSize += columnLength;
    	}
    }

    return recordMaxSize;
}
