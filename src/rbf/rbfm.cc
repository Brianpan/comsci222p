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
	void *recordData = malloc(PAGE_SIZE);
	// use pointer to pointer for malloc in function
	if( prepareRecord( recordDescriptor, localOffset, recordData, data ) != 0 )
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
		memcpy( &remainSize, (char*)tmpPage + getRestSizeOffset() , sizeof(RecordMinLen) );
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
				memcpy( &remainSize, (char*)tmpPage + getRestSizeOffset() , sizeof(RecordMinLen) );
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
		memcpy( (char*)directory + tmpOffset, slot, sizeof(DIRECTORYSLOT) );
		tmpOffset += sizeof(DIRECTORYSLOT);
		memcpy( (char*)directory + tmpOffset,  &deletedPointer, sizeof(RecordMinLen) );
		tmpOffset += sizeof(RecordMinLen);
		memcpy( (char*)directory + tmpOffset, &slotSize, sizeof(RecordMinLen) );
		tmpOffset += sizeof(RecordMinLen);
		memcpy( (char*)directory + tmpOffset,  &restSize, sizeof(RecordMinLen) );

		//copy directory to tmpPage
		memcpy( (char*)tmpPage + (PAGE_SIZE-directorySize), directory, directorySize );
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
		memcpy( &restSize, (char*)tmpPage + getRestSizeOffset(), sizeof(RecordMinLen) );
		memcpy( &slotSize, (char*)tmpPage + getSlotCountOffset(), sizeof(RecordMinLen) );

		//!!! get curr slotSize
		RecordMinLen deletedPointer;
		memcpy( &deletedPointer, (char*)tmpPage + getDeletedPointerOffset(), sizeof(RecordMinLen) );
		if( deletedPointer != -1 )
		{
			// use deleted slot
			rid.pageNum = pageNum;
			rid.slotNum = deletedPointer;

			DIRECTORYSLOT slot;
			slot.slotType = Normal;
			slot.recordSize = localOffset;

			// traverse get page offset
			if(deletedPointer == 0)
			{
				slot.pageOffset = 0;
			}
			else
			{
				int prevIdx = deletedPointer - 1;
				DIRECTORYSLOT s;
				while(prevIdx >=0)
				{
					memcpy( &s, (char*)tmpPage+getSlotOffset(prevIdx), sizeof(DIRECTORYSLOT) );
					if( s.slotType != Deleted)
						break;
					prevIdx -= 1;
				}
				if(s.slotType != Deleted)
				{
					slot.pageOffset = s.pageOffset + s.recordSize;
				}
				else
				{
					slot.pageOffset = 0;
				}
			}
			DIRECTORYSLOT nextSlot,lastSlot;
			DIRECTORYSLOT s;
			s.slotType = Deleted;

			int nextIdx = deletedPointer + 1;
			bool shouldMoveRecord = false;
			while( nextIdx < slotSize )
			{
				memcpy( &s, (char*)tmpPage+getSlotOffset(nextIdx), sizeof(DIRECTORYSLOT) );
				if(s.slotType != Deleted)
				{
					break;
				}
				nextIdx += 1;
			}

			if( s.slotType != Deleted )
			{
				nextSlot = s;
				int lastIdx = slotSize - 1;

				while(lastIdx > deletedPointer)
				{
					memcpy( &s, (char*)tmpPage+getSlotOffset(lastIdx), sizeof(DIRECTORYSLOT) );
					if(s.slotType != Deleted)
					{
						break;
					}
					lastIdx -= 1;
				}
				lastSlot = s;

				int nextIdx = deletedPointer + 1;
				while(nextIdx<slotSize)
				{
					memcpy( &s, (char*)tmpPage+getSlotOffset(nextIdx), sizeof(DIRECTORYSLOT) );
					if(s.slotType != Deleted)
					{
						s.pageOffset += slot.recordSize;
						memcpy( (char*)tmpPage+getSlotOffset(nextIdx), &s, sizeof(DIRECTORYSLOT) );
					}
					nextIdx += 1;
				}
				int shiftedRecordLen = lastSlot.pageOffset + lastSlot.recordSize - nextSlot.pageOffset;
				memmove( (char*)tmpPage+slot.pageOffset+slot.recordSize, (char*)tmpPage+nextSlot.pageOffset, shiftedRecordLen );
			}


			// update deletedPointer
			s.slotType = Normal;
			RecordMinLen nextDeletedPointer = deletedPointer + 1;
			while(nextDeletedPointer < slotSize)
			{
				memcpy( &s, (char*)tmpPage+getSlotOffset(nextDeletedPointer), sizeof(DIRECTORYSLOT) );
				if(s.slotType == Deleted)
					break;
				nextDeletedPointer += 1;
			}
			if( s.slotType != Deleted )
			{
				nextDeletedPointer = -1;
			}
			memcpy( (char*)tmpPage + getDeletedPointerOffset(), &nextDeletedPointer, sizeof(RecordMinLen) );

			// copy and set restSize
			restSize -= localOffset;
			memcpy( (char*)tmpPage + getSlotOffset(deletedPointer), &slot, sizeof(DIRECTORYSLOT) );
			memcpy( (char*)tmpPage+slot.pageOffset, recordData, slot.recordSize );
		}
		else
		{

			DIRECTORYSLOT lastSlot;
			int lastPivot = slotSize-1;
			lastSlot.slotType = Deleted;
			while(lastPivot >= 0)
			{
				memcpy( &lastSlot, (char*)tmpPage + getSlotOffset(lastPivot), sizeof(DIRECTORYSLOT) );
				if(lastSlot.slotType != Deleted)
				{
					break;
				}
				lastPivot -= 1;
			}

			// insert new slot
			DIRECTORYSLOT *slot = (DIRECTORYSLOT *) malloc(sizeof(DIRECTORYSLOT));
			slot->recordSize = localOffset;
			slot->slotType = Normal;
			if(lastSlot.slotType != Deleted)
			{
				slot->pageOffset = lastSlot.pageOffset + lastSlot.recordSize;
			}
			else
			{
				slot->pageOffset = 0;
			}
			
			slotSize += 1;
			restSize -= (localOffset + sizeof(DIRECTORYSLOT) );
			memcpy( (char*)tmpPage + getSlotOffset(slotSize-1), slot, sizeof(DIRECTORYSLOT) );
			// copy recordData to page
			memcpy( (char*)tmpPage + slot->pageOffset, recordData, localOffset );

			// free memory
			free(slot);

			rid.pageNum = pageNum;
			rid.slotNum = slotSize - 1;
		}

		// update value back
		memcpy( (char*)tmpPage + getRestSizeOffset(), &restSize, sizeof(RecordMinLen) );
		memcpy( (char*)tmpPage + getSlotCountOffset(), &slotSize, sizeof(RecordMinLen) );

		// write to page
		fileHandle.writePage(pageNum, tmpPage);
	}

	//free pointer and return
	free(tmpPage);
	free(recordData);

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
    	free(tmpPage);
    	return  -1;
    }
    // get record offset
    DIRECTORYSLOT slot;
    memcpy( &slot, (char*)tmpPage + getSlotOffset(slotNum), sizeof(DIRECTORYSLOT) );


    RecordMinLen pageOffset =  slot.pageOffset;
    RecordMinLen tmpSize = slot.recordSize;
    SlotType slotType = slot.slotType;
    // record deleted
    if( slotType == Deleted )
    {
    	free(tmpPage);
    	return -1;
    }

    RID tmpRid;
    while( slotType == MasterPointer || slotType == SlavePointer )
    {
    	memcpy( &tmpRid, (char*)tmpPage + slot.pageOffset, sizeof(RID) );
    	pageNum = tmpRid.pageNum;
    	slotNum = tmpRid.slotNum;
    	if( fileHandle.readPage(pageNum, tmpPage) == -1 )
    	{
    	    free(tmpPage);
    		return -1;
    	}
    	memcpy( &slot, (char*)tmpPage + getSlotOffset(slotNum), sizeof(DIRECTORYSLOT) );
    	pageOffset = slot.pageOffset;
    	tmpSize = slot.recordSize;
    	slotType = slot.slotType;
    }

    int nullBytes= getActualBytesForNullsIndicator( recordSize );
    unsigned char* nullIndicator = (unsigned char*) malloc(nullBytes);
    // move to NullBytes position
    memcpy( nullIndicator, (char*)tmpPage + pageOffset + sizeof(RecordMinLen), nullBytes );

    unsigned int recordOffset = sizeof(RecordMinLen) + nullBytes + recordSize*sizeof(RecordMinLen);
    unsigned int recordActualSize = tmpSize - recordOffset;
    void* recordData = malloc(recordActualSize);
    memcpy( recordData, (char*)tmpPage+pageOffset+recordOffset, recordActualSize );

    // copy to dest (void*)data
    memcpy( data, nullIndicator, nullBytes);
    memcpy( (char*)data + nullBytes, recordData, recordActualSize );

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
    	int shiftBit = 8*nullBytes - i - 1;
    	int nullIndex = nullBytes - (int)(shiftBit/8) - 1;
    	bool isNull = nullIndicator[nullIndex] & ( 1<< (shiftBit%8) );

    	if( !isNull )
    	{
    		if( columnType == TypeVarChar )
    		{
    			int charCount;
    			memcpy( &charCount, (char*)data + offset, sizeof(int));
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
    				memcpy( &realVal, (char*)data + offset, sizeof(float) );
    				offset += sizeof(float);
    				cout<<realVal<<'	';
    			}else
    			{
    				int intVal;
    				memcpy( &intVal, (char*)data + offset, sizeof(int) );
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
RC RecordBasedFileManager::prepareRecord( const vector<Attribute> &recordDescriptor, unsigned int &localOffset, void *recordData, const void *data ) {
	int recordSize = recordDescriptor.size();
	RecordMinLen recordAttrCount = recordSize;
	// copy NULL check addr
	int nullBytes= getActualBytesForNullsIndicator( recordSize );
	unsigned char* nullIndicator = (unsigned char*) malloc(nullBytes);
	memcpy( nullIndicator, (char*) data, nullBytes );

	// malloc address offset pointer
	unsigned int recordAddrPointerSize = sizeof(RecordMinLen)*recordSize;
	void *recordAddrPointer = malloc( recordAddrPointerSize );
	memset( recordAddrPointer, 0, recordAddrPointerSize );

	// offset of record before insert (column count + recordAddrPointer + nullIndicator)
	RecordMinLen recordOffset = sizeof(RecordMinLen) + recordAddrPointerSize + nullBytes;
	// used for fetching data
	unsigned int dataOffset = nullBytes;
	// the data of record size
	unsigned int recordActualSize = 0;

	for( int i = 0; i<recordSize;i++ )
	{
		AttrType columnType = recordDescriptor[i].type;
		AttrLength columnLength =recordDescriptor[i].length;

		int shiftBit = 8*nullBytes - i - 1;
		int nullIndex = nullBytes - (int)(shiftBit/8) - 1;
		bool isNull = nullIndicator[nullIndex] & ( 1<< (shiftBit%8) );
		// NULL case
		if( isNull )
		{
			memcpy( (char*)recordAddrPointer + i*sizeof(RecordMinLen), &recordOffset, sizeof(RecordMinLen) );
			continue;
		}

		// not NULL case
		switch (columnType)
		{
			case TypeVarChar:
			{
				int charCount;
				memcpy( &charCount, (char*)data + dataOffset, sizeof(int) );
				// varchar over length
				// if( charCount > columnLength )
				// {
				// 	free(recordAddrPointer);
				// 	free(nullIndicator);
				// 	return -1;
				// 	charCount = columnLength;
				// }
				RecordMinLen varcharRecordSize = charCount*sizeof(char) + sizeof(int);
				recordOffset += varcharRecordSize;
				recordActualSize += varcharRecordSize;
				dataOffset += varcharRecordSize;
				break;
			}
			case TypeInt:
			{
				recordOffset += columnLength;
				recordActualSize += columnLength;
				dataOffset += columnLength;
				break;
			}
			case TypeReal:
			{
				recordOffset += columnLength;
				recordActualSize += columnLength;
				dataOffset += columnLength;
				break;
			}
		}
		// each time shift sizeof(RecordMinLen)
		// set recordAddrPointer
		memcpy( (char*)recordAddrPointer + i*sizeof(RecordMinLen), &recordOffset, sizeof(RecordMinLen) );
	}
	// memcpy data to recordData
	memcpy( (char*) recordData, &recordAttrCount, sizeof(RecordMinLen) );
	localOffset += sizeof(RecordMinLen);
	memcpy( (char*) recordData + localOffset, (char*)nullIndicator, nullBytes );
	localOffset += nullBytes;
	memcpy( (char*) recordData + localOffset, (char*)recordAddrPointer, recordAddrPointerSize );
	localOffset += recordAddrPointerSize;

	memcpy( (char*) recordData + localOffset, (char*) data + nullBytes, recordActualSize );
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

	// already delete
	if( deletedRecordSlotType == Deleted )
	{
		return 0;
	}
	// get slot num
	RecordMinLen slotCount;
	memcpy( &slotCount, (char*) tmpPage + getSlotCountOffset(), sizeof(RecordMinLen) );

	// get restSize
	RecordMinLen restSize;
	memcpy( &restSize, (char*) tmpPage + getRestSizeOffset(), sizeof(RecordMinLen) );
	restSize += deletedRecordSize;
	memcpy( (char*) tmpPage + getRestSizeOffset(), &restSize, sizeof(RecordMinLen) );

	// get rid
	bool shouldTreverseDeleteNode = false;
	RID treverseRid;
	if( deletedRecordSlotType == MasterPointer || deletedRecordSlotType == SlavePointer )
	{
		shouldTreverseDeleteNode = true;
		memcpy( &treverseRid, (char*)tmpPage + deletedRecordOffset, sizeof(RID) );
	}
	
	// calculate total size between slotNum+1 -> slotNum End
	// not the last slot
	if( slotNum != (slotCount-1) )
	{
		DIRECTORYSLOT nextSlot;
		int nextPivot = slotNum + 1;
		do{
				memcpy( &nextSlot, (char*)tmpPage + getSlotOffset(nextPivot), sizeof(DIRECTORYSLOT) );
				nextPivot += 1;
		}while( nextSlot.slotType == Deleted && nextPivot < slotCount );

		if( nextSlot.slotType != Deleted )
		{
			DIRECTORYSLOT lastSlot;
			int slotIdx = slotCount - 1;
			// if last is deleted move back
			do{
				memcpy( &lastSlot, (char*)tmpPage + getSlotOffset(slotIdx), sizeof(DIRECTORYSLOT) );
				slotIdx -= 1;
			}while( lastSlot.slotType == Deleted && slotIdx >= (slotNum+1) );

			// the slotNum is the last available Slot
			if( lastSlot.slotType != Deleted )
			{
				RecordMinLen shiftedLen = lastSlot.pageOffset + lastSlot.recordSize - nextSlot.pageOffset;

				// from here add deletedRecordSize to memset 0
				RecordMinLen shiftedResetOffset = lastSlot.pageOffset + lastSlot.recordSize - deletedRecordSize;

				// shift pageOffset
				memmove( (char*)tmpPage + deletedRecordOffset, (char*)tmpPage + nextSlot.pageOffset, shiftedLen );
				memset( (char*)tmpPage + shiftedResetOffset, 0, deletedRecordSize );
				// update slots after slotNum
				RecordMinLen slotIter = slotNum + 1;
				DIRECTORYSLOT s;
				while( slotIter < slotCount )
				{
					memcpy( &s, (char*)tmpPage + getSlotOffset(slotIter), sizeof(DIRECTORYSLOT) );
					if( s.slotType != Deleted )
					{
						s.pageOffset -= deletedRecordSize;
						memcpy( (char*)tmpPage + getSlotOffset(slotIter), &s, sizeof(DIRECTORYSLOT) );
					}
					slotIter += 1;
				}
			}
			else
			{
				memset( (char*)tmpPage + deletedRecordOffset, 0, deletedRecordSize );
			}
		}
		else
		{
			memset( (char*)tmpPage + deletedRecordOffset, 0, deletedRecordSize );
		}
	}
	else
	{
	 	memset( (char*)tmpPage + deletedRecordOffset, 0, deletedRecordSize );
	}

	// clear slot and save
	tmpSlot.slotType = Deleted;
	tmpSlot.pageOffset = -1;
	tmpSlot.recordSize = 0;
	memcpy( (char*)tmpPage + getSlotOffset(slotNum), &tmpSlot, sizeof(DIRECTORYSLOT) );

	// update deletedPointer
	RecordMinLen deletedPointer;
	memcpy( &deletedPointer, (char*)tmpPage + getDeletedPointerOffset(), sizeof(RecordMinLen) );
	RecordMinLen sNum = slotNum;

	if( deletedPointer == -1 || deletedPointer > sNum )
	{
		memcpy( (char*)tmpPage + getDeletedPointerOffset(), &sNum, sizeof(RecordMinLen) );
	}

	// write page
	fileHandle.writePage(pageNum, tmpPage);

	fileHandle.readPage(pageNum, tmpPage);
	memcpy(&tmpSlot, (char*)tmpPage+ getSlotOffset(slotNum), sizeof(DIRECTORYSLOT) );
	// free
	free(tmpPage);

	// if the record is pointer, it should delete recursively
	if( shouldTreverseDeleteNode )
	{
		if( deleteRecord( fileHandle, recordDescriptor, treverseRid ) != 0 )
			return -1;
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
	memcpy( &curSlot, (char*)tmpPage + getSlotOffset(slotNum), sizeof(DIRECTORYSLOT) );
	curSlot.slotType = slotType;
	memcpy( (char*)tmpPage + getSlotOffset(slotNum), &curSlot, sizeof(DIRECTORYSLOT) );

	fileHandle.writePage(pageNum, tmpPage);
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
		memcpy( &restSize, (char*) tmpPage + getRestSizeOffset(), sizeof(RecordMinLen) );
		// Copy slot to dest
		memcpy( &curSlot, (char*) tmpPage + getSlotOffset(slotNum), sizeof(DIRECTORYSLOT) );
		// Get pageNum, slotNum
		RID tmpRid;
		if( curSlot.slotType == MasterPointer || curSlot.slotType == SlavePointer )
		{
			memcpy( &tmpRid, (char*)tmpPage + curSlot.pageOffset, curSlot.recordSize );
			pageNum = tmpRid.pageNum;
			slotNum = tmpRid.slotNum;
		}

	}while( curSlot.slotType == MasterPointer || curSlot.slotType == SlavePointer );
	
	// memcpy data to recordData
	unsigned int recordFullSize = 0;
	void *recordData = malloc(PAGE_SIZE);
	SlotType curSlotType = curSlot.slotType;

	// use pointer to pointer for malloc in function
	if( prepareRecord( recordDescriptor, recordFullSize, recordData, data ) != 0 )
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
		copySize = recordFullSize;
		restSize -= recordDiff;
		if( curSlotType == Deleted )
		{
			curSlot.slotType = Normal;
			
			// update deletedSlot page offset
			updateDeletedSlotPageOffset( slotNum, curSlot, slotCount, tmpPage );

			// update deletedPointer
			RecordMinLen deletedPointer;
			memcpy( &deletedPointer, (char*)tmpPage + getDeletedPointerOffset(), sizeof(RecordMinLen) );
			
			if( deletedPointer >= slotNum )
			{
				int restSlotNum = slotCount - slotNum - 1;
				bool hasDeletedNode = false;
				if( restSlotNum > 0 )
				{
					int pivot = 0;
					DIRECTORYSLOT *afterCurSlots = new DIRECTORYSLOT[restSlotNum];
					memcpy( afterCurSlots, (char*)tmpPage + getSlotOffset(deletedPointer+1), sizeof(DIRECTORYSLOT)*restSlotNum );
					
					while( pivot < restSlotNum )
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

				memcpy( (char*)tmpPage + getDeletedPointerOffset(), &deletedPointer, sizeof(RecordMinLen) );
			}
			// end update
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
				// update deletedSlot page offset
				updateDeletedSlotPageOffset( slotNum, curSlot, slotCount, tmpPage );

				// update deletedPointer
				RecordMinLen deletedPointer;
				memcpy( &deletedPointer, (char*)tmpPage + getDeletedPointerOffset(), sizeof(RecordMinLen) );
				
				if( deletedPointer >= slotNum )
				{
					int restSlotNum = slotCount - slotNum - 1;
					bool hasDeletedNode = false;
					if( restSlotNum > 0 )
					{
						int pivot = 0;
						DIRECTORYSLOT *afterCurSlots = new DIRECTORYSLOT[restSlotNum];
						memcpy( afterCurSlots, (char*)tmpPage + getSlotOffset(deletedPointer+1), sizeof(DIRECTORYSLOT)*restSlotNum );
						
						while( pivot < restSlotNum )
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

					memcpy( (char*)tmpPage + getDeletedPointerOffset(), &deletedPointer, sizeof(RecordMinLen) );
				}
				// end update
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
	// update slot info
	memcpy( (char*)tmpPage + getSlotOffset(slotNum), &curSlot, sizeof(DIRECTORYSLOT) );

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
		memmove( (char*)tmpPage + nextSlot.pageOffset + recordDiff, (char*)tmpPage + nextSlot.pageOffset, shiftedLen );

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
		memcpy( (char*)tmpPage + curSlot.pageOffset, recordData, copySize );
	}
	else
	{
		memcpy( (char*)tmpPage + curSlot.pageOffset, &slaveRid, copySize );
	}

	memcpy( (char*)tmpPage + getRestSizeOffset(), &restSize, sizeof(RecordMinLen) );
	// write back
	fileHandle.writePage(pageNum, tmpPage);

	// free
	free(recordData);
	free(tmpPage);

	return 0;
}

RC RecordBasedFileManager::updateDeletedSlotPageOffset( RecordMinLen slotNum, DIRECTORYSLOT &curSlot, RecordMinLen slotCount, void  *tmpPage )
{
	RecordMinLen offset = 0;
	bool frontCheck = true;

	if( slotNum != (slotCount -1) )
	{
		DIRECTORYSLOT nextSlot;
		int pivot = slotNum + 1;
		do{
			memcpy( &nextSlot, (char*)tmpPage + getSlotOffset(pivot), sizeof(DIRECTORYSLOT) );
			pivot += 1;
		}while( nextSlot.slotType == Deleted && pivot < slotCount );


		// the slots after deleted slot are deleted
		if( nextSlot.slotType != Deleted )
		{
			// front traverse
			curSlot.pageOffset = nextSlot.pageOffset;
			frontCheck = false;
		}
	}

	// front traverse
	if( frontCheck )
	{
		int pivot = slotNum - 1;
		DIRECTORYSLOT prevSlot;
		bool noFrontSlot = true;

		while( pivot >=0 )
		{
			memcpy( &prevSlot, (char*)tmpPage + getSlotOffset(pivot), sizeof(DIRECTORYSLOT) );
			if( prevSlot.slotType != Deleted )
			{
				noFrontSlot = false;
				break;
			}
			pivot -= 1;
		}

		if(noFrontSlot)
		{
			curSlot.pageOffset = 0;
		}
		else
		{
			curSlot.pageOffset = prevSlot.pageOffset + prevSlot.recordSize;
		}
	}

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

	if( attributeNames.size() == 0 )
	{
		return -1;
	}

	unsigned recordSize = recordDescriptor.size();
	for( int i=0; i<recordSize;i++ )
	{
		rbfm_ScanIterator._recordName.push_back( recordDescriptor[i].name );
	}

	// check whether attributeNames contain the attribute not in recordDescriptor
	if( compOp != NO_OP )
	{
		for( int i=0;i<attributeNames.size(); i++ )
		{
			if( find( rbfm_ScanIterator._recordName.begin(), rbfm_ScanIterator._recordName.end(), attributeNames[i] ) == rbfm_ScanIterator._recordName.end() )
			{
				return -1;
			}
		}
	}
	string columnName;
	AttrType columnType;
	AttrLength columnLength;

	for( int i=0;i<recordSize;i++ )
		{
			columnName = recordDescriptor[i].name;
			columnType = recordDescriptor[i].type;
			columnLength = recordDescriptor[i].length;
		if( columnName == conditionAttribute )
		{
			rbfm_ScanIterator._columnPivot = i;
			rbfm_ScanIterator._columnType = columnType;
			break;
		}
	}
	rbfm_ScanIterator._recordDescriptor = recordDescriptor;
	rbfm_ScanIterator._conditionAttribute = conditionAttribute;
	rbfm_ScanIterator._compOp = compOp;
	rbfm_ScanIterator._value = (void*)value;
	rbfm_ScanIterator._attributeNames = attributeNames;
	rbfm_ScanIterator._fileHandlePtr = &fileHandle;

	// init load first tmpPage
	if ( rbfm_ScanIterator._fileHandlePtr->readPage( 0, rbfm_ScanIterator._tmpPage ) != 0 )
		return -1;
	return 0;
}

// read attribute
RC RecordBasedFileManager::readAttribute(FileHandle &fileHandle, const vector<Attribute> &recordDescriptor, const RID &rid, const string &attributeName, void *data){
	RecordMinLen pageNum = rid.pageNum;
	RecordMinLen slotNum = rid.slotNum;
	void *tmpPage = malloc(PAGE_SIZE);

	if( fileHandle.readPage(pageNum, tmpPage) != 0 )
	{
		free(tmpPage);
		return -1;
	}

	DIRECTORYSLOT slot;
	memcpy( &slot, (char*)tmpPage+getSlotOffset(slotNum), sizeof(DIRECTORYSLOT) );


	RID tmpRid;
	// traverse nodes til point to data
	while( slot.slotType == MasterPointer || slot.slotType == SlavePointer )
	{
		memcpy( &tmpRid, (char*)tmpPage+slot.pageOffset, slot.recordSize );
		if( (unsigned)tmpRid.pageNum != (unsigned)pageNum )
		{
			if( fileHandle.readPage( tmpRid.pageNum, tmpPage ) != 0 )
			{
				free(tmpPage);
				return -1;
			}
			pageNum = tmpRid.pageNum;
		}

		slotNum = tmpRid.slotNum;
		memcpy( &slot, (char*)tmpPage+getSlotOffset(slotNum), sizeof(DIRECTORYSLOT) );
	}

	void *tmpData = malloc(slot.recordSize);
	memcpy( tmpData, (char*)tmpPage+slot.pageOffset, slot.recordSize );

	int recordColumnLen = recordDescriptor.size();
	int nullBytes = getActualBytesForNullsIndicator( recordColumnLen );
	unsigned char* nullIndicator = (unsigned char*) malloc(nullBytes);
	memcpy( (void*) nullIndicator, (char*) tmpData + getNullBytesOffset(), nullBytes );

	string columnName;
	AttrType columnType;
	AttrLength columnLength;
	RC success = -1;
	unsigned char *destColumnNullIndicator = (unsigned char*) malloc(1);

	for( int i=0; i<recordColumnLen; i++ )
	{
		columnName = recordDescriptor[i].name;

		if( columnName == attributeName )
		{
			int shiftBit = 8*nullBytes - i - 1;
			int nullIndex = nullBytes - (int)(shiftBit/8) - 1;
			bool isNull = nullIndicator[nullIndex] & ( 1<<(shiftBit%8) );

			if( isNull )
			{
				destColumnNullIndicator[0] = 1;
				success = 0;
				break;
			}

			destColumnNullIndicator[0] = 0;
			columnType = recordDescriptor[i].type;
			columnLength = recordDescriptor[i].length;

			if( columnType == TypeVarChar )
			{
				RecordMinLen addressColumnStartOffset;
				RecordMinLen addressColumnEndOffset;
				if( getColumnStartAndEndPivot( addressColumnStartOffset, addressColumnEndOffset, i, nullBytes, tmpData, recordDescriptor ) != 0 )
				{
					break;
				}

				unsigned columnSize = addressColumnEndOffset - addressColumnStartOffset;
				memcpy( (char*)data+1, (char*)tmpData+addressColumnStartOffset, columnSize );

				success = 0;
				break;
			}
			else
			{
				RecordMinLen addressColumnEndOffset;
				RecordMinLen addressPointerOffset = getNullBytesOffset() + nullBytes + i*sizeof(RecordMinLen);
				memcpy( &addressColumnEndOffset, (char*)tmpData+addressPointerOffset, sizeof(RecordMinLen) );

				memcpy( (char*)data+1, (char*)tmpData+addressColumnEndOffset-columnLength, columnLength );
				success = 0;
				break;
			}
		}
	}

	// copy null indicator to data
	memcpy( data, destColumnNullIndicator, 1 );
	free(destColumnNullIndicator);
	free(nullIndicator);
	free(tmpData);
	free(tmpPage);
	return success;

}

// RBFM_ScanIterator

RBFM_ScanIterator::RBFM_ScanIterator(){
	_isFirstIter = true;
	_value = 0;
	_tmpPage = malloc(PAGE_SIZE);
}

RBFM_ScanIterator::~RBFM_ScanIterator(){
}

RC RBFM_ScanIterator::close(){
//	_fileHandlePtr->_handler->close();
	free(_tmpPage);
	return 0;
}
RC RBFM_ScanIterator::getNextRecord(RID &rid, void *data){
	// check rbfm pointer
	unsigned pageNum = 0;
	unsigned slotNum = 0;
	int maxPage = _fileHandlePtr->getNumberOfPages() - 1;

	if( !_isFirstIter )
	{
		pageNum = _cursor.pageNum;
		slotNum = _cursor.slotNum;

		unsigned ridPageNum = rid.pageNum;
		if( ridPageNum != pageNum )
		{
			if( (int)pageNum > maxPage || _fileHandlePtr->readPage(pageNum, _tmpPage) != 0 )
			{
				return -1;
			}
		}
	}

	RecordMinLen curTotalSlot;
	RecordMinLen restSize;
	memcpy( &curTotalSlot, (char*)_tmpPage + getSlotCountOffset(), sizeof(RecordMinLen) );

	if( (unsigned)slotNum >= (unsigned)curTotalSlot )
	{
		// no more data to search for
		if( maxPage == (int)pageNum )
		{
			return -1;
		}
		// traverse next page
		else
		{
			pageNum += 1;
			slotNum = 0;
			if( _fileHandlePtr->readPage(pageNum, _tmpPage) != 0 )
			{
				return -1;
			}
			memcpy( &curTotalSlot, (char*)_tmpPage + getSlotCountOffset(), sizeof(RecordMinLen) );
		}

	}
	unsigned recordMaxSize = getRecordSize( _recordDescriptor );
	void *tmpData = malloc(PAGE_SIZE);
	RID tmpRid;
	DIRECTORYSLOT tmpSlot;
	// traverse records in page
	unsigned curSlotId = slotNum;
	unsigned curPageId = pageNum;
	bool notScan = true;
	while(notScan && curTotalSlot > 0)
	{
		if( (int)curPageId > maxPage )
		{
			break;
		}
		// read new page
		if( curPageId != pageNum )
		{
			_fileHandlePtr->readPage(curPageId, _tmpPage);
			memcpy( &curTotalSlot, (char*)_tmpPage + getSlotCountOffset(), sizeof(RecordMinLen) );
			while( curTotalSlot == 0 && (int)curPageId < maxPage )
			{
				curPageId += 1;
				_fileHandlePtr->readPage(curPageId, _tmpPage);
				memcpy( &curTotalSlot, (char*)_tmpPage + getSlotCountOffset(), sizeof(RecordMinLen) );
				curSlotId = 0;
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
		memcpy( &tmpSlot, (char*)_tmpPage+getSlotOffset(curSlotId), sizeof(DIRECTORYSLOT) );
		SlotType slotType = tmpSlot.slotType;
		// traverse only when the slotType is Normal or MasterPointer
		if( slotType == Normal || slotType == MasterPointer )
		{
			if( checkRecord(tmpRid, tmpData) == 0 )
			{
				notScan = false;
				break;
			}
		}

		// iteration params updating
		if( (unsigned)(curSlotId + 1) < (unsigned)curTotalSlot )
		{
			curSlotId += 1;
		}
		else{
			curPageId += 1;
			curSlotId = 0;
		}
	}

	RC success = RBFM_EOF;

	_cursor.pageNum = tmpRid.pageNum;
	_cursor.slotNum = tmpRid.slotNum+1;
	if( !notScan )
	{
		_isFirstIter = false;
		rid.pageNum = tmpRid.pageNum;
		rid.slotNum = tmpRid.slotNum;
		prepareRecord( tmpData, data );
		success = 0;
	}
	free(tmpData);
	return success;
}

RC RBFM_ScanIterator::prepareRecord(void *fetchedData, void *data){
	int needColumnSize = _attributeNames.size();

	unsigned recordSize = _recordDescriptor.size();
	vector<string> recordName;
	RC success = 0;

	int nullBytes= getActualBytesForNullsIndicator( recordSize );
	unsigned char* nullIndicator = (unsigned char*) malloc(nullBytes);
	memcpy( (void*) nullIndicator, (char*) fetchedData + getNullBytesOffset(), nullBytes );

	int selectNullBytes = getActualBytesForNullsIndicator( needColumnSize );
	unsigned char* selectNullIndicator = (unsigned char*) malloc(selectNullBytes);
	int base10NullBytes[selectNullBytes];
	memset( base10NullBytes, 0, sizeof(int)*selectNullBytes );
	unsigned newRecordOffset = selectNullBytes;

	for( int i=0; i< needColumnSize;i++ )
	{
		auto it = find( _recordName.begin(), _recordName.end(), _attributeNames[i] );
		if( it==_recordName.end() )
		{
			success = -1;
			break;
		}
		auto columnIndex = distance( _recordName.begin(), it );
		int shiftBit = 8*nullBytes - columnIndex - 1;
		int nullIndex = nullBytes - (int)(shiftBit/8) - 1;
		bool isNull = nullIndicator[nullIndex] & ( 1<<(shiftBit%8) );

		if( isNull )
		{
			int mod = i/8;
			base10NullBytes[mod] += pow( 2, shiftBit );
		}
		else
		{
			RecordMinLen addressColumnStartOffset;
			RecordMinLen addressColumnEndOffset;
			getColumnStartAndEndPivot( addressColumnStartOffset, addressColumnEndOffset, (int) columnIndex, nullBytes, fetchedData, _recordDescriptor );

			unsigned destColumnSize = addressColumnEndOffset - addressColumnStartOffset;
			memcpy( (char*)data+newRecordOffset, (char*)fetchedData + addressColumnStartOffset, destColumnSize );
			newRecordOffset += destColumnSize;
		}

	}

	//!! here may modify
	for(int i = 0; i<selectNullBytes;i++){
		selectNullIndicator[i] = base10NullBytes[i];
	}

	memcpy( data, selectNullIndicator, selectNullBytes );

	free(selectNullIndicator);
	free(nullIndicator);
	return success;
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
	memcpy( nullIndicator, (char*)data+getNullBytesOffset(), nullBytes );


	unsigned int shiftBit;
	bool isNull = false;


	// which one is dest column
	unsigned columnPivot = 0;

	shiftBit = 8*nullBytes - _columnPivot - 1;
	// select specific nullIndicator
	int nullIndex = nullBytes - (int)(shiftBit/8) - 1;
	isNull = nullIndicator[nullIndex] & ( 1 << shiftBit );
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

	RecordMinLen addressColumnStartOffset;
	RecordMinLen addressColumnEndOffset;

	getColumnStartAndEndPivot( addressColumnStartOffset, addressColumnEndOffset, (int) _columnPivot, nullBytes, data, _recordDescriptor );
	unsigned destColumnSize = addressColumnEndOffset - addressColumnStartOffset;

	// copy destColumnData
	destColumnData = malloc(destColumnSize);
	memcpy( destColumnData, (char*)data+addressColumnStartOffset, destColumnSize );

	// prepare data for varchar
	int compLen;
	int destLen;
	void *compColumn;
	void *destColumn;
	if( _columnType == TypeVarChar )
	{
		memcpy( &compLen, (char*)_value, sizeof(int) );
		memcpy( &destLen, (char*)destColumnData, sizeof(int) );
		compColumn = malloc(compLen);
		destColumn = malloc(destLen);

		memcpy( compColumn, (char*)_value+sizeof(int), compLen );
		memcpy( destColumn, (char*)destColumnData+sizeof(int), destLen );
	}
	else
	{
		int allocateSize = _columnType == TypeInt ? sizeof(int) : sizeof(float);

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
			if( _columnType == TypeVarChar )
			{
				if ( strcmp( (char*)destColumn, (char*)compColumn ) < 0 )
				{
					success = 0;
					break;
				}
			}
			else
			{
				if( _columnType == TypeInt )
				{
					compBool = ( *((int*)destColumn) < *((int*)compColumn) );
				}
				else
				{
					compBool = ( *((float*)destColumn) < *((float*)compColumn) );
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
			if( _columnType == TypeVarChar )
			{
				if ( strcmp( (char*)destColumn, (char*)compColumn ) <= 0 )
				{
					success = 0;
					break;
				}
			}
			else
			{
				if( _columnType == TypeInt )
				{
					compBool = ( *((int*)destColumn) <= *((int*)compColumn) );
				}
				else
				{
					compBool = ( *((float*)destColumn) <= *((float*)compColumn) );
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
			if( _columnType == TypeVarChar )
			{
				if ( strcmp( (char*)destColumn, (char*)compColumn ) > 0 )
				{
					success = 0;
					break;
				}
			}
			else
			{
				if( _columnType == TypeInt )
				{
					compBool = ( *((int*)destColumn) > *((int*)compColumn) );
				}
				else
				{
					compBool = ( *((float*)destColumn) > *((float*)compColumn) );
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
			if( _columnType == TypeVarChar )
			{
				if ( strcmp( (char*)destColumn, (char*)compColumn ) >= 0 )
				{
					success = 0;
					break;
				}
			}
			else
			{
				if( _columnType == TypeInt )
				{
					compBool = ( *((int*)destColumn) >= *((int*)compColumn) );
				}
				else
				{
					compBool = ( *((float*)destColumn) >= *((float*)compColumn) );
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
			if( _columnType == TypeVarChar )
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
				if( _columnType == TypeInt )
				{
					compBool = ( *((int*)destColumn) != *((int*)compColumn) );
				}
				else
				{
					compBool = ( *((float*)destColumn) != *((float*)compColumn) );
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
    if( _fileHandlePtr->readPage(pageNum, tmpPage) == -1 )
    {
    	free(tmpPage);
    	return  -1;
    }
    // get record offset
    DIRECTORYSLOT slot;
    memcpy( (void*) &slot, (char*)tmpPage + getSlotOffset(slotNum), sizeof(DIRECTORYSLOT) );


    RecordMinLen pageOffset =  slot.pageOffset;
    RecordMinLen tmpSize = slot.recordSize;
    SlotType slotType = slot.slotType;

    // record deleted
    if( slotType == Deleted )
    {
    	free(tmpPage);
    	return -1;
    }
    RID tmpRid;
    while( slotType == MasterPointer || slotType == SlavePointer )
    {
    	memcpy( &tmpRid, (char*)tmpPage + slot.pageOffset, sizeof(RID) );
    	
    	if( tmpRid.pageNum != pageNum )
    	{
    		if( _fileHandlePtr->readPage(tmpRid.pageNum, tmpPage) == -1 )
    		{
    	    	free(tmpPage);
    			return -1;
    		}
    		pageNum = tmpRid.pageNum;
    	}
    	
    	slotNum = tmpRid.slotNum;

    	memcpy( (void*) &slot, (char*)tmpPage + getSlotOffset(slotNum), sizeof(DIRECTORYSLOT) );
    	pageOffset = slot.pageOffset;
    	tmpSize = slot.recordSize;
    	slotType = slot.slotType;
    }

    // copy to dest
    memcpy( data, (char*)tmpPage+pageOffset, tmpSize );

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

// accessory function
RC getColumnStartAndEndPivot(RecordMinLen &addressColumnStartOffset, RecordMinLen &addressColumnEndOffset, int columnIndex, int nullBytes, void *data, const vector<Attribute> &recordDescriptor){
	RecordMinLen addressPointerOffset = getNullBytesOffset() + nullBytes + columnIndex*sizeof(RecordMinLen);
	int recordSize = recordDescriptor.size();
	memcpy( &addressColumnEndOffset, (char*)data+addressPointerOffset, sizeof(RecordMinLen) );
	addressColumnStartOffset = getNullBytesOffset() + nullBytes + recordSize*sizeof(RecordMinLen);
	if(columnIndex != 0)
	{
		unsigned addressPrevPointerOffset = getNullBytesOffset() + nullBytes + (columnIndex-1)*sizeof(RecordMinLen);
		memcpy( &addressColumnStartOffset, (char*)data+addressPrevPointerOffset, sizeof(RecordMinLen) );
	}
	return 0;
}
