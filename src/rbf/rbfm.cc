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
	//
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

	// maximum size of the record
	unsigned int maxRecordSize;

	for( int i = 0; i<recordSize;i++ )
	{
		AttrType columnType = recordDescriptor[i].type;
		AttrLength columnLength =recordDescriptor[i].length;

		// calculate max length
		maxRecordSize += columnLength;

		bool isNull = nullIndicator[0] & ( 1<<( recordSize - i - 1) );
		// NULL case
		if( isNull )
		{
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
				if( charCount > columnLength )
				{
					free(recordAddrPointer);
					free(nullIndicator);
					return -1;
				}
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
	unsigned int localOffset = 0;
	void *recordData = malloc(recordOffset);
	// copy data to recordData
	memcpy( (char*)recordData, &recordAttrCount, sizeof(RecordMinLen) );
	localOffset += sizeof(RecordMinLen);
	memcpy( (char*)recordData + localOffset, nullIndicator, nullBytes );
	localOffset += nullBytes;
	memcpy( (char*)recordData + localOffset, (char*)recordAddrPointer, recordAddrPointerSize );
	localOffset += recordAddrPointerSize;

	memcpy( (char*)recordData + localOffset, (char*) data + nullBytes, recordActualSize );
	localOffset += recordActualSize;
	// test record copy work
//	RecordMinLen *s = new RecordMinLen;
//	memcpy(s, recordData, sizeof(RecordMinLen));
//	cout<<"Len: "<<*s<<endl;
//
//	cout<<"EmpLen:";
//	int ccout;
//	memcpy( (void*) &ccout, (char*)recordData+localOffset, sizeof(int) );
//	cout<<ccout;
//	char *sss = new char[ccout];
//	memcpy( (void*) sss, (char*)recordData+localOffset+sizeof(int), ccout);
//	for(int i = 0; i<ccout;i++)
//	{
//		cout<<*(sss+i);
//	}
//	cout<<"POS:"<<endl;
//	RecordMinLen r;
//	memcpy( &r, recordData+sizeof(RecordMinLen)+nullBytes, sizeof(RecordMinLen) );
//	cout<<r;
//	cout<<endl;

	// find a page or append page
	// store tmpPage
	void *tmpPage = malloc(PAGE_SIZE);

	for(int page_id=0; page_id < (int) fileHandle.getNumberOfPages();page_id++){
		if( fileHandle.readPage(page_id, tmpPage) )
		{
			//get the last bool
			RecordMinLen remainSize;
			memcpy( (void*) &remainSize, (void*)tmpPage + PAGE_SIZE-sizeof(RecordMinLen) , sizeof(RecordMinLen) );
			// this page is full continue
			// should add slot size to record size
			if( remainSize < localOffset + sizeof(DIRECTORYSLOT) )
			{
				continue;
			}
			// select pageNum
			shouldAppendPage = false;
			pageNum = page_id;
			break;
		}
	}

	// insert data into new page
	if( shouldAppendPage )
	{
		// init empty page
		memset(tmpPage, 0, PAGE_SIZE);
		// create slot
		DIRECTORYSLOT *slot = new DIRECTORYSLOT;
		slot->recordSize = localOffset;
		slot->pageOffset = 0;
		// create for storing rest of the bytes
		// 2 * 2bytes (1 for note rest of bytes, one for number of slot)
		unsigned int directorySize = sizeof(DIRECTORYSLOT) + 2*sizeof(RecordMinLen);
		RecordMinLen restSize = PAGE_SIZE - directorySize - localOffset;
		RecordMinLen slotSize = 1;

		void *directory = malloc(directorySize);
		int tmpOffset = 0;
		memcpy( directory + tmpOffset, (void*)slot, sizeof(DIRECTORYSLOT) );
		tmpOffset += sizeof(DIRECTORYSLOT);
		memcpy(directory + tmpOffset, &slotSize, sizeof(RecordMinLen) );
		tmpOffset += sizeof(RecordMinLen);
		memcpy( directory + tmpOffset,  &restSize, sizeof(RecordMinLen) );
		//copy directory to tmpPage
		memcpy(tmpPage + (PAGE_SIZE-directorySize), directory, directorySize);
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
		memcpy( (void*) &restSize, tmpPage + PAGE_SIZE - sizeof(RecordMinLen), sizeof(RecordMinLen) );
		memcpy( (void*) &slotSize, tmpPage + PAGE_SIZE - 2*sizeof(RecordMinLen), sizeof(RecordMinLen) );

		DIRECTORYSLOT lastSlot;
		memcpy( (void*) &lastSlot, tmpPage + PAGE_SIZE - 2*sizeof(RecordMinLen) - (slotSize-1)*sizeof(DIRECTORYSLOT), sizeof(DIRECTORYSLOT) );

		// get curr slotSize

		DIRECTORYSLOT *slot = new DIRECTORYSLOT;
		slot->recordSize = localOffset;
		slot->pageOffset = lastSlot.pageOffset + lastSlot.recordSize;

		slotSize += 1;
		restSize -= (localOffset + sizeof(DIRECTORYSLOT) );
		// update value back
		memcpy( tmpPage + PAGE_SIZE - sizeof(RecordMinLen), (void*) &restSize, sizeof(RecordMinLen) );
		memcpy( tmpPage + PAGE_SIZE - 2*sizeof(RecordMinLen), (void*) &slotSize, sizeof(RecordMinLen) );
		memcpy( tmpPage + PAGE_SIZE - 2*sizeof(RecordMinLen) - (slotSize-1)*sizeof(DIRECTORYSLOT), slot, sizeof(DIRECTORYSLOT) );
		// copy recordData to page
		memcpy( tmpPage + slot->pageOffset, recordData, localOffset );

		// write to page
		fileHandle.writePage(pageNum, tmpPage);
		rid.pageNum = pageNum;
		rid.slotNum = slotSize - 1;

		// free memory
		free(slot);
	}
	//free pointer and return
	cout<<"Record Len:"<<localOffset<<endl;
	free(tmpPage);
	free(recordAddrPointer);
	free(nullIndicator);
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
    	cout<<"Can not read page !"<<endl;
    	return  -1;
    }
    // get record offset
    DIRECTORYSLOT slot;
    memcpy( (void*) &slot, tmpPage + PAGE_SIZE - 2*sizeof(RecordMinLen) - (slotNum+1)*sizeof(DIRECTORYSLOT), sizeof(DIRECTORYSLOT) );


    RecordMinLen pageOffset =  slot.pageOffset;
    RecordMinLen tmpSize = slot.recordSize;
    int nullBytes= getActualBytesForNullsIndicator( recordSize );
    unsigned char* nullIndicator = (unsigned char*) malloc(nullBytes);
    // move to NullBytes position
    memcpy( (void*) nullIndicator, (void*)tmpPage + pageOffset+sizeof(RecordMinLen), nullBytes );

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
	int nullBytes= getActualBytesForNullsIndicator( recordSize );
    unsigned char* nullIndicator = (unsigned char*) malloc(nullBytes);
    memcpy( (void*) nullIndicator, (void*) data, nullBytes );

    // loop record
    unsigned int offset = nullBytes;
    for(int i = 0; i<recordSize;i++)
    {
    	string columnName = recordDescriptor[i].name;
    	AttrType columnType = recordDescriptor[i].type;
    	AttrLength columnLength = recordDescriptor[i].length;

    	cout<<columnName<<':'<<'	';
    	bool isNull = nullIndicator[0] & ( 1<<( recordSize - i - 1) );

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
    				for(int i=0; i<charCount ; i++)
    				{
    					cout<<charVal[i];
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
    		cout<<'NULL'<<'	';
    	}

    }

    cout<<endl;
    free(nullIndicator);
    return 0;
}

// accessory functions
// Calculate actual bytes for nulls-indicator for the given field counts
int getActualBytesForNullsIndicator(int fieldCount) {
    return ceil((double) fieldCount / CHAR_BIT);
}
