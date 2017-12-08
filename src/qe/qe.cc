
#include "qe.h"

// start filter
Filter::Filter(Iterator* input, const Condition &condition) {
	_inputIterator = input;
	_condition = condition;

	_inputIterator->getAttributes(_attributes);

	_attrPosition = getAttributePosition(_attributes, _condition.lhsAttr);
	_filterAttribute = _attributes[_attrPosition];
}

Filter::~Filter(){
	_inputIterator = NULL;
}

RC Filter::getNextTuple(void *data){
	RC success = -1;

	void *columnData = malloc(PAGE_SIZE);

	while( _inputIterator->getNextTuple(data) != QE_EOF )
	{
		// fetch attr from

		int notNull = getColumnData(data, columnData, _attributes, _attrPosition);

		// NO_OP and NULL can return
		if( _condition.op == NO_OP )
		{
			success = 0;
			break;
		}
		else
		{
			if(notNull == -1)
			{
				if( _condition.op ==  EQ_OP )
				{
					if(_condition.rhsValue.data == NULL)
					{
						success = 0;
						break;
					}

				}
				continue;
			}
		}

		// fetch data first
		if( _filterAttribute.type == TypeVarChar )
		{
			int charLen;
			memcpy( &charLen, _condition.rhsValue.data, sizeof(int) );
			string compVarchar = string( (char*)((char*)_condition.rhsValue.data+sizeof(int)), charLen );
			int fetchedCharLen;
			memcpy( &fetchedCharLen, columnData, sizeof(int) );
			string fetchedVarchar = string( (char*)((char*)columnData+sizeof(int)), fetchedCharLen );

			int compFlag = fetchedVarchar.compare(compVarchar);

			switch(_condition.op){
				case EQ_OP:
					if( compFlag == 0 ){
						success = 0;
					}
					break;
				case LT_OP:
					if( compFlag < 0 ){
						success = 0;
					}
					break;
				case LE_OP:
					if( compFlag <= 0 ){
						success = 0;
					}
					break;
				case GT_OP:
					if( compFlag > 0 ){
						success = 0;
					}
					break;
				case GE_OP:
					if( compFlag >= 0 ){
						success = 0;
					}
					break;
				case NE_OP:
					if( compFlag != 0 ){
						success = 0;
					}
					break;
			}
		}
		else
		{
			if( _filterAttribute.type == TypeInt )
			{
				int compVal;
				memcpy( &compVal, _condition.rhsValue.data, sizeof(int) );
				int fetchedVal;
				memcpy( &fetchedVal, columnData, sizeof(int) );

				switch(_condition.op){
					case EQ_OP:
						if( fetchedVal == compVal ){
							success = 0;
						}
						break;
					case LT_OP:
						if( fetchedVal < compVal ){
							success = 0;
						}
						break;
					case LE_OP:
						if( fetchedVal <= compVal ){
							success = 0;
						}
						break;
					case GT_OP:
						if( fetchedVal > compVal ){
							success = 0;
						}
						break;
					case GE_OP:
						if( fetchedVal >= compVal ){
							success = 0;
						}
						break;
					case NE_OP:
						if( fetchedVal != compVal ){
							success = 0;
						}
						break;
				}

			}
			else
			{
				float compVal;
				memcpy( &compVal, _condition.rhsValue.data, sizeof(float) );
				float fetchedVal;
				memcpy( &fetchedVal, columnData, sizeof(float) );

				switch(_condition.op){
					case EQ_OP:
						if( fetchedVal == compVal ){
							success = 0;
						}
						break;
					case LT_OP:
						if( fetchedVal < compVal ){
							success = 0;
						}
						break;
					case LE_OP:
						if( fetchedVal <= compVal ){
							success = 0;
						}
						break;
					case GT_OP:
						if( fetchedVal > compVal ){
							success = 0;
						}
						break;
					case GE_OP:
						if( fetchedVal >= compVal ){
							success = 0;
						}
						break;
					case NE_OP:
						if( fetchedVal != compVal ){
							success = 0;
						}
						break;
				}
			}
		}

		// find the tuple
		if( success == 0 )
		{
			break;
		}
	}
	free(columnData);
	return success;
}

void Filter::getAttributes(vector<Attribute> &attrs) const{
	attrs = _attributes;
}

// start project
Project::Project(Iterator *input, const vector<string> &attrNames){
	_inputIterator = input;
	input->getAttributes(_attributes);
	// filter and put in projectAttr
	Attribute tmpAttr;
	for(int i=0;i<_attributes.size();i++)
	{
		tmpAttr = _attributes[i];
		if( find( attrNames.begin(), attrNames.end(), tmpAttr.name) != attrNames.end() )
		{
			_projectAttributes.push_back(tmpAttr);
		}
	}
	// necessary for sorting
	sort( _projectAttributes.begin(), _projectAttributes.end(), sortAttr) ;
}

RC Project::getNextTuple(void *data){
	RC success = -1;
	void *tmpData = malloc(PAGE_SIZE);

	if( _inputIterator->getNextTuple(tmpData) != QE_EOF )
	{
		prepareData(data, tmpData);
		success = 0;
	}

	free(tmpData);
	return success;
}

void Project::prepareData( void *data, void *tmpData){
	int nullBytes = getActualBytesForNullsIndicator(_attributes.size());
	int offset = nullBytes;
	unsigned char *nullIndicator = (unsigned char*) malloc(nullBytes);
	memcpy( nullIndicator, tmpData, nullBytes );

	// for project null bytes
	int selectNullBytes = getActualBytesForNullsIndicator(_projectAttributes.size());
	unsigned char* selectNullIndicator = (unsigned char*) malloc(selectNullBytes);
	int base10NullBytes[selectNullBytes];
	memset( base10NullBytes, 0, sizeof(int)*selectNullBytes );
	int selectOffset = selectNullBytes;

	Attribute tmpAttr;
	for(int i=0; i<_attributes.size();i++)
	{
		tmpAttr = _attributes[i];
		int attrPosition = getAttributePosition(_projectAttributes, tmpAttr.name);
		if(attrPosition < 0)
		{
			continue;
		}
		// check null
		int shiftedBit = 8*nullBytes - i - 1;
		int nullIndex = nullBytes - (int)(shiftedBit/8) - 1;
		bool isNull = nullIndicator[nullIndex] & ( 1<<(shiftedBit%8) );

		if(isNull)
		{
			int mod = attrPosition/8;
			int selectShiftBit = 8*selectNullBytes - attrPosition - 1;
			base10NullBytes[mod] += pow(2, (selectShiftBit%8) );
			continue;
		}
		// copy data
		int selectedColumnSize = getColumnData( tmpData, ((char*)data+selectOffset), _attributes, i);

		selectOffset += selectedColumnSize;
	}

	// copy null bytes
	for(int i=0; i<selectNullBytes;i++)
	{
		selectNullIndicator[i] = base10NullBytes[i];
	}

	memcpy( data, selectNullIndicator, selectNullBytes );

	free(nullIndicator);
	free(selectNullIndicator);
}

void Project::getAttributes(vector<Attribute> &attrs) const{
	attrs = _projectAttributes;
}

Project::~Project(){
	_inputIterator = NULL;
}

// BNLJoin
BNLJoin::BNLJoin(Iterator *leftIn,            // Iterator of input R
               TableScan *rightIn,           // TableScan Iterator of input S
               const Condition &condition,   // Join condition
               const unsigned numPages       // # of pages that can be loaded into memory,
               ){

	leftIn->getAttributes(_leftAttributes);
	rightIn->getAttributes(_rightAttributes);

	_leftIn = leftIn;
	_rightIn = rightIn;
	_numPages = numPages;
	_condition = condition;

	_shouldReloadMap = true;
	_shouldGetNextRight = true;

	_attributes = _leftAttributes;
	_attributes.insert( _attributes.end(), _rightAttributes.begin(), _rightAttributes.end() );
}

BNLJoin::~BNLJoin(){
	_leftIn = NULL;
	_rightIn = NULL;
}

RC BNLJoin::getNextTuple(void *data){
	RC success = -1;
	void *rightData = malloc(PAGE_SIZE);
	void *rightColumnData = malloc(PAGE_SIZE);

	int attrPosition = getAttributePosition(_rightAttributes, _condition.rhsAttr);
	Attribute attr = _rightAttributes[attrPosition];

	while(true)
	{
		if(_shouldReloadMap)
		{
			if( loadLeftMap() == -1 ){
				free(rightData);
				free(rightColumnData);
				return success;
			}

			_shouldReloadMap = false;
		}

		if(_shouldGetNextRight)
		{
			while(_rightIn->getNextTuple(rightData) != QE_EOF)
			{
				int columnSize = getColumnData(rightData, rightColumnData, _rightAttributes, attrPosition);
				if( columnSize < -1 )
				{
					string nullKey = NULL_KEY;
					auto it = _leftMap.find(nullKey);
					if( it != _leftMap.end() )
					{
						int leftValueSize = _leftMap[nullKey].size();
						if(leftValueSize > 0)
						{
							_curLeftValues = _leftMap[nullKey];
							JoinMapValue leftR = _curLeftValues.back();
							_curLeftValues.pop_back();
							createJoinRecord( data, leftR, rightData );

							success = 0;
							free(rightData);
							free(rightColumnData);

							if(leftValueSize == 1)
							{
								_shouldGetNextRight = true;
							}

							return success;
						}
					}
				}
				// non NULL case
				else
				{
					// prepare rightKey
					string rightKey;
					if(attr.type == TypeVarChar)
					{
						int charLen;
						memcpy( &charLen, rightColumnData, sizeof(int) );
						rightKey = string( ( (char*)rightColumnData+4 ), charLen );
					}
					else
					{
						if(attr.type == TypeInt)
						{
							int rightVal;
							memcpy( &rightVal, rightColumnData, sizeof(int) );
							rightKey = to_string(rightVal);
						}
						else
						{
							float rightVal;
							memcpy( &rightVal, rightColumnData, sizeof(float) );
							rightKey = to_string(rightVal);
						}
					}
					// start get hashmap vector
					auto it = _leftMap.find(rightKey);
					if( it != _leftMap.end() )
					{
						int leftValueSize = _leftMap[rightKey].size();
						if(leftValueSize > 0)
						{
							_curLeftValues = _leftMap[rightKey];
							JoinMapValue leftR = _curLeftValues.back();
							_curLeftValues.pop_back();
							createJoinRecord( data, leftR, rightData );

							success = 0;
							free(rightData);
							free(rightColumnData);

							if(leftValueSize == 1)
							{
								_shouldGetNextRight = true;
							}

							return success;
						}
					}
				}
			}
		}
		else
		{
			JoinMapValue leftR = _curLeftValues.back();
			_curLeftValues.pop_back();
			createJoinRecord( data, leftR, rightData );
			if(_curLeftValues.size() == 0)
			{
				_shouldGetNextRight = true;
			}

			success = 0;
			free(rightData);
			free(rightColumnData);

			return success;
		}

		//_rightIn should rescan
		_rightIn->iter->_rbf_scanIter._isFirstIter = true;
		_shouldReloadMap = true;

	}

	free(rightData);
	free(rightColumnData);
	return success;
}

void BNLJoin::getAttributes(vector<Attribute> &attrs) const{
	attrs = _attributes;
}

RC BNLJoin::loadLeftMap(){
	RC success = -1;

	// init _leftMap
	_freeSize = _numPages*PAGE_SIZE;
	_leftMap.clear();

	void *data = malloc(PAGE_SIZE);
	int recordSize;
	JoinMapValue mapValue;

	int attrPosition = getAttributePosition(_leftAttributes, _condition.lhsAttr);
	Attribute attr = _leftAttributes[attrPosition];
	int nullBytes = getActualBytesForNullsIndicator(_leftAttributes.size());

	void *columnData = malloc(PAGE_SIZE);

	while( _leftIn->getNextTuple(data) != QE_EOF )
	{
		success = 0;
		recordSize = getRecordSize(data, _leftAttributes);
		mapValue.size = recordSize;
		mapValue.data = string( (char*)data, recordSize+nullBytes );

		int joinColumnSize = getColumnData(data, columnData, _leftAttributes, attrPosition);

		// null case using special key NULL_KEY
		if(joinColumnSize == -1)
		{
			string nullKey = NULL_KEY;
			auto it = _leftMap.find(nullKey);
			if( it != _leftMap.end() )
			{
				_leftMap[nullKey].push_back(mapValue);
			}
			else
			{
				vector<JoinMapValue> jmap;
				jmap.push_back(mapValue);
				_leftMap[nullKey] = jmap;
			}

			_freeSize -= recordSize;
			if(_freeSize < 0)
			{
				success = 0;
				break;
			}
			continue;
		}

		// Non-Null key
		string mapKey;
		if(attr.type == TypeVarChar)
		{
			int charLen;
			memcpy( &charLen, (char*)columnData, sizeof(int) );
			mapKey = string( ((char*)columnData+sizeof(int)), charLen );
		}
		else
		{
			if(attr.type == TypeInt)
			{
				int joinKeyVal;
				memcpy( &joinKeyVal, columnData, sizeof(int) );
				mapKey = to_string(joinKeyVal);
			}
			else
			{
				float joinKeyVal;
				memcpy( &joinKeyVal, columnData, sizeof(float) );
				mapKey = to_string(joinKeyVal);
			}
		}

		auto it = _leftMap.find(mapKey);
		if(it != _leftMap.end())
		{
			_leftMap[mapKey].push_back(mapValue);
		}
		else
		{
			vector<JoinMapValue> jmap;
			jmap.push_back(mapValue);
			_leftMap[mapKey] = jmap;
		}

		_freeSize -= recordSize;
		if(_freeSize < 0)
		{
			success = 0;
			break;
		}
	}

	free(data);
	free(columnData);
	return success;
}

void BNLJoin::createJoinRecord(void *data, JoinMapValue leftValue, const void *rightData){
	// prepare left data
	const char *leftData  = leftValue.data.c_str();

	int leftAttributeSize = _leftAttributes.size();
	int rightAttributeSize = _rightAttributes.size();
	int leftNullBytes = getActualBytesForNullsIndicator( leftAttributeSize );
	int rightNullBytes = getActualBytesForNullsIndicator( rightAttributeSize );

	int combinedSize = leftAttributeSize + rightAttributeSize;
	int nullBytes = getActualBytesForNullsIndicator(combinedSize);

	// init null bytes
	unsigned char *leftNullIndicator = (unsigned char*) malloc(leftNullBytes);
	unsigned char *rightNullIndicator = (unsigned char*) malloc(rightNullBytes);
	unsigned char *nullIndicator = (unsigned char*) malloc(nullBytes);
	int base10NullBytes[nullBytes];
	memset( base10NullBytes, 0, sizeof(int)*nullBytes );

	memcpy( leftNullIndicator, leftData, leftNullBytes );
	memcpy( rightNullIndicator, rightData, rightNullBytes );

	int joinIdx = 0;
	for(int i=0;i<_leftAttributes.size();i++)
	{
		// check null
		int shiftedBit = 8*leftNullBytes - i - 1;
		int nullIndex = leftNullBytes - (int)(shiftedBit/8) - 1;
		bool isNull = leftNullIndicator[nullIndex] & ( 1<<(shiftedBit%8) );

		if(isNull)
		{
			int mod = joinIdx/8;
			int joinShiftBit = 8*nullBytes- joinIdx - 1;
			base10NullBytes[mod] += pow( 2, (joinShiftBit%8) );
			continue;
		}

		joinIdx += 1;
	}

	for(int i=0;i<_rightAttributes.size();i++)
	{
		// check null
		int shiftedBit = 8*rightNullBytes - i - 1;
		int nullIndex = rightNullBytes - (int)(shiftedBit/8) - 1;
		bool isNull = rightNullIndicator[nullIndex] & ( 1<<(shiftedBit%8) );

		if(isNull)
		{
			int mod = joinIdx/8;
			int joinShiftBit = 8*nullBytes- joinIdx - 1;
			base10NullBytes[mod] += pow( 2, (joinShiftBit%8) );
			continue;
		}

		joinIdx += 1;
	}
	// from int to char 8 bits
	for(int i=0;i<nullBytes;i++)
	{
		nullIndicator[i] = base10NullBytes[i];
	}

	// copy null Indicator
	memcpy( data, nullIndicator, nullBytes );

	// start copy data
	int offset = nullBytes;
	// be careful the left data should offset leftNullBytes
	// copy left
	memcpy( (char*)data+offset, (char*)leftData+leftNullBytes, leftValue.size );
	offset += leftValue.size;
	// copy right
	int rightRecordSize = getRecordSize(rightData, _rightAttributes);
	memcpy( (char*)data+offset, (char*)rightData+rightNullBytes, rightRecordSize );
}


int BNLJoin::getRecordSize(const void *data, const vector<Attribute> attrs){
	int totalColSize = attrs.size();
	int nullBytes = getActualBytesForNullsIndicator( totalColSize );
	unsigned char* nullIndicator = (unsigned char*) malloc(nullBytes);
	memcpy( nullIndicator, (char*)data, nullBytes );
	int recordSize = 0;

	int offset = nullBytes;

	Attribute tmpAttr;
	for(int attrPosition=0; attrPosition<attrs.size();attrPosition++)
	{
		tmpAttr = attrs[attrPosition];

		// check is null or not
		int shiftedBit = 8*nullBytes - attrPosition - 1;
		int nullIndex = nullBytes - (int)(shiftedBit/8) - 1;
		bool isNull = nullIndicator[nullIndex] & (1 << (shiftedBit%8) );

		if(isNull)
		{
			continue;
		}

		if( tmpAttr.type == TypeVarChar )
		{
			int charLen;
			memcpy( &charLen, (char*)data+offset, sizeof(int) );
			int charTotalSize = sizeof(int) + charLen;
			offset += charTotalSize;
		}
		else
		{
			offset += 4;
		}
	}

	recordSize = offset - nullBytes;

	free(nullIndicator);

	return recordSize;
}

//INLJoin
INLJoin::INLJoin(Iterator *leftIn, IndexScan *rightIn,
		const Condition &condition)
{
	leftIn->getAttributes(_leftAttributes);
	rightIn->getAttributes(_rightAttributes);

	_leftIn = leftIn;
	_rightIn = rightIn;
	_condition = condition;
	_leftPosition = getAttributePosition( _leftAttributes, condition.lhsAttr );

	_attributes = _leftAttributes;
	_attributes.insert( _attributes.end(), _rightAttributes.begin(), _rightAttributes.end() );
	_getLeftRecord = true;
	_leftNullBytes = getActualBytesForNullsIndicator(_leftAttributes.size());
	_columnData = malloc(PAGE_SIZE);

}

INLJoin::~INLJoin() {
	_leftIn = NULL;
	_rightIn = NULL;
	free(_columnData);
}

RC INLJoin::getNextTuple(void *data) {
	RC success = -1;
	while(true)
	{
		if( _getLeftRecord )
		{
			void *tmpData = malloc(PAGE_SIZE);
			if( _leftIn->getNextTuple(tmpData) == QE_EOF )
			{
				free(tmpData);
				return success;
			}
			// get scan val

			memset(_columnData, 0, PAGE_SIZE);
			int columnSize = getColumnData(tmpData, _columnData, _leftAttributes, _leftPosition);
			// column data should add column size back
			if(columnSize > 0 && _leftAttributes[_leftPosition].type == TypeVarChar)
			{
				memmove( (char*)_columnData+sizeof(int), _columnData, columnSize );
				memcpy( &columnSize, _columnData, sizeof(int) );
			}

			_leftRecord.size = getRecordSize(tmpData, _leftAttributes);
			_leftRecord.data = string( (char*)tmpData, (_leftRecord.size+_leftNullBytes) );

			// set iter
			switch(_condition.op)
			{
				case EQ_OP:
					// NULL case
					if(columnSize == -1)
					{
						_rightIn->setIterator(NULL, NULL, true, true);
					}
					else
					{
						_rightIn->setIterator(_columnData, _columnData, true, true);
					}
					break;
				case LT_OP:      // <
					break;
	           	case LE_OP:      // <=
	           		break;
	           	case GT_OP:      // >
	           		break;
	           	case GE_OP:      // >=
	           		break;
	           	case NE_OP:      // !=
	           		break;
			}

			free(tmpData);
		}

		_getLeftRecord = false;
		void *rightData = malloc(PAGE_SIZE);
		if( _rightIn->getNextTuple(rightData) != QE_EOF )
		{
			success = 0;
			createJoinRecord( data, _leftRecord, rightData );
			free(rightData);
			return success;
		}
		// update flag to continue
		_getLeftRecord = true;
		free(rightData);
	}
}

void INLJoin::createJoinRecord(void *data, JoinMapValue leftValue, const void *rightData){
	// prepare left data
	const char *leftData  = leftValue.data.c_str();

	int leftAttributeSize = _leftAttributes.size();
	int rightAttributeSize = _rightAttributes.size();
	int leftNullBytes = getActualBytesForNullsIndicator( leftAttributeSize );
	int rightNullBytes = getActualBytesForNullsIndicator( rightAttributeSize );

	int combinedSize = leftAttributeSize + rightAttributeSize;
	int nullBytes = getActualBytesForNullsIndicator(combinedSize);

	// init null bytes
	unsigned char *leftNullIndicator = (unsigned char*) malloc(leftNullBytes);
	unsigned char *rightNullIndicator = (unsigned char*) malloc(rightNullBytes);
	unsigned char *nullIndicator = (unsigned char*) malloc(nullBytes);
	int base10NullBytes[nullBytes];
	memset( base10NullBytes, 0, sizeof(int)*nullBytes );

	memcpy( leftNullIndicator, leftData, leftNullBytes );
	memcpy( rightNullIndicator, rightData, rightNullBytes );

	int joinIdx = 0;
	for(int i=0;i<_leftAttributes.size();i++)
	{
		// check null
		int shiftedBit = 8*leftNullBytes - i - 1;
		int nullIndex = leftNullBytes - (int)(shiftedBit/8) - 1;
		bool isNull = leftNullIndicator[nullIndex] & ( 1<<(shiftedBit%8) );

		if(isNull)
		{
			int mod = joinIdx/8;
			int joinShiftBit = 8*nullBytes- joinIdx - 1;
			base10NullBytes[mod] += pow( 2, (joinShiftBit%8) );
			continue;
		}

		joinIdx += 1;
	}

	for(int i=0;i<_rightAttributes.size();i++)
	{
		// check null
		int shiftedBit = 8*rightNullBytes - i - 1;
		int nullIndex = rightNullBytes - (int)(shiftedBit/8) - 1;
		bool isNull = rightNullIndicator[nullIndex] & ( 1<<(shiftedBit%8) );

		if(isNull)
		{
			int mod = joinIdx/8;
			int joinShiftBit = 8*nullBytes- joinIdx - 1;
			base10NullBytes[mod] += pow( 2, (joinShiftBit%8) );
			continue;
		}

		joinIdx += 1;
	}
	// from int to char 8 bits
	for(int i=0;i<nullBytes;i++)
	{
		nullIndicator[i] = base10NullBytes[i];
	}

	// copy null Indicator
	memcpy( data, nullIndicator, nullBytes );

	// start copy data
	int offset = nullBytes;
	// be careful the left data should offset leftNullBytes
	// copy left
	memcpy( (char*)data+offset, (char*)leftData+leftNullBytes, leftValue.size );
	offset += leftValue.size;
	// copy right
	int rightRecordSize = getRecordSize(rightData, _rightAttributes);
	memcpy( (char*)data+offset, (char*)rightData+rightNullBytes, rightRecordSize );
}


int INLJoin::getRecordSize(const void *data, const vector<Attribute> attrs){
	int totalColSize = attrs.size();
	int nullBytes = getActualBytesForNullsIndicator( totalColSize );
	unsigned char* nullIndicator = (unsigned char*) malloc(nullBytes);
	memcpy( nullIndicator, (char*)data, nullBytes );
	int recordSize = 0;

	int offset = nullBytes;

	Attribute tmpAttr;
	for(int attrPosition=0; attrPosition<attrs.size();attrPosition++)
	{
		tmpAttr = attrs[attrPosition];

		// check is null or not
		int shiftedBit = 8*nullBytes - attrPosition - 1;
		int nullIndex = nullBytes - (int)(shiftedBit/8) - 1;
		bool isNull = nullIndicator[nullIndex] & (1 << (shiftedBit%8) );

		if(isNull)
		{
			continue;
		}

		if( tmpAttr.type == TypeVarChar )
		{
			int charLen;
			memcpy( &charLen, (char*)data+offset, sizeof(int) );
			int charTotalSize = sizeof(int) + charLen;
			offset += charTotalSize;
		}
		else
		{
			offset += 4;
		}
	}

	recordSize = offset - nullBytes;

	free(nullIndicator);

	return recordSize;
}

void INLJoin::getAttributes(vector<Attribute> &attrs) const {
	attrs = _attributes;
}

// GH Join
GHJoin::GHJoin(Iterator *leftIn,               // Iterator of input R
            Iterator *rightIn,               // Iterator of input S
            const Condition &condition,      // Join condition (CompOp is always EQ)
            const unsigned numPartitions     // # of partitions for each relation (decided by the optimizer)
      ){
	leftIn->getAttributes(_leftAttributes);
	rightIn->getAttributes(_rightAttributes);

	_leftIn = leftIn;
	_rightIn = rightIn;

	_condition = condition;
	_leftPosition = getAttributePosition( _leftAttributes, condition.lhsAttr );
	_rightPosition = getAttributePosition( _rightAttributes, condition.rhsAttr );

	_attributes = _leftAttributes;
	_attributes.insert( _attributes.end(), _rightAttributes.begin(), _rightAttributes.end() );
	_leftNullBytes = getActualBytesForNullsIndicator(_leftAttributes.size());
	_rightNullBytes = getActualBytesForNullsIndicator( _rightAttributes.size() );

	// params for partition
	_shouldLoadPartition = true;
	_curPartitionIdx = -1;
	_shouldGetNextRight = true;
	_numPartitions = numPartitions;

	// do partition
	void *data = malloc(PAGE_SIZE);
	void *columnData = malloc(PAGE_SIZE);

	// open file create table
	string partitionName;
	RelationManager *rm = RelationManager::instance();

	// left partition
	for(int idx=0;idx<_numPartitions;idx++)
	{
		partitionName = getPartitionName(condition.lhsAttr, idx);
		_partitionMapNames.push_back(partitionName);
		rm->createTable(partitionName, _leftAttributes);

	}
	// right partition
	for(int idx=0;idx<_numPartitions;idx++)
	{
		partitionName = getPartitionName(condition.rhsAttr, idx);
		_rightPartitionMapNames.push_back(partitionName);
		rm->createTable(partitionName, _rightAttributes);
	}

	// insert to left partition
	while( _leftIn->getNextTuple(data) != QE_EOF)
	{
		memset(columnData, 0, PAGE_SIZE);
		int columnSize = getColumnData(data, columnData, _leftAttributes, _leftPosition);
		// column data should add column size back
		string key;
		if(columnSize < 0)
		{
			key = "null";
		}
		if(columnSize > 0 && _leftAttributes[_leftPosition].type == TypeVarChar)
		{
			memcpy( &columnSize, columnData, sizeof(int) );
			key = string( (char*)columnData+4, columnSize );
		}
		else
		{
			key = string( columnSize, 4 );
		}
		// partition hashing
		int hashInt = 0;
		if(_leftAttributes[_leftPosition].type == TypeVarChar)
		{
			for(int i=0;i<key.size();i++)
			{
				hashInt = hashInt << 1 ^ key[i];
			}
		}
		else
		{
			if(_leftAttributes[_leftPosition].type == TypeReal)
			{
				float f;
				memcpy(&f, columnData, 4);
				hashInt = f;
			}
			else
			{
				memcpy(&hashInt, columnData, 4);
			}
		}

		int partitionId = hashInt % _numPartitions;
		string partitionTable = getPartitionName(condition.lhsAttr, partitionId);
		RID rid;
		if( rm->insertTuple(partitionTable, data, rid) != 0)
		{
			cout<<"Something Wrong"<<endl;
		}

		memset(data, 0, PAGE_SIZE);
	}

	// insert to right partition
	while( _rightIn->getNextTuple(data) != QE_EOF)
	{
		memset(columnData, 0, PAGE_SIZE);
		int columnSize = getColumnData(data, columnData, _rightAttributes, _rightPosition);
		// column data should add column size back
		string key;
		if(columnSize < 0)
		{
			key = "null";
		}
		if(columnSize > 0 && _rightAttributes[_rightPosition].type == TypeVarChar)
		{
			memcpy( &columnSize, columnData, sizeof(int) );
			key = string( (char*)columnData+4, columnSize );
		}
		else
		{
			key = string( columnSize, 4 );
		}

		int hashInt = 0;
		if(_rightAttributes[_rightPosition].type == TypeVarChar)
		{
			for(int i=0;i<key.size();i++)
			{
				hashInt = hashInt << 1 ^ key[i];
			}
		}
		else
		{
			if(_rightAttributes[_rightPosition].type == TypeReal)
			{
				float f;
				memcpy(&f, columnData, 4);
				hashInt = f;
			}
			else
			{
				memcpy(&hashInt, columnData, 4);
			}
		}

		int partitionId = hashInt % _numPartitions;
		string partitionTable = getPartitionName(condition.rhsAttr, partitionId);
		RID rid;
		if( rm->insertTuple(partitionTable, data, rid) != 0)
		{
			cout<<"Something Wrong"<<endl;
		}

		memset(data, 0, PAGE_SIZE);
	}

	free(data);
	free(columnData);
}

GHJoin::~GHJoin(){
	_leftIn = NULL;
	_rightIn = NULL;
	string partitionName;
	RelationManager *rm = RelationManager::instance();

	// delete partition tables
	for(int i=0;i<_partitionMapNames.size();i++)
	{
		partitionName = _partitionMapNames[i];
		rm->deleteTable(partitionName);
	}

	for(int i=0;i<_rightPartitionMapNames.size();i++)
	{
		partitionName = _rightPartitionMapNames[i];
		rm->deleteTable(partitionName);
	}
}


RC GHJoin::getNextTuple(void *data){
	RC success = -1;

	RelationManager *rm = RelationManager::instance();
	void *rightData = malloc(PAGE_SIZE);
	void *rightColumnData = malloc(PAGE_SIZE);
	int attrPosition = getAttributePosition(_rightAttributes, _condition.rhsAttr);
	Attribute attr = _rightAttributes[attrPosition];

	while(true)
	{
		if(_shouldLoadPartition)
		{
			_curPartitionIdx += 1;
			// no partition left
			if(_curPartitionIdx >= _numPartitions)
			{
				break;
			}
			// load all partition into hash
			loadLeftMap();

			// reset rmScanIterator
			string rightPartitionTable = getPartitionName(_condition.rhsAttr, _curPartitionIdx);

			vector<string> rightAttrNames;
			for(int i=0;i<_rightAttributes.size();i++)
			{
				rightAttrNames.push_back(_rightAttributes[i].name);
			}

			RM_ScanIterator rmScanIterator;
			_rightRmIterator = rmScanIterator;
			rm->scan(rightPartitionTable, "", NO_OP, NULL, rightAttrNames, _rightRmIterator);
			_shouldLoadPartition = false;
			_shouldGetNextRight = true;
		}

		if(_shouldGetNextRight)
		{
			RID rid;
			while( _rightRmIterator.getNextTuple(rid, rightData) != RM_EOF )
			{
				int columnSize = getColumnData(rightData, rightColumnData, _rightAttributes, attrPosition);
				if( columnSize <= -1 )
				{
					string nullKey = NULL_KEY;
					auto it = _leftMap.find(nullKey);
					if( it != _leftMap.end() )
					{
						int leftValueSize = _leftMap[nullKey].size();
						if(leftValueSize > 0)
						{
							_curLeftValues = _leftMap[nullKey];
							JoinMapValue leftR = _curLeftValues.back();
							_curLeftValues.pop_back();
							createJoinRecord( data, leftR, rightData );

							success = 0;
							free(rightData);
							free(rightColumnData);

							if(leftValueSize == 1)
							{
								_shouldGetNextRight = true;
							}

							return success;
						}
					}
				}
				// non NULL case
				else
				{
					// prepare rightKey
					string rightKey;
					if(attr.type == TypeVarChar)
					{
						int charLen;
						memcpy( &charLen, rightColumnData, sizeof(int) );
						rightKey = string( ( (char*)rightColumnData+4 ), charLen );
					}
					else
					{
						if(attr.type == TypeInt)
						{
							int rightVal;
							memcpy( &rightVal, rightColumnData, sizeof(int) );
							rightKey = to_string(rightVal);
						}
						else
						{
							float rightVal;
							memcpy( &rightVal, rightColumnData, sizeof(float) );
							rightKey = to_string(rightVal);
						}
					}
					// start get hashmap vector
					auto it = _leftMap.find(rightKey);
					if( it != _leftMap.end() )
					{
						int leftValueSize = _leftMap[rightKey].size();
						if(leftValueSize > 0)
						{
							_curLeftValues = _leftMap[rightKey];
							JoinMapValue leftR = _curLeftValues.back();
							_curLeftValues.pop_back();
							createJoinRecord( data, leftR, rightData );

							success = 0;
							free(rightData);
							free(rightColumnData);

							if(leftValueSize == 1)
							{
								_shouldGetNextRight = true;
							}

							return success;
						}
					}
				}
			}
		}
		else
		{
			JoinMapValue leftR = _curLeftValues.back();
			_curLeftValues.pop_back();
			createJoinRecord( data, leftR, rightData );
			if(_curLeftValues.size() == 0)
			{
				_shouldGetNextRight = true;
			}

			success = 0;
			free(rightData);
			free(rightColumnData);

			return success;
		}

		// update flag
		_shouldLoadPartition = true;
		_rightRmIterator.close();
	}

	free(rightData);
	free(rightColumnData);
	return success;
}

// load all records from partition
RC GHJoin::loadLeftMap(){
	RC success = 0;

	// init _leftMap
	_leftMap.clear();

	void *data = malloc(PAGE_SIZE);
	int recordSize;
	JoinMapValue mapValue;

	int attrPosition = getAttributePosition(_leftAttributes, _condition.lhsAttr);
	Attribute attr = _leftAttributes[attrPosition];
	int nullBytes = getActualBytesForNullsIndicator(_leftAttributes.size());

	void *columnData = malloc(PAGE_SIZE);
	// get iteration partition file
	string partitionTable = getPartitionName(_condition.lhsAttr, _curPartitionIdx);
	RelationManager *rm = RelationManager::instance();
	vector<string> leftAttrNames;
	for(int i=0;i<_leftAttributes.size();i++)
	{
		leftAttrNames.push_back(_leftAttributes[i].name);
	}
	RM_ScanIterator rmIterator;

	rm->scan( partitionTable, "", NO_OP, NULL, leftAttrNames, rmIterator);
	RID rid;

	while( rmIterator.getNextTuple(rid,data) != RM_EOF )
	{
		recordSize = getRecordSize(data, _leftAttributes);
		mapValue.size = recordSize;
		mapValue.data = string( (char*)data, recordSize+nullBytes );

		int joinColumnSize = getColumnData(data, columnData, _leftAttributes, attrPosition);

		// null case using special key NULL_KEY
		if(joinColumnSize == -1)
		{
			string nullKey = NULL_KEY;
			auto it = _leftMap.find(nullKey);
			if( it != _leftMap.end() )
			{
				_leftMap[nullKey].push_back(mapValue);
			}
			else
			{
				vector<JoinMapValue> jmap;
				jmap.push_back(mapValue);
				_leftMap[nullKey] = jmap;
			}

			continue;
		}

		// Non-Null key
		string mapKey;
		if(attr.type == TypeVarChar)
		{
			int charLen;
			memcpy( &charLen, (char*)columnData, sizeof(int) );
			mapKey = string( ((char*)columnData+sizeof(int)), charLen );
		}
		else
		{
			if(attr.type == TypeInt)
			{
				int joinKeyVal;
				memcpy( &joinKeyVal, columnData, sizeof(int) );
				mapKey = to_string(joinKeyVal);
			}
			else
			{
				float joinKeyVal;
				memcpy( &joinKeyVal, columnData, sizeof(float) );
				mapKey = to_string(joinKeyVal);
			}
		}

		auto it = _leftMap.find(mapKey);
		if(it != _leftMap.end())
		{
			_leftMap[mapKey].push_back(mapValue);
		}
		else
		{
			vector<JoinMapValue> jmap;
			jmap.push_back(mapValue);
			_leftMap[mapKey] = jmap;
		}
	}

	// free & close
	rmIterator.close();
	free(data);
	free(columnData);
	return success;
}


string GHJoin::getPartitionName(string attrName, int idx){
	string sIdx = to_string(idx);
	return (_partitionPrefix + attrName + sIdx);
}

void GHJoin::getAttributes(vector<Attribute> &attrs) const{
	attrs = _attributes;
}

void GHJoin::createJoinRecord(void *data, JoinMapValue leftValue, const void *rightData){
	// prepare left data
	const char *leftData  = leftValue.data.c_str();

	int leftAttributeSize = _leftAttributes.size();
	int rightAttributeSize = _rightAttributes.size();
	int leftNullBytes = getActualBytesForNullsIndicator( leftAttributeSize );
	int rightNullBytes = getActualBytesForNullsIndicator( rightAttributeSize );

	int combinedSize = leftAttributeSize + rightAttributeSize;
	int nullBytes = getActualBytesForNullsIndicator(combinedSize);

	// init null bytes
	unsigned char *leftNullIndicator = (unsigned char*) malloc(leftNullBytes);
	unsigned char *rightNullIndicator = (unsigned char*) malloc(rightNullBytes);
	unsigned char *nullIndicator = (unsigned char*) malloc(nullBytes);
	int base10NullBytes[nullBytes];
	memset( base10NullBytes, 0, sizeof(int)*nullBytes );

	memcpy( leftNullIndicator, leftData, leftNullBytes );
	memcpy( rightNullIndicator, rightData, rightNullBytes );

	int joinIdx = 0;
	for(int i=0;i<_leftAttributes.size();i++)
	{
		// check null
		int shiftedBit = 8*leftNullBytes - i - 1;
		int nullIndex = leftNullBytes - (int)(shiftedBit/8) - 1;
		bool isNull = leftNullIndicator[nullIndex] & ( 1<<(shiftedBit%8) );

		if(isNull)
		{
			int mod = joinIdx/8;
			int joinShiftBit = 8*nullBytes- joinIdx - 1;
			base10NullBytes[mod] += pow( 2, (joinShiftBit%8) );
			continue;
		}

		joinIdx += 1;
	}

	for(int i=0;i<_rightAttributes.size();i++)
	{
		// check null
		int shiftedBit = 8*rightNullBytes - i - 1;
		int nullIndex = rightNullBytes - (int)(shiftedBit/8) - 1;
		bool isNull = rightNullIndicator[nullIndex] & ( 1<<(shiftedBit%8) );

		if(isNull)
		{
			int mod = joinIdx/8;
			int joinShiftBit = 8*nullBytes- joinIdx - 1;
			base10NullBytes[mod] += pow( 2, (joinShiftBit%8) );
			continue;
		}

		joinIdx += 1;
	}
	// from int to char 8 bits
	for(int i=0;i<nullBytes;i++)
	{
		nullIndicator[i] = base10NullBytes[i];
	}

	// copy null Indicator
	memcpy( data, nullIndicator, nullBytes );

	// start copy data
	int offset = nullBytes;
	// be careful the left data should offset leftNullBytes
	// copy left
	memcpy( (char*)data+offset, (char*)leftData+leftNullBytes, leftValue.size );
	offset += leftValue.size;
	// copy right
	int rightRecordSize = getRecordSize(rightData, _rightAttributes);
	memcpy( (char*)data+offset, (char*)rightData+rightNullBytes, rightRecordSize );
}

int GHJoin::getRecordSize(const void *data, const vector<Attribute> attrs){
	int totalColSize = attrs.size();
	int nullBytes = getActualBytesForNullsIndicator( totalColSize );
	unsigned char* nullIndicator = (unsigned char*) malloc(nullBytes);
	memcpy( nullIndicator, (char*)data, nullBytes );
	int recordSize = 0;

	int offset = nullBytes;

	Attribute tmpAttr;
	for(int attrPosition=0; attrPosition<attrs.size();attrPosition++)
	{
		tmpAttr = attrs[attrPosition];

		// check is null or not
		int shiftedBit = 8*nullBytes - attrPosition - 1;
		int nullIndex = nullBytes - (int)(shiftedBit/8) - 1;
		bool isNull = nullIndicator[nullIndex] & (1 << (shiftedBit%8) );

		if(isNull)
		{
			continue;
		}

		if( tmpAttr.type == TypeVarChar )
		{
			int charLen;
			memcpy( &charLen, (char*)data+offset, sizeof(int) );
			int charTotalSize = sizeof(int) + charLen;
			offset += charTotalSize;
		}
		else
		{
			offset += 4;
		}
	}

	recordSize = offset - nullBytes;

	free(nullIndicator);

	return recordSize;
}
// Aggregate
Aggregate::Aggregate(Iterator *input,          // Iterator of input R
                  Attribute aggAttr,        // The attribute over which we are computing an aggregate
                  AggregateOp op            // Aggregate operation
        ){

	_inputIterator = input;
	_aggAttr = aggAttr;
	_op = op;
	aggAttr.position = 1;
	// generate attributes
	_inputIterator->getAttributes(_inputAttributes);

	Attribute aggData;
	aggData.position = 2;
	aggData.length = 4;
	switch(_op){
		case MIN:
			if(aggAttr.type == TypeInt)
			{
				aggData.type = TypeInt;
			}
			else
			{
				aggData.type = TypeReal;
			}
			aggData.name = "MIN("+aggAttr.name+")";
			break;
		case MAX:
			if(aggAttr.type == TypeInt)
			{
				aggData.type = TypeInt;
			}
			else
			{
				aggData.type = TypeReal;
			}
			aggData.name = "MAX("+aggAttr.name+")";
			break;
		case COUNT:
			aggData.type = TypeInt;
			aggData.name = "COUNT("+aggAttr.name+")";
			break;
		case SUM:
			if(aggAttr.type == TypeInt)
			{
				aggData.type = TypeInt;
			}
			else
			{
				aggData.type = TypeReal;
			}
			aggData.name = "SUM("+aggAttr.name+")";
			break;
		case AVG:
			aggData.type = TypeReal;
			aggData.name = "AVG("+aggAttr.name+")";
			break;
	}

	_attributes.push_back(aggData);
}

Aggregate::~Aggregate(){
	_inputIterator = NULL;
}

RC Aggregate::getNextTuple(void *data){
	RC success = -1;

	success = calculateAggregate(data);
	return success;
}

RC Aggregate::calculateAggregate(void *data){
	RC success = -1;

	void *tmpData = malloc(PAGE_SIZE);
	void *columnData = malloc(PAGE_SIZE);
	int attrPosition = getAttributePosition(_inputAttributes, _aggAttr.name);

	float aggrValue = 0;
	float aggrCount = 0;
	while( _inputIterator->getNextTuple(tmpData) != QE_EOF )
	{
		int columnSize = getColumnData(tmpData, columnData, _inputAttributes, attrPosition);
		// start aggregate
		switch(_op){
			case MIN:
				if(_aggAttr.type == TypeInt)
				{
					if(success == -1)
					{
						aggrValue = *((int*)columnData);
					}
					else
					{
						int curValue = *((int*)columnData);
						if( curValue < aggrValue )
						{
							aggrValue = curValue;
						}
					}
				}
				else
				{
					if(success == -1)
					{
						aggrValue = *((float*)columnData);
					}
					else
					{
						float curValue = *((float*)columnData);
						if( curValue < aggrValue )
						{
							aggrValue = curValue;
						}
					}
				}
				break;
			case MAX:
				if(_aggAttr.type == TypeInt)
				{
					if(success == -1)
					{
						aggrValue = *((int*)columnData);
					}
					else
					{
						int curValue = *((int*)columnData);
						if( curValue > aggrValue )
						{
							aggrValue = curValue;
						}
					}
				}
				else
				{
					if(success == -1)
					{
						aggrValue = *((float*)columnData);
					}
					else
					{
						float curValue = *((float*)columnData);
						if( curValue > aggrValue )
						{
							aggrValue = curValue;
						}
					}
				}
				break;
			case COUNT:
				aggrCount += 1;
				break;
			case SUM:
				if(_aggAttr.type == TypeInt)
				{
					aggrValue += *((int*)columnData);
				}
				else
				{
					aggrValue += *((float*)columnData);
				}
				break;
			case AVG:
				if(_aggAttr.type == TypeInt)
				{
					aggrValue += *((int*)columnData);
				}
				else
				{
					aggrValue += *((float*)columnData);
				}
				aggrCount += 1;
				break;
		}

		success = 0;

	}

	// prepare data
	if(success == 0)
	{
		memset(data, 0, 1);
		if(_op == AVG)
		{
			float aggrAvg = ((float)aggrValue)/aggrCount;
				memcpy( (char*)data+1, &aggrAvg, 4);
		}
		else if(_op == COUNT)
		{
			memcpy( (char*)data+1, &aggrCount, 4);
		}
		else
		{
			memcpy( (char*)data+1, &aggrValue, 4);
		}
	}

	free(tmpData);
	free(columnData);
	return success;
}

void Aggregate::getAttributes(vector<Attribute> &attrs) const{
	attrs = _attributes;
}
// accessory function
int getAttributePosition(const vector<Attribute> attrs, const string attrName)
{
	int idx = -1;
	for(int i=0; i<attrs.size();i++)
	{
		if(attrs[i].name == attrName)
		{
			idx = i;
			break;
		}
	}

	return idx;
}

int getColumnData( const void *data, void *columnData, const vector<Attribute> attrs, int attrPosition )
{
	int totalColSize = attrs.size();
	int nullBytes = getActualBytesForNullsIndicator( totalColSize );
	unsigned char* nullIndicator = (unsigned char*) malloc(nullBytes);
	memcpy( nullIndicator, (char*)data, nullBytes );
	// check is null or not
	int shiftedBit = 8*nullBytes - attrPosition - 1;
	int nullIndex = nullBytes - (int)(shiftedBit/8) - 1;
	bool isNull = nullIndicator[nullIndex] & (1 << (shiftedBit%8) );

	int columnSize = -1;
	if(isNull)
	{
		return columnSize;
	}

	unsigned offset = nullBytes;
	bool isDestCol = false;
	for(int i=0; i<totalColSize;i++)
	{
		Attribute tmpAttr = attrs[i];
		// check is null
		shiftedBit = 8*nullBytes - i - 1;
		nullIndex = nullBytes - (int)(shiftedBit/8) - 1;

		isNull = nullIndicator[nullIndex] & (1 << (shiftedBit%8) );

		if( !isNull )
		{
			if( i==attrPosition )
			{
					isDestCol = true;
			}
			if( tmpAttr.type == TypeVarChar )
			{
				int charLen;
				memcpy( &charLen, (char*)data+offset, sizeof(int) );
				int charTotalSize = sizeof(int) + charLen;

				if(isDestCol)
				{
					memcpy( columnData, (char*)data+offset, charTotalSize );
					columnSize = charTotalSize;
					break;
				}
				offset += charTotalSize;
			}
			else
			{
				if(isDestCol)
				{
					memcpy( columnData, (char*)data+offset, 4 );
					columnSize = 4;
					break;
				}
				offset += 4;
			}
		}
	}

	free(nullIndicator);
	return columnSize;
}
