
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

}

INLJoin::~INLJoin() {
	_leftIn = NULL;
	_rightIn = NULL;
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
			void *columnData = malloc(PAGE_SIZE);
			memset(columnData, 0, PAGE_SIZE);
			int columnSize = getColumnData(tmpData, columnData, _leftAttributes, _leftPosition);
			// column data should add column size back
			if(columnSize > 0 && _leftAttributes[_leftPosition].type == TypeVarChar)
			{
				memmove( (char*)columnData+sizeof(int), columnData, columnSize );
				memcpy( &columnSize, columnData, sizeof(int) );
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
						_rightIn->setIterator(columnData, columnData, true, true);
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
			free(columnData);
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


/////
void readField(const void *input, void *data, vector<Attribute> attrs,
		int attrPos, AttrType type) {

	int totalColSize = attrs.size();
	int nullBytes = getActualBytesForNullsIndicator( totalColSize );
	unsigned char* nullIndicator = (unsigned char*) malloc(nullBytes);
	memcpy( nullIndicator, (char*)data, nullBytes);
	// check is null or not
	int shiftedBit = 8*nullBytes - attrPos - 1;
	int nullIndex = nullBytes - (int)(shiftedBit/8) - 1;
	bool isNull = nullIndicator[nullIndex] & (1 << (shiftedBit%8) );

	unsigned offset = nullBytes;
	bool isDestCol = false;

	memcpy((char*)input, (char*)nullIndicator, nullBytes);

	int attrLength=0;

	for(int i=0; i<totalColSize;i++)
	{
		Attribute tmpAttr = attrs[i];
		// check is null
		shiftedBit = 8*nullBytes - i - 1;
		nullIndex = nullBytes - (int)(shiftedBit/8) - 1;

		isNull = nullIndicator[nullIndex] & (1 << (shiftedBit%8) );

		if( !isNull )
		{
			if( i==attrPos )
			{
					isDestCol = true;
			}
			if( tmpAttr.type == TypeVarChar )
			{
				int charLen;
				memcpy( &charLen, (char*)data+offset, sizeof(int) );
				int charTotalSize = sizeof(int) + charLen;
				attrLength+=charTotalSize;

				if(isDestCol)
				{
					memcpy( (char*)input+offset, (char*)data+offset, charTotalSize );
					//columnSize = charTotalSize;
					break;
				}
				offset += charTotalSize;
			}
			else
			{
				if(isDestCol)
				{
					memcpy( (char*)input+offset, (char*)data+offset, sizeof(int));
					attrLength+=sizeof(int);
					break;
				}
				offset += sizeof(int);
			}
		}
	}

	//memcpy((char*)input+nullBytes, (char*)data+nullBytes, attrLength);

	free(nullIndicator);

}

int getTupleLength(const void *tuple, vector<Attribute> attrs, int attrPos) {

	int result = 0;

	int totalColSize = attrs.size();
	int nullBytes = getActualBytesForNullsIndicator( totalColSize );
	unsigned char* nullIndicator = (unsigned char*) malloc(nullBytes);
	memcpy( nullIndicator, (char*)tuple, nullBytes );
	// check is null or not
	int shiftedBit = 8*nullBytes - attrPos - 1;
	int nullIndex = nullBytes - (int)(shiftedBit/8) - 1;
	bool isNull = nullIndicator[nullIndex] & (1 << (shiftedBit%8) );

	unsigned offset = nullBytes;
	//bool isDestCol = false;

	for (unsigned i = 0; i < attrs.size(); i++) {
		if (attrs[i].type == TypeInt)
			result += sizeof(int);
		else if (attrs[i].type == TypeReal)
			result += sizeof(float);
		else {
			int stringLength = *(int *) ((char *) tuple + result);
			result += sizeof(int) + stringLength;
		}
	}

	result+=nullBytes;

	return result;

}

bool compareField(const void *attribute, const void *condition, AttrType type,
		CompOp compOp) {
	if (condition == NULL)
		return true;

	bool result = true;

	switch (type) {
	case TypeInt: {
		int attr = *(int *) attribute;
		int cond = *(int *) condition;

		switch (compOp) {
		case EQ_OP:
			result = attr == cond;
			break;
		case LT_OP:
			result = attr < cond;
			break;
		case GT_OP:
			result = attr > cond;
			break;
		case LE_OP:
			result = attr <= cond;
			break;
		case GE_OP:
			result = attr >= cond;
			break;
		case NE_OP:
			result = attr != cond;
			break;
		case NO_OP:
			break;
		}

		break;
	}

	case TypeReal: {
		float attr = *(float *) attribute;
		float cond = *(float *) condition;

		int temp = 0;

		if (attr - cond > 0.00001) //use approximate comparison for real value
			temp = 1;
		else if (attr - cond < -0.00001)
			temp = -1;

		switch (compOp) {
		case EQ_OP:
			result = temp == 0;
			break;
		case LT_OP:
			result = temp < 0;
			break;
		case GT_OP:
			result = temp > 0;
			break;
		case LE_OP:
			result = temp <= 0;
			break;
		case GE_OP:
			result = temp >= 0;
			break;
		case NE_OP:
			result = temp != 0;
			break;
		case NO_OP:
			break;
		}

		break;
	}

	case TypeVarChar: {
		int attriLeng = *(int *) attribute;
		string attr((char *) attribute + sizeof(int), attriLeng);
		int condiLeng = *(int *) condition;
		string cond((char *) condition + sizeof(int), condiLeng);

		switch (compOp) {
		case EQ_OP:
			result = strcmp(attr.c_str(), cond.c_str()) == 0;
			break;
		case LT_OP:
			result = strcmp(attr.c_str(), cond.c_str()) < 0;
			break;
		case GT_OP:
			result = strcmp(attr.c_str(), cond.c_str()) > 0;
			break;
		case LE_OP:
			result = strcmp(attr.c_str(), cond.c_str()) <= 0;
			break;
		case GE_OP:
			result = strcmp(attr.c_str(), cond.c_str()) >= 0;
			break;
		case NE_OP:
			result = strcmp(attr.c_str(), cond.c_str()) != 0;
			break;
		case NO_OP:
			break;
		}

		break;
	}
	}
	return result;
}
