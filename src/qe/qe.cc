
#include "qe.h"

// input
Filter::Filter(Iterator* input, const Condition &condition) {
	_inputIterator = input;
	_condition = condition;

	getAttributes(_attributes);

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

		bool notNull = getColumnData(data, columnData, _attributes, _attrPosition);

		// NO_OP and NULL can return
		if( _condition.op == NO_OP )
		{
			success = 0;
			break;
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
	_inputIterator->getAttributes(attrs);
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

bool getColumnData( const void *data, void *columnData, const vector<Attribute> attrs, int attrPosition )
{
	int totalColSize = attrs.size();
	int nullBytes = getActualBytesForNullsIndicator( totalColSize );
	unsigned char* nullIndicator = (unsigned char*) malloc(nullBytes);
	memcpy( nullIndicator, (char*)data, nullBytes );
	// check is null or not
	int shiftedBit = 8*nullBytes - attrPosition - 1;
	int nullIndex = nullBytes - (int)(shiftedBit/8) - 1;
	bool isNull = nullIndicator[nullIndex] & (1 << (shiftedBit%8) );
	if(isNull)
	{
		return false;
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
					break;
				}
				offset += charTotalSize;
			}
			else
			{
				if(isDestCol)
				{
					memcpy( columnData, (char*)data+offset, 4 );
					break;
				}
				offset += 4;
			}
		}
	}

	free(nullIndicator);
	return true;
}
