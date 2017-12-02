
#include "qe.h"

// start filter
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
	_inputIterator->getAttributes(attrs);
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
			base10NullBytes[mod] += pow(2, (attrPosition%8));
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
