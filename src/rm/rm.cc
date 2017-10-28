
#include "rm.h"

RelationManager* RelationManager::instance()
{
    static RelationManager _rm;
    return &_rm;
}

RelationManager::RelationManager()
{
	_rbf_manager = RecordBasedFileManager::instance();
	// create table of table
	// create table of attribute
}

RelationManager::~RelationManager()
{
}

RC RelationManager::createCatalog()
{
    return -1;
}

RC RelationManager::deleteCatalog()
{
    return -1;
}

RC RelationManager::createTable(const string &tableName, const vector<Attribute> &attrs)
{
    return -1;
}

RC RelationManager::deleteTable(const string &tableName)
{
    return -1;
}

RC RelationManager::getAttributes(const string &tableName, vector<Attribute> &attrs)
{
    return -1;
}

RC RelationManager::insertTuple(const string &tableName, const void *data, RID &rid)
{
	vector<Attribute> recordDescriptor;
	if( getTableAttributes( tableName, recordDescriptor ) != 0 )
		return -1;

	FileHandle fileHandle;

	if( _rbf_manager->insertRecord( fileHandle, recordDescriptor, data, rid ) == 0 )
	{
		_rbf_manager->closeFile(fileHandle);
		return 0;
	}

	return -1;
}

RC RelationManager::deleteTuple(const string &tableName, const RID &rid)
{
	vector<Attribute> recordDescriptor;
	if( getTableAttributes( tableName, recordDescriptor ) != 0 )
		return -1;

	FileHandle fileHandle;

	if( _rbf_manager->deleteRecord( fileHandle, recordDescriptor, rid ) == 0 )
	{
		_rbf_manager->closeFile(fileHandle);
		return 0;
	}

	return -1;
}

RC RelationManager::updateTuple(const string &tableName, const void *data, const RID &rid)
{
	vector<Attribute> recordDescriptor;
	if( getTableAttributes( tableName, recordDescriptor ) != 0 )
		return -1;

	FileHandle fileHandle;

	if( _rbf_manager->updateRecord( fileHandle, recordDescriptor, data, rid ) == 0 )
	{
		_rbf_manager->closeFile(fileHandle);
		return 0;
	}

	return -1;
}

RC RelationManager::readTuple(const string &tableName, const RID &rid, void *data)
{
	vector<Attribute> recordDescriptor;
	if( getTableAttributes( tableName, recordDescriptor ) != 0 )
		return -1;

	FileHandle fileHandle;
	if ( _rbf_manager->openFile( tableName, fileHandle ) != 0 )
		return -1;

	if ( _rbf_manager->readRecord( fileHandle, recordDescriptor, rid, data ) != 0 )
		return -1;

	_rbf_manager->closeFile(fileHandle);
	return 0;
}

RC RelationManager::printTuple(const vector<Attribute> &attrs, const void *data)
{
	return _rbf_manager->printRecord( attrs, data );
}

RC RelationManager::readAttribute(const string &tableName, const RID &rid, const string &attributeName, void *data)
{
	// prepare recordDescriptor
	vector<Attribute> recordDescriptor;
	if( getTableAttributes( tableName, recordDescriptor ) != 0)
		return -1;

	FileHandle fileHandle;

	if ( _rbf_manager->openFile( tableName, fileHandle ) != 0 )
			return -1;

	if( _rbf_manager->readAttribute( fileHandle, recordDescriptor, rid, attributeName, data ) == 0 )
	{
		_rbf_manager->closeFile(fileHandle);
		return 0;
	}
	return -1;
}

RC RelationManager::scan(const string &tableName,
      const string &conditionAttribute,
      const CompOp compOp,                  
      const void *value,                    
      const vector<string> &attributeNames,
      RM_ScanIterator &rm_ScanIterator)
{
	RecordBasedFileManager*_rbf_manager = RecordBasedFileManager::instance();
	FileHandle fileHandle;
	if ( _rbf_manager->openFile( tableName, fileHandle ) != 0 )
		return -1;
	// prepare recordDescriptor
	vector<Attribute> recordDescriptor;
	if( getAttributes( tableName, recordDescriptor ) != 0)
		return -1;

	// set up rm_ScanIterator
	rm_ScanIterator._fileHandle = fileHandle;
	rm_ScanIterator._rbf_scanIter._fileHandlePtr = &rm_ScanIterator._fileHandle;

	// run record scan
	return _rbf_manager->scan( rm_ScanIterator._fileHandle, recordDescriptor, conditionAttribute, compOp, value, attributeNames, rm_ScanIterator._rbf_scanIter );
}

RC RelationManager::getTableAttributes(const string &tableName, vector<Attribute> &attrs){
    Attribute attr;

    if( tableName == "Tables" )
    {
        attr.name="table-id";
        attr.type=TypeInt;
        attr.length=sizeof(int);
        attr.position=1;
        attrs.push_back(attr);

        attr.name="table-name";
        attr.type=TypeVarChar;
        attr.length=VarChar;
        attr.position=2;
        attrs.push_back(attr);

        attr.name="file-name";
        attr.type=TypeVarChar;
        attr.length=VarChar;
        attr.position=3;
        attrs.push_back(attr);

        attr.name="SystemTable";
        attr.type=TypeInt;
        attr.length=sizeof(int);
        attr.position=4;
        attrs.push_back(attr);

        return 0;
    }
    else if( tableName == "Columns" )
    {
        attr.name="table-id";
        attr.type=TypeInt;
        attr.length=sizeof(int);
        attr.position=1;
        attrs.push_back(attr);

        attr.name="column-name";
        attr.type=TypeVarChar;
        attr.length=VarChar;
        attr.position=2;
        attrs.push_back(attr);

        attr.name="column-type";
        attr.type=TypeInt;
        attr.length=sizeof(int);
        attr.position=3;
        attrs.push_back(attr);

        attr.name="column-length";
        attr.type=TypeInt;
        attr.length=sizeof(int);
        attr.position=4;
        attrs.push_back(attr);

        attr.name="column-position";
        attr.type=TypeInt;
        attr.length=sizeof(int);
        attr.position=5;
        attrs.push_back(attr);


        attr.name="NullFlag";
        attr.type=TypeInt;
        attr.length=sizeof(int);
        attr.position=6;
        attrs.push_back(attr);

        return 0;
    }

    return getAttributes(tableName, attrs);
}

// RM ScanIterator
RC RM_ScanIterator::getNextTuple(RID &rid, void *data){
	return _rbf_scanIter.getNextRecord(rid, data);
}

RC RM_ScanIterator::close(){
	return _rbf_scanIter.close();
}

// Extra credit work
RC RelationManager::dropAttribute(const string &tableName, const string &attributeName)
{
    return -1;
}

// Extra credit work
RC RelationManager::addAttribute(const string &tableName, const Attribute &attr)
{
    return -1;
}
