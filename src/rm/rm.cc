#include "rm.h"
    
#include <stdio.h>
#include <assert.h>

#define VarChar 50


RC RelationManager::PrepareCatalogDescriptor(string tablename,vector<Attribute> &attributes){
	string tables="Tables";
	string columns="Columns";
	Attribute attr;

	if(tables.compare(tablename)==0){
		attr.name="table-id";
		attr.type=TypeInt;
		attr.length=sizeof(int);
		attr.position=1;
		attributes.push_back(attr);

		attr.name="table-name";
		attr.type=TypeVarChar;
		attr.length=VarChar;
		attr.position=2;
		attributes.push_back(attr);

		attr.name="file-name";
		attr.type=TypeVarChar;
		attr.length=VarChar;
		attr.position=3;
		attributes.push_back(attr);

		attr.name="SystemTable";
		attr.type=TypeInt;
		attr.length=sizeof(int);
		attr.position=4;
		attributes.push_back(attr);


		return 0;
	}
	else if(columns.compare(tablename)==0){
		attr.name="table-id";
		attr.type=TypeInt;
		attr.length=sizeof(int);
		attr.position=1;
		attributes.push_back(attr);

		attr.name="column-name";
		attr.type=TypeVarChar;
		attr.length=VarChar;
		attr.position=2;
		attributes.push_back(attr);

		attr.name="column-type";
		attr.type=TypeInt;
		attr.length=sizeof(int);
		attr.position=3;
		attributes.push_back(attr);

		attr.name="column-length";
		attr.type=TypeInt;
		attr.length=sizeof(int);
		attr.position=4;
		attributes.push_back(attr);

		attr.name="column-position";
		attr.type=TypeInt;
		attr.length=sizeof(int);
		attr.position=5;
		attributes.push_back(attr);


		attr.name="NullFlag";
		attr.type=TypeInt;
		attr.length=sizeof(int);
		attr.position=6;
		attributes.push_back(attr);


		return 0;
	}
	else{
		////f("Error! PrepareCatalogDescriptor can only be used to get Catalog's record descriptor\n");
		return -1;
	}

}

RC RelationManager::CreateTablesRecord(void *data,int tableid,const string tablename,int systemtable){
	int offset=0;
	int size = tablename.size();
	char nullind=0;
	
	//copy null indicator
	memcpy((char *)data+offset,&nullind,1);
	offset=offset+1;

	memcpy((char *)data+offset,&tableid,sizeof(int));
	offset=offset+sizeof(int);
	//copy table name
	memcpy((char *)data+offset,&size,sizeof(int));
	offset=offset+sizeof(int);

	memcpy((char *)data+offset,tablename.c_str(),size);
	offset=offset+size;

	//copy file name
	memcpy((char *)data+offset,&size,sizeof(int));
	offset=offset+sizeof(int);

	memcpy((char *)data+offset,tablename.c_str(),size);
	offset=offset+size;

	//copyt SystemTable
	memcpy((char *)data+offset,&systemtable,sizeof(int));
	offset=offset+sizeof(int);
	
	////f("\ncreate table record offset is %d\n",offset);

	return 0;

}

RC RelationManager::CreateColumnsRecord(void * data,int tableid, Attribute attr, int position, int nullflag){
	int offset=0;
	int size=attr.name.size();
	char null[1];
	null[0]=0;


	//null indicator
	memcpy((char *)data+offset,null,1);
	offset+=1;

	memcpy((char *)data+offset,&tableid,sizeof(int));
	offset=offset+sizeof(int);

	//copy VarChar
	memcpy((char *)data+offset,&size,sizeof(int));
	offset=offset+sizeof(int);
	memcpy((char *)data+offset,attr.name.c_str(),size);
	offset=offset+size;

	//copy  type
	memcpy((char *)data+offset,&(attr.type),sizeof(int));
	offset=offset+sizeof(int);

	//copy attribute length
	memcpy((char *)data+offset,&(attr.length),sizeof(int));
	offset=offset+sizeof(int);

	//copy position
	memcpy((char *)data+offset,&position,sizeof(int));
	offset=offset+sizeof(int);

	//copy nullflag
	memcpy((char *)data+offset,&nullflag,sizeof(int));
	offset=offset+sizeof(int);
	
	////f("\ncreate column record offset is %d\n",offset);

	return 0;

}
RC RelationManager::UpdateColumns(int tableid,vector<Attribute> attributes){
	int size=attributes.size();
	RecordBasedFileManager *rbfm=RecordBasedFileManager::instance();
	FileHandle table_filehandle;
	char *data=(char *)malloc(PAGE_SIZE);
	vector<Attribute> columndescriptor;
	RID rid;

	PrepareCatalogDescriptor("Columns",columndescriptor);
	if(rbfm->openFile("Columns", table_filehandle)==0){

		for(int i=0;i<size;i++){
			CreateColumnsRecord(data,tableid,attributes[i],attributes[i].position,0);
			rbfm->insertRecord(table_filehandle,columndescriptor,data,rid);
			
			////f("In UpdateColumns\n");
			rbfm->printRecord(columndescriptor,data);
		}
		rbfm->closeFile(table_filehandle);
		free(data);
		return 0;
	}

	////f("There is bug on UpdateColumns\n");
	free(data);
	return -1;
}

int RelationManager::GetFreeTableid(){

	RM_ScanIterator rm_ScanIterator;
	RID rid;
	char *data=(char *)malloc(PAGE_SIZE);

	vector<string> attrname;
	attrname.push_back("table-id");
	int tableID = -1;
	int foundID;
	bool scanID[TABLE_SIZE];
	std::fill_n(scanID,TABLE_SIZE,0);

	void *v = malloc(1);
	if( scan("Tables","",NO_OP,v,attrname,rm_ScanIterator)==0 ){

		while(rm_ScanIterator.getNextTuple(rid,data)!=RM_EOF){
			//!!!! skip null indicator
			memcpy(&foundID,(char *)data+1,sizeof(int));
			//f("found table ID is %d\n",foundID);
			scanID[foundID-1]=true;

		}
		for(int i=0;i<TABLE_SIZE;i++){
			if(!scanID[i]){
				tableID=i+1;
				break;
			}
		}

		free(data);
		rm_ScanIterator.close();
		//f("Get free table id: %d\n",tableID);
		free(v);
		return tableID;
	}

	//f("There is bug on GetFreeTableid\n");
	return -1;

}
RC RelationManager::CreateVarChar(void *data,const string &str){
	int size=str.size();
	int offset=0;
	memcpy((char *)data+offset,&size,sizeof(int));
	offset+=sizeof(int);
	memcpy((char *)data+offset,str.c_str(),size);
	offset+=size;


	return 0;
}

int RelationManager::VarCharToString(void *data,string &str){
	int size;
	int offset=0;
	char * VarCharData=(char *) malloc(PAGE_SIZE);

	memcpy(&size,(char *)data+offset,sizeof(int));
	offset+=sizeof(int);

	memcpy(VarCharData,(char *)data+offset,size);
	offset+=size;

	VarCharData[size]='\0';
	string tempstring(VarCharData);
	str=tempstring;


	free(VarCharData);

	return 0;


}

RelationManager* RelationManager::_rm = 0;

RelationManager* RelationManager::instance()
{
    if(!_rm)
        _rm = new RelationManager();

    return _rm;
}

RelationManager::RelationManager()
{  
//    debug = true;
    rbfm = RecordBasedFileManager::instance();
}

RelationManager::~RelationManager()
{
}

RC RelationManager::createCatalog()
{
	vector<Attribute> tablesdescriptor;
	vector<Attribute> columnsdescriptor;

	RecordBasedFileManager *rbfm=RecordBasedFileManager::instance();
	FileHandle table_filehandle;
	RID rid;


	//creat Tables
	if((rbfm->createFile("Tables"))==0){

		void *data=malloc(PAGE_SIZE);
		int tableid=1;
		int systemtable=1;

		// open table file 
		rbfm->openFile("Tables",table_filehandle);
		
		PrepareCatalogDescriptor("Tables",tablesdescriptor);
		CreateTablesRecord(data,tableid,"Tables",systemtable);
		RC rc = rbfm->insertRecord(table_filehandle,tablesdescriptor,data,rid);
		assert( rc == 0 && "insert table should not fail");
		
		tableid=2;
		CreateTablesRecord(data,tableid,"Columns",systemtable);
		rc = rbfm->insertRecord(table_filehandle,tablesdescriptor,data,rid);
		assert( rc == 0 && "insert table should not fail");
		// close table file
		rbfm->closeFile(table_filehandle);


		//create Columns
		if((rbfm->createFile("Columns"))==0){
			UpdateColumns(1,tablesdescriptor);
			PrepareCatalogDescriptor("Columns",columnsdescriptor);
			UpdateColumns(tableid,columnsdescriptor);

			free(data);
			//f("successfully create catalog\n");
			return 0;
		}
	}

	//f("Fail to create catalog\n");
	return -1;
}

RC RelationManager::deleteCatalog()
{

	if(rbfm->destroyFile("Tables")==0){
		if(rbfm->destroyFile("Columns")==0){
			//f("successfully delete Tables and Columns \n");
			return 0;
		}
	}
    return -1;
}

RC RelationManager::createTable(const string &tableName, const vector<Attribute> &attrs)
{
	RecordBasedFileManager *rbfm=RecordBasedFileManager::instance();
	FileHandle filehandle;
	FileHandle nullhandle;
	vector<Attribute> tablesdescriptor;
	char *data=(char *)malloc(PAGE_SIZE);
	RID rid;
	int tableid;
	vector<Attribute> tempattrs=attrs;
	for(int i=0;i< tempattrs.size();i++){
		tempattrs[i].position=i+1;
	}
	if(rbfm->createFile(tableName)==0){


		if(rbfm->openFile("Tables",filehandle)==0){
			//f("before get free table id \n");
			tableid=GetFreeTableid();
			//f("table id is %d\n",tableid);
			
			PrepareCatalogDescriptor("Tables",tablesdescriptor);
			CreateTablesRecord(data,tableid,tableName,0);
			RC rc = rbfm->insertRecord(filehandle,tablesdescriptor,data,rid);
			assert( rc == 0 && "insert table should not fail");
			
			//f("In createTable\n");
			rbfm->printRecord(tablesdescriptor,data);
			    
			rbfm->closeFile(filehandle);
			if(UpdateColumns(tableid,tempattrs)==0){
				free(data);
				return 0;
			}
		}

	}
	assert( false && "There is bug on createTable \n");
	free(data);
	return -1;
}

int RelationManager::getTableId(const string &tableName){

	RM_ScanIterator rm_ScanIterator;
	RID rid;
	int tableid = -1;
	char *VarChardata=(char *)malloc(PAGE_SIZE);
	char *data=(char *)malloc(PAGE_SIZE);
	vector<string> attrname;
	attrname.push_back("table-id");
	int count=0;

	CreateVarChar(VarChardata,tableName);

	//f("In getTableId tableName: %s\n",tableName.c_str());

	if( scan("Tables","table-name",EQ_OP,VarChardata,attrname,rm_ScanIterator) == 0 ){
		while(rm_ScanIterator.getNextTuple(rid,data)!=RM_EOF){

			//!!!! skip null indicator
			memcpy(&tableid,(char *)data+1,sizeof(int));
			count++;
			break;
		}
		rm_ScanIterator.close();

		free(VarChardata);
		free(data);
		return tableid;
	}
	free(VarChardata);
	free(data);
	return -1;
}

RC RelationManager::deleteTable(const string &tableName)
{
	FileHandle filehandle;
	RM_ScanIterator rm_ScanIterator;
	RM_ScanIterator rm_ScanIterator2;
	RID rid;
	int tableid;

	char *data=(char *)malloc(PAGE_SIZE);
	vector<string> attrname;
	attrname.push_back("table-id");
	vector<RID> rids;

	vector<Attribute> tablesdescriptor;
	PrepareCatalogDescriptor("Tables",tablesdescriptor);
	vector<Attribute> columnsdescriptor;
	PrepareCatalogDescriptor("Columns",columnsdescriptor);
	if(tableName.compare("Tables") && tableName.compare("Columns")){
		if(rbfm->destroyFile(tableName)==0){
			tableid=getTableId(tableName);

			//f("\n\nDelete table id %d\n",tableid);

			rbfm->openFile("Tables",filehandle);

			if(RelationManager::scan("Tables","table-id",EQ_OP,&tableid,attrname,rm_ScanIterator)==0){
				while(rm_ScanIterator.getNextTuple(rid,data)!=RM_EOF){
					rids.push_back(rid);
				}
				for(int j=0;j<rids.size();j++){
					rbfm->deleteRecord(filehandle,tablesdescriptor,rids[j]);

				}
				rbfm->closeFile(filehandle);
				rm_ScanIterator.close();

				rbfm->openFile("Columns",filehandle);
				if( scan("Columns","table-id",EQ_OP,&tableid,attrname,rm_ScanIterator2) == 0 ){
					while(rm_ScanIterator2.getNextTuple(rid,data)!=RM_EOF){
						rbfm->deleteRecord(filehandle,columnsdescriptor,rid);
					}
					rm_ScanIterator2.close();
					rbfm->closeFile(filehandle);

					free(data);
					//f("Successfully delete %s\n",tableName.c_str());
					return 0;

				}


			}

		}
	}
	assert( false &&"There is bug on deleteTable ");
	free(data);
    return -1;
}

RC RelationManager::getAttributes(const string &tableName, vector<Attribute> &attrs)
{

	RM_ScanIterator rm_ScanIterator;
	RID rid;
	int tableid;
	char *data=(char *)malloc(PAGE_SIZE);
	vector<string> attrname;
	attrname.push_back("column-name");
	attrname.push_back("column-type");
	attrname.push_back("column-length");
	attrname.push_back("column-position");

	attrname.push_back("NullFlag");

	Attribute attr;
	string tempstr;
	int nullflag;
	int offset = 0;

	tableid=getTableId(tableName);  // scan table ID 
	//f("tableid ========= %d\n",tableid);

	//assert( tableid != -1 && "Table id shouldn't be -1 , maybe forgot to createCatalog first? ");
	//if( tableid == -1 ) return -1;

	if( scan("Columns","table-id",EQ_OP,&tableid,attrname,rm_ScanIterator) == 0 ){

		while(rm_ScanIterator.getNextTuple(rid,data)!=RM_EOF){
			//skip null indicatior
			offset=1;
			VarCharToString(data+offset,tempstr);
			attr.name=tempstr;
			offset+=(sizeof(int)+tempstr.size());
			memcpy(&(attr.type),data+offset,sizeof(int));
			offset+=sizeof(int);
			memcpy(&(attr.length),data+offset,sizeof(int));
			offset+=sizeof(int);
			memcpy(&(attr.position),data+offset,sizeof(int));
			offset+=sizeof(int);
			memcpy(&(nullflag),data+offset,sizeof(int));
			offset+=sizeof(int);
			if(nullflag==1){
				attr.length=0;
			}
			attrs.push_back(attr);

		}
		rm_ScanIterator.close();
		free(data);

		//f("Successfully getAttribute\nSize of attrs %d\n",attrs.size());
		//f("the first attriubt name: %s\nthe last attriubt name: %s\n",attrs[0].name.c_str(),attrs[attrs.size()-1].name.c_str());
		return 0;
	}

	//assert( false && "There is bug on getAttribute \n");
	free(data);
	return -1;

}
int RelationManager::IsSystemTable(const string &tableName){
	RM_ScanIterator rm_ScanIterator;
	RID rid;
	int systemtable;
	char *VarChardata=(char *)malloc(PAGE_SIZE);
	char *data=(char *)malloc(PAGE_SIZE);
	vector<string> attrname;
	attrname.push_back("SystemTable");
	int count=0;

	CreateVarChar(VarChardata,tableName);

	if( scan("Tables","table-name",EQ_OP,VarChardata,attrname,rm_ScanIterator) == 0 ){
		while(rm_ScanIterator.getNextTuple(rid,data)!=RM_EOF){
			//!!!! skip null indicator
			memcpy(&systemtable,(char *)data+1,sizeof(int));
			count++;
			break;
//			if(count>=2){
//				cout<<"There are two record in Tables with same table name "<<endl;
//			}
		}
		rm_ScanIterator.close();
		free(VarChardata);
		free(data);
		return systemtable;

	}

	free(VarChardata);
	free(data);
	return -1;
}

RC RelationManager::insertTuple(const string &tableName, const void *data, RID &rid)
{
	vector<Attribute> recordDescriptor;
	if( getTableAttributes( tableName, recordDescriptor ) != 0 )
		return -1;

	FileHandle fileHandle;
	if ( _rbf_manager->openFile( tableName, fileHandle ) != 0 )
			return -1;

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
	if ( _rbf_manager->openFile( tableName, fileHandle ) != 0 )
		return -1;

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
	if ( _rbf_manager->openFile( tableName, fileHandle ) != 0 )
		return -1;

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
	_rbf_manager = RecordBasedFileManager::instance();
	FileHandle fileHandle;
	if ( _rbf_manager->openFile( tableName, fileHandle ) != 0 )
		return -1;
	// prepare recordDescriptor
	vector<Attribute> recordDescriptor;
	if( getTableAttributes( tableName, recordDescriptor ) != 0)
		return -1;

	// set up rm_ScanIterator
	rm_ScanIterator._fileHandle = fileHandle;
	rm_ScanIterator._rbf_scanIter._fileHandlePtr = &rm_ScanIterator._fileHandle;

	// run record scan
	RC success = _rbf_manager->scan( rm_ScanIterator._fileHandle, recordDescriptor, conditionAttribute, compOp, value, attributeNames, rm_ScanIterator._rbf_scanIter );
	return success;
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
RC RelationManager::addAttribute(const string &tableName, const Attribute &attr)
{

}

// Extra credit work
RC RelationManager::dropAttribute(const string &tableName, const string &attributeName)
{

}

/*
RC RelationManager::printTable(const string &tableName){
	RM_ScanIterator rm_ScanIterator;
	RID rid;
	char *data=(char *)malloc(PAGE_SIZE);
	vector<Attribute> recordDescriptor;
	getAttributes(tableName,recordDescriptor);
	vector<string> attrname;
	for(int j=0;j<recordDescriptor.size();j++){
		attrname.push_back(recordDescriptor[j].name);
	}

	if( scan(tableName,"",NO_OP,NULL,attrname,rm_ScanIterator)==0 ){
		while(rm_ScanIterator.getNextTuple(rid,data)!=RM_EOF){
			//!!!! skip null indicator
			rbfm->printRecord(recordDescriptor,data);

		}
		free(data);
		rm_ScanIterator.close();
		//f("Successfully printTabale \n");
		return 0;

	}
	//f("There is bug on printTabale \n");
	free(data);
	return -1;
}
*/
