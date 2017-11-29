
#ifndef _rm_h_
#define _rm_h_

#include <string>
#include <vector>
#include <climits>
#include <math.h>
#include <stdlib.h>
#include <algorithm>
#include <sstream>
#include <iostream>
#include <cstdio>
#include <cstring>
#include <bitset>

#include "../rbf/rbfm.h"
#include "../ix/ix.h"

using namespace std;
# define TABLE_SIZE 4096
# define RM_EOF (-1)  // end of a scan operator

#define INDEXTABLE "indexTable"
// RM_ScanIterator is an iteratr to go through tuples

//class RM_ScanIterator : public DebugMsg {

class RM_ScanIterator {
private:
	//RID _cursor;

public:
  RM_ScanIterator() {};
  ~RM_ScanIterator() {};

  RBFM_ScanIterator _rbf_scanIter;

  FileHandle _fileHandle;

  // "data" follows the same format as RelationManager::insertTuple()
  RC getNextTuple(RID &rid, void *data);
  RC close();
};

// RM_IndexScanIterator is an iterator to go through index entries
class RM_IndexScanIterator {
  public:
    RM_IndexScanIterator() {};    // Constructor
    ~RM_IndexScanIterator() {};   // Destructor

    // "key" follows the same format as in IndexManager::insertEntry()
    RC getNextEntry(RID &rid, void *key);    // Get next matching entry
    RC close();                 // Terminate index scan

    IX_ScanIterator _ix_scanIter;
    IXFileHandle _ixFileHandle;
    string _indexFName;

};

// Relation Manager
class RelationManager //: public DebugMsg
{
public:
  static RelationManager* instance();

  RC createCatalog();

  RC deleteCatalog();

  RC createTable(const string &tableName, const vector<Attribute> &attrs);

  RC deleteTable(const string &tableName);

  RC getAttributes(const string &tableName, vector<Attribute> &attrs);
  // accessory function for getTableAttributes
  RC getTableAttributes(const string &tableName, vector<Attribute> &attrs);

  RC insertTuple(const string &tableName, const void *data, RID &rid);

  RC deleteTuple(const string &tableName, const RID &rid);

  RC updateTuple(const string &tableName, const void *data, const RID &rid);

  RC readTuple(const string &tableName, const RID &rid, void *data);

  // mainly for debugging
  // Print a tuple that is passed to this utility method.
  RC printTuple(const vector<Attribute> &attrs, const void *data);

  // mainly for debugging
  RC readAttribute(const string &tableName, const RID &rid, const string &attributeName, void *data);

  // scan returns an iterator to allow the caller to go through the results one by one.
  RC scan(const string &tableName,
      const string &conditionAttribute,
      const CompOp compOp,                  // comparison type such as "<" and "="
      const void *value,                    // used in the comparison
      const vector<string> &attributeNames, // a list of projected attributes
      RM_ScanIterator &rm_ScanIterator);
  RC printTable(const string &tableName);

  RC PrepareCatalogDescriptor(string tablename,vector<Attribute> &attributes);

  // project 4 index
  RC createIndex(const string &tableName, const string &attributeName);

  RC destroyIndex(const string &tableName, const string &attributeName);

  // indexScan returns an iterator to allow the caller to go through qualified entries in index
  RC indexScan(const string &tableName,
                        const string &attributeName,
                        const void *lowKey,
                        const void *highKey,
                        bool lowKeyInclusive,
                        bool highKeyInclusive,
                        RM_IndexScanIterator &rm_IndexScanIterator);
  // create Index Table
  RC createIndexTable();

// Extra credit work (10 points)
public:
  RC dropAttribute(const string &tableName, const string &attributeName);

  RC addAttribute(const string &tableName, const Attribute &attr);
  int getTableId(const string &tableName);

protected:
  RelationManager();
  ~RelationManager();

private:
  static RelationManager *_rm;
  RecordBasedFileManager *_rbf_manager;

  int GetFreeTableid();
  int IsSystemTable(const string &tableName);
  RC UpdateColumns(int tableid,vector<Attribute> attributes);
  RC CreateVarChar(void *data,const string &str);


  RC CreateTablesRecord(void *data,int tableid,string tablename,int systemtable);
  RC CreateColumnsRecord(void * data,int tableid, Attribute attr, int position, int nullflag);

};

bool sortAttr(Attribute a, Attribute b);
string indexFileName(string tableName, string attributeName);
#endif
