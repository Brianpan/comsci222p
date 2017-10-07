#include "pfm.h"

PagedFileManager* PagedFileManager::_pf_manager = 0;

PagedFileManager* PagedFileManager::instance()
{
    if(!_pf_manager)
        _pf_manager = new PagedFileManager();

    return _pf_manager;
}


PagedFileManager::PagedFileManager()
{
	if( !boost::filesystem::exists(_folder_name) )
	{
		boost::filesystem::path path (_folder_name);
		boost::filesystem::create_directory(path);
	}

}


PagedFileManager::~PagedFileManager()
{
	if( !boost::filesystem::exists(_folder_name) )
	{

		boost::filesystem::remove(_folder_name);
	}
}


RC PagedFileManager::createFile(const string &fileName)
{
	if( boost::filesystem::exists(fileName) )
    {
    	cout<<"File already existed !"<<endl;
		return -1;
    }

    // create file

    fstream fp;
    fp.open(fileName, ios::out);
    if( !fp ){
    	cout<<"Create file failed !";
    	return -1;
    }
    fp.close();
    return 0;
}


RC PagedFileManager::destroyFile(const string &fileName)
{

	if( !boost::filesystem::exists(fileName) )
    {
    	cout<<"File not existed !"<<endl;
		return -1;
    }

    boost::filesystem::remove(fileName);

    return 0;
}


RC PagedFileManager::openFile(const string &fileName, FileHandle &fileHandle)
{

	if(fileHandle._handler != NULL)
	{
		cout<<"File handler in used !"<<endl;
		return -1;
	}

	if( !boost::filesystem::exists(fileName) )
	{
	    	cout<<"File not existed !"<<endl;
			return -1;
	}
	// open file
	// for here we should init
	fileHandle._handler= new fstream();
	fileHandle._handler->open(fileName);
	fileHandle.fetchFileData();

	return 0;
}


RC PagedFileManager::closeFile(FileHandle &fileHandle)
{
    if(fileHandle._handler == NULL)
    {
    	cout<<"File not existed !"<<endl;
    	return -1;
    }

    // save pages to disk

    // close file
    fileHandle.saveCounter();
    fileHandle._handler->close();

	delete fileHandle._handler;
	fileHandle._handler = NULL;
    return 0;
}


FileHandle::FileHandle()
{
    pageCounter = 0;
	readPageCounter = 0;
    writePageCounter = 0;
    appendPageCounter = 0;
}


FileHandle::~FileHandle()
{

}


RC FileHandle::readPage(PageNum pageNum, void *data)
{
    if( (int)pageNum >= getNumberOfPages() )
    {
    	cout<<"Read page not existed !"<<endl;
    	return -1;
    }

    // read from specific position
    // first 4096 are private page
    int page_offset = -(getNumberOfPages() - pageNum)*PAGE_SIZE;
    _handler->seekg(page_offset, ios_base::end);
    _handler->read( (char*) data, PAGE_SIZE );

    readPageCounter += 1;
	return 0;
}


RC FileHandle::writePage(PageNum pageNum, const void *data)
{
    if(pageNum >= pageCounter)
    {
    	cout<<"pageNum not existed!"<<endl;
    	return -1;
    }
    int page_offset = (pageNum+1)*PAGE_SIZE;
    _handler->clear();
    _handler->seekg(page_offset, ios_base::beg);
    _handler->write((char*) data, PAGE_SIZE);

    writePageCounter += 1;
    return 0;
}


RC FileHandle::appendPage(const void *data)
{
	if(_handler == NULL)
	{
		cout<<"Not open a file yet !"<<endl;
		return -1;
	}
	_handler->seekg(0, ios_base::end);
	_handler->write( (char*) data, PAGE_SIZE );

	// add counter
	appendPageCounter += 1;
	pageCounter += 1;

	return 0;
}


unsigned FileHandle::getNumberOfPages()
{
//    _handler->seekg(0, ios_base::end);
//    ios_base::streampos end_pos = _handler->tellg();
//    unsigned page_size = (unsigned) end_pos/PAGE_SIZE;
//	return page_size;
	return pageCounter;
}


RC FileHandle::collectCounterValues(unsigned &readPageCount, unsigned &writePageCount, unsigned &appendPageCount)
{
    readPageCount = readPageCounter;
    writePageCount = writePageCounter;
    appendPageCount = appendPageCounter;
	return 0;
}

void FileHandle::fetchFileData()
{

	// check if there is record exists
	// if so load them
	//the order of integer is pageCounter, readPageCounter, writePageCounter, appendPageCounter end with #
	_handler->seekg(0,  ios_base::beg);
	unsigned counter[4];
	int p = 0;

	_handler->seekg(0, ios_base::end);
	ios_base::streampos end_pos = _handler->tellg();
	if(end_pos >= PAGE_SIZE)
	{
		_handler->seekg(0,  ios_base::beg);
		//notive the brackets
		while( p<4 && (*_handler)>>counter[p] )
		{
			p++;
		}
	}
	// init the hidden page
	if(p != 4 )
	{
		_handler->seekg(0, ios_base::beg);

		memset(counter, 0, sizeof(unsigned)*4);
		string init = "0 0 0 0#";
		unsigned init_size = sizeof(init.c_str());
		_handler->write( (char *)init.c_str(), init_size );

		// write the rest of #
		int rest_of_bytes = PAGE_SIZE - init_size;
		char rest_chars[rest_of_bytes];
		for(int i=0;i < rest_of_bytes;i++)
		{
			rest_chars[i] = '#';
		}
		_handler->write(rest_chars, rest_of_bytes);
	}

	pageCounter = counter[0];
	readPageCounter = counter[1];
	writePageCounter = counter[2];
	appendPageCounter = counter[3];

	// release counter
	return;
}

void FileHandle::saveCounter()
{
	//
	int page_offset = -(pageCounter+1)*PAGE_SIZE;
	//http://www.cplusplus.com/forum/beginner/30644/
	// back to the first of file pointer
	_handler->clear();
	_handler->seekg(0, ios_base::beg);

	// save those counters to the file
	string record_counters = to_string(pageCounter) + " " + to_string(readPageCounter)+ " " + to_string(writePageCounter) +" "+ to_string(appendPageCounter)+"#";

	unsigned init_size = sizeof(record_counters.c_str());
	_handler->write( record_counters.c_str(), init_size );

	// write the rest of #
	int rest_of_bytes = PAGE_SIZE - init_size;
	char rest_chars[rest_of_bytes];
	for(int i=0;i < rest_of_bytes;i++)
	{
		rest_chars[i] = '#';
	}
	_handler->write(rest_chars, rest_of_bytes);
	return;
}
// accessory functions
string addressToString(FileHandle& fh){
	ostringstream buffer;
	buffer << &fh;
	return buffer.str();
}
