#include "rm_test_util.h"

RC TEST_LEAK(const string &tableName)
{
    // Functions Tested for memory leak:

    cout << endl << "***** In RM Test LEAK *****" << endl;

    RC success = 0;
    for(int i = 0; i<2000; i++){
        int tableid = rm->getTableId("Columns");
    }
    

    cout << "***** Test Case LEAK Finished. The result will be examined. *****" << endl << endl;

    return success;
}

int main()
{
	// Insert Tuple
    RC rcmain = TEST_LEAK("tbl_employee4");

    return rcmain;
}
