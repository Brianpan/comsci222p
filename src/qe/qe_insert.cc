#include <fstream>
#include <iostream>
#include <vector>
#include <cstdlib>
#include <cstdio>
#include <cstring>

#include "qe_test_util.h"

int main() {
	// Tables created: left
	// Indexes created: left.B, left.C

	// Initialize the system catalog
	if (deleteAndCreateCatalog() != success) {
		cerr << "***** deleteAndCreateCatalog() failed." << endl;
		cerr << "***** [FAIL] QE Test Case 1 failed. *****" << endl;
		return fail;
	}

	if (createLargeLeftTable2() != success) {
		cerr << "***** [FAIL] QE Private Test Case 1 failed. *****" << endl;
		return fail;
	}

	cout<<"Insert records"<<endl;
	if (populateLargeLeftTable2() != success) {
		cerr << "***** [FAIL] QE Private Test Case 1 failed. *****" << endl;
		return fail;
	}
	cout<<"Create Index"<<endl;
	if (createIndexforLargeLeftB2() != success) {
		cerr << "***** [FAIL] QE Private Test Case 1 failed. *****" << endl;
		return fail;
	}

	cout<<"Insert more records"<<endl;
	if (addRecordsToLargeLeftTable2() != success) {
		cout << "***** [FAIL] QE Private Test Case 3 failed. *****" << endl;
		return fail;
	}
}
