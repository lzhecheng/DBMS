#include <iostream>

#include <cstdlib>
#include <cstdio>
#include <cstring>

#include "ix.h"
#include "ix_test_util.h"

IndexManager *indexManager;

int test(const string &indexFileName, const Attribute &attribute)
{
    // Functions tested
    // 1. Create Index File
    // 2. Open Index File
    // 3. Insert entry
    // 4. Scan entries NO_OP -- open
    // 5. Scan close **
    // 6. Close Index File
    // NOTE: "**" signifies the new functions being tested in this test case.
    cerr << endl << "***** In my Test Case *****" << endl;

    RID rid;
    IXFileHandle ixfileHandle;
    IX_ScanIterator ix_ScanIterator;

    // create index file
    RC rc = indexManager->createFile(indexFileName);
    assert(rc == success && "indexManager::createFile() should not fail.");

    // open index file
    rc = indexManager->openFile(indexFileName, ixfileHandle);
    assert(rc == success && "indexManager::openFile() should not fail.");

    int numOfTuples = 50000;
    unsigned key;

	for(unsigned i = 0; i <= numOfTuples; i++)
	{
		key = i;
		rid.pageNum = key;
		rid.slotNum = key * 3;

		rc = indexManager->insertEntry(ixfileHandle, attribute, &key, rid);
		assert(rc == success && "indexManager::insertEntry() should not fail.");
	}

    // Scan
    rc = indexManager->scan(ixfileHandle, attribute, NULL, NULL, true, true, ix_ScanIterator);
    assert(rc == success && "indexManager::scan() should not fail.");

    indexManager->printBtree(ixfileHandle, attribute);

    // Close Index
    rc = indexManager->closeFile(ixfileHandle);
    assert(rc == success && "indexManager::closeFile() should not fail.");

    return success;
}


int main()
{
    // Global Initialization
    indexManager = IndexManager::instance();

    const string indexFileName = "age1_idx";
    Attribute attrAge;
    attrAge.length = 4;
    attrAge.name = "age1";
    attrAge.type = TypeInt;

    indexManager->destroyFile("age1_idx");

    RC result = test(indexFileName, attrAge);
    if (result == success) {
        cerr << "***** my Test Case finished. The result will be examined. *****" << endl;
        return success;
    } else {
        cerr << "***** [FAIL] my Test Case failed. *****" << endl;
        return fail;
    }

}

