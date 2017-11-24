#ifndef _ix_h_
#define _ix_h_

#include <vector>
#include <string>
#include <sstream>
#include <map>

#include "../rbf/rbfm.h"

# define IX_EOF (-1)  // end of the index scan

typedef struct
{
  int pid;			// newly split pageNum
  int entryLength;  // length of popped entry
  void* entry;		// popped entry
} POP;

class IXFileHandle;
class IX_ScanIterator;
class IXPagedFileManager;

class IndexManager {

public:
	static IndexManager* instance();

	// Create an index file.
	RC createFile(const string &fileName);

	// Delete an index file.
	RC destroyFile(const string &fileName);

	// Open an index and return an ixfileHandle.
	RC openFile(const string &fileName, IXFileHandle &ixfileHandle);

	// Close an ixfileHandle for an index.
	RC closeFile(IXFileHandle &ixfileHandle);

	// Insert an entry into the given index that is indicated by the given ixfileHandle.
	RC insertEntry(IXFileHandle &ixfileHandle, const Attribute &attribute, const void *key, const RID &rid);

	// Delete an entry from the given index that is indicated by the given ixfileHandle.
	RC deleteEntry(IXFileHandle &ixfileHandle, const Attribute &attribute, const void *key, const RID &rid);

	// Initialize and IX_ScanIterator to support a range search
	RC scan(IXFileHandle &ixfileHandle,
			const Attribute &attribute,
			const void *lowKey,
			const void *highKey,
			bool lowKeyInclusive,
			bool highKeyInclusive,
			IX_ScanIterator &ix_ScanIterator);

	// Print the B+ tree in pre-order (in a JSON record format)
	void printBtree(IXFileHandle &ixfileHandle, const Attribute &attribute) const;

	// Self-added functions
	bool isEmpty(IXFileHandle& ixfh);
	bool isLeaf(const void* page);

	short getFreeSpace(const void* page);
	RC setFreeSpace(short freeSpace, void* page);

	short getNumberOfKeys(const void* page);
	RC setNumberOfKeys(short numberOfKeys, void* page);

	short getKeyLength(short keyNum, const void* page);
	RC setKeyLength(short keyLength, short keyNum, void* page);

	short getKeyOffset(short keyNum, const void* page);
	RC setKeyOffset(short keyOffset, short keyNum, void* page);

	bool validKeyNum(short keyNum, const void* page);
	bool validKeyOffset(short keyOffset);
	bool validFreeSpace(short freeSpace);

	short getTotalOffset(const void* page);

	RC setPointer(int pageNum, int keyNum, void* page);
	RC setLeftMostPointer(int pageNum, void* page);
	RC getLeftPointer(int& pageNum, short slotNum, const void* page);
	RC getRightPointer(int& pageNum, short slotNum, const void* page);

	short getFlag(const void* page);
	RC setFlag(short flag, void* page);

	RC key2Int(int& value, RID& rid, const void* entry);
	RC key2Real(float& value, RID& rid, const void* entry);

	short compact(short slotNum, short compactOffset, void* page);

	RC insertCompositeKey(IXFileHandle& ixfh, RID& rid, POP& pop, vector<POP>& pops, void* entry);
	RC searchEntry(void* page, vector<RID>& rids, IXFileHandle& ixfh, int& pageNum, const void* newEntry, short& newEntryLength);
	RC searchExactEntry(IXFileHandle ixfh, int pageNum, const void* entry, RID& rid, short& entryLength);

	// compare 2 entries according to both key and rid, true if the former is greater
	bool compareEntry(AttrType& type, const void* entry1, const void* entry2, short& length1, short& length2);
	bool compareRid(const RID& rid1, const RID& rid2);

	// compare 2 entries according to both key and rid, true if both are same
	bool compareExactEntry(AttrType& type, const void* entry1, const void* entry2, short& length1, short& length2);
	bool compareExactRid(RID& rid1, RID& rid2);

	short buildEntry(AttrType type, const void* key, const RID& rid, void* entry);

    protected:
        IndexManager();
        ~IndexManager();

    private:
        static IndexManager *_index_manager;
        IXPagedFileManager *ixpfm;

        int printBtreeHelper(unsigned currentNum, unsigned level, IXFileHandle &ixfileHandle, const Attribute &attribute) const;
        int trueDelete(IXFileHandle &ixfileHandle, const Attribute &attribute, const void *key, RID &entryRid, const RID &rid); // return 0 for finish, 1 for not
        string Other2string(int type, unsigned unsignedValue, int intValue, float floatValue) const; // 0 for unsigned, 1 for int, 2 for float
};

class IXFileHandle {
    public:

    // variables to keep counter for each operation
    unsigned readPageCounter;
    unsigned writePageCounter;
    unsigned appendPageCounter;
    	unsigned pageNumber;
	unsigned rootNum;

	AttrType type;
	string fileName;
	FILE *fp;

    // Constructor
    IXFileHandle();

    // Destructor
    ~IXFileHandle();
    RC readPage(PageNum pageNum, void *data);                             // Get a specific page
    RC writePage(PageNum pageNum, const void *data);                      // Write a specific page
    RC appendPage(const void *data);                                      // Append a specific page
    unsigned getNumberOfPages();                                          // Get the number of pages in the file
	// Put the current counter values of associated PF FileHandles into variables
	RC collectCounterValues(unsigned &readPageCount, unsigned &writePageCount, unsigned &appendPageCount);

};

class IX_ScanIterator {
    public:

		// Constructor
        IX_ScanIterator();

        // Destructor
        ~IX_ScanIterator();

        // Get next matching entry
        RC getNextEntry(RID &rid, void *key);

        // Terminate index scan
        RC close();

        RC prepare(IXFileHandle &ixfileHandle, const Attribute &attribute,
        		const void *lowKey, const void *highKey, bool lowKeyInclusive,
        		bool highKeyInclusive);

    private:
        IXFileHandle *ixfileHandle;
        Attribute attribute;
        const void *lowKey;
		const void *highKey;
        bool lowKeyInclusive;
        bool highKeyInclusive;

        int pageNum;
        short slotNum;
        bool firstSearch;

        int getToLeafPage(unsigned rootPage);
        void iterCompare(void *space, short keyOffset, short keyLength, int &lowResult, int &highResult); // current ? key : -1 for smaller, 0 for equal, 1 for larger
    		RID lastRID;

    		string lowVarchar;
    		string highVarchar;
    		int lowInt;
    		int highInt;
    		float lowReal;
    		float highReal;
};

class IXPagedFileManager {
public:
    static IXPagedFileManager* instance();                                  // Access to the _pf_manager instance

	IXPagedFileManager();
	~IXPagedFileManager();

    RC createIXFile    (const string &fileName);                            // Create a new file
    RC destroyIXFile   (const string &fileName);                            // Destroy a file
    RC openIXFile      (const string &fileName, IXFileHandle &ixfileHandle);    // Open a file
    RC closeIXFile     (IXFileHandle &ixfileHandle);

private:
    static IXPagedFileManager *_ix_pf_manager;

};

#endif
