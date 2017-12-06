
#include "rm.h"

void RelationManager::createRD4Tables(vector<Attribute> &RD1) {
	Attribute attr;
	attr.name = "table-id";
	attr.type = TypeInt;
	attr.length = (AttrLength)4;
	RD1.push_back(attr);

	attr.name = "table-name";
	attr.type = TypeVarChar;
	attr.length = (AttrLength)50;
	RD1.push_back(attr);

	attr.name = "file-name";
	attr.type = TypeVarChar;
	attr.length = (AttrLength)50;
	RD1.push_back(attr);

}


void RelationManager::createRD4Columns(vector<Attribute> &RD2) {
	Attribute attr;
	attr.name = "table-id";
	attr.type = TypeInt;
	attr.length = (AttrLength)4;
	RD2.push_back(attr);

	attr.name = "column-name";
	attr.type = TypeVarChar;
	attr.length = (AttrLength)50;
	RD2.push_back(attr);

	attr.name = "column-type";
	attr.type = TypeInt;
	attr.length = (AttrLength)4;
	RD2.push_back(attr);

	attr.name = "column-length";
	attr.type = TypeInt;
	attr.length = (AttrLength)4;
	RD2.push_back(attr);

	attr.name = "column-position";
	attr.type = TypeInt;
	attr.length = (AttrLength)4;
	RD2.push_back(attr);

}

void RelationManager::createData4Tables(int table_id, const string& table_name, const string& file_name, void *data) {
	int offset = 0, length1, length2;
	char ch = 0;
	memcpy((char*)data + offset, &ch, sizeof(char));
	offset += sizeof(char);
	memcpy((char*)data + offset, &table_id, sizeof(int));
	offset += sizeof(int);
	length1 = table_name.size();
	length2 = file_name.size();
	memcpy((char*)data + offset, &length1, sizeof(int));
	offset += sizeof(int);
	const char *cstr = table_name.c_str();
	for(int i = 0; i < length1; i++)
	{
		memcpy((char*)data + offset + i, (char*)cstr + i, 1);
	}
	offset += length1;
	memcpy((char*)data + offset, &length2, sizeof(int));
	offset += sizeof(int);
	const char *cstr1 = file_name.c_str();
	for(int i = 0; i < length1; i++)
	{
		memcpy((char*)data + offset + i, (char*)cstr1 + i, 1);
	}
	offset += length2;
}

void RelationManager::createData4Columns(int table_id, const string& column_name, int column_type, int column_length, int column_position, void *data) {
	int offset = 0, length1;
	char ch = 0;
	memcpy((char*)data + offset, &ch, sizeof(char));
	offset += sizeof(char);
	memcpy((char*)data + offset, &table_id, sizeof(int));
	offset += sizeof(int);
	length1 = column_name.size();
	memcpy((char*)data + offset, &length1, sizeof(int));
	offset += sizeof(int);

	const char *cstr = column_name.c_str();
	for(int i = 0; i < length1; i++)
	{
		memcpy((char*)data + offset + i, (char*)cstr + i, 1);
	}

	offset += length1;
	memcpy((char*)data + offset, &column_type, sizeof(int));
	offset += sizeof(int);
	memcpy((char*)data + offset, &column_length, sizeof(int));
	offset += sizeof(int);
	memcpy((char*)data + offset, &column_position, sizeof(int));
	offset += sizeof(int);
}

bool RelationManager::validTable(const string& tableName) {
    if (tableName == "Tables" || tableName == "Columns" || tableName == "") return false;
    return true;
}

void RelationManager::createRD(const string& tableName, vector<Attribute>& RD) {
	if (tableName == "Tables") createRD4Tables(RD);
	else if (tableName == "Columns") createRD4Columns(RD);
}


RC RelationManager::insertTupleNoCheck(FileHandle& fh, const string& tableName, const void* data1, RID& rid) {
	vector<Attribute> RD;
	getAttributes(tableName, RD);
	RC rc = rbfm.insertRecord(fh, RD, data1, rid);
	return rc;
}


RC RelationManager::updateTupleNoCheck(FileHandle& fh, const string& tableName, const void* data2, const RID& rid) {
    vector<Attribute> RD;
    getAttributes(tableName, RD);
    RC rc = rbfm.updateRecord(fh, RD, data2, rid);
    return rc;

}

RC RelationManager::deleteTupleNoCheck(FileHandle& fh, const string& tableName, const RID& rid) {
    vector<Attribute> RD;
    getAttributes(tableName, RD);
    RC rc = rbfm.deleteRecord(fh, RD, rid);
    return rc;

}

void RelationManager::createRD_id(vector<string>& RD_id) {
	RD_id.push_back("table-id");
}

void RelationManager::setZero(RID& rid) {
	rid.slotNum = rid.pageNum = 0;
}

void RelationManager::createRD_column(vector<string>& RD_column) {
	RD_column.push_back("column-name");
	RD_column.push_back("column-type");
	RD_column.push_back("column-length");
}

RC RelationManager::insertAttr(const void* data, vector<Attribute>& attrs) {
	Attribute attr;
	int nameLength, offset = 1;
	memcpy(&nameLength, (char*)data + offset, sizeof(int));
	offset += sizeof(int);
	string tmpString = "";
	for(int i = 0; i < nameLength; i++)
	{
		tmpString += *((char*)data+offset);
		offset++;
	}
	attr.name = tmpString;
	memcpy(&(attr.type), (char*)data + offset, sizeof(AttrType));
	offset += sizeof(AttrType);
	memcpy(&(attr.length), (char*)data + offset, sizeof(int));
	attrs.push_back(attr);

	return 0;
}


void RelationManager::createIndexName(string& indexName, const string &tableName, const string &attributeName) {
	indexName = "Index_" + tableName + "_" + attributeName;
}


bool RelationManager::hasIndex(RelationManager rm, const string& tableName, const string& attrName) {

	RC rc = 0;
	string indexName;
	createIndexName(indexName, tableName, attrName);

	// search "Tables" to see if index file exists
	vector<string> RD_id;
	RID rid;
	RM_ScanIterator rm_ScanIterator;
	createRD_id(RD_id);
	int length = indexName.length();
	void* name = malloc(length + 4);
	if (name == NULL) cerr << "malloc() failed" << endl;
	memset(name, 0, length + sizeof(int));
	memcpy(name, &length, sizeof(int));
	const char *cstr = indexName.c_str();

	FILE* fp = fopen(cstr, "rb+");
	if (fp == NULL) {
		return false;
	}
	else {
		fclose(fp);
	}
	return true;

}


RC RelationManager::insertInIndex(const string& tableName, const void* data, const RID& rid) {
    // deal with index file (if exists)
    vector<Attribute> attrs;
    Attribute attr;
    IXFileHandle ixfh;
    RelationManager rm;
    string indexName;
    RC rc = 0;

    void* key = malloc(PAGE_SIZE);
    if (key == NULL) return -12;
    memset(key, 0, PAGE_SIZE);
    rc = getAttributes(tableName, attrs);
    if (rc != 0) return -12;
    int length = 0, nullOffset = 0, dataOffset = attrs.size() / 8 + 1;
    for (int i = 0; i < attrs.size(); i++) {
    	attr = attrs[i];
    	createIndexName(indexName, tableName, attr.name);
    	nullOffset = i / 8;
    	char nullByte;
    	memcpy(&nullByte, (char*)data + nullOffset, 1);
    	if ((nullByte & (1 << (7 - i % 8))) == 0) {
    		if (attr.type == TypeVarChar) {
    			memcpy(&length, (char*)data + dataOffset, sizeof(int));
    			memcpy(key, (char*)data + dataOffset, length + sizeof(int));
    			dataOffset += sizeof(int) + length;
    		} else if (attr.type == TypeInt) {
    			memcpy(key, (char*)data + dataOffset, sizeof(int));
    			dataOffset += sizeof(int);
    		} else if (attr.type == TypeReal) {
    			memcpy(key, (char*)data + dataOffset, sizeof(float));
    			dataOffset += sizeof(float);
    		}

    		if (hasIndex(rm, tableName, attr.name)) {
    			ixm.openFile(indexName, ixfh);
    			if (rc != 0) return -12;
    			ixm.insertEntry(ixfh, attr, key, rid);
    			if (rc != 0) return -12;
    			ixm.closeFile(ixfh);
    			if (rc != 0) return -12;
    		}
    	}
    }

    free(key);
	return rc;
}

RC RelationManager::deleteInIndex(const string& tableName, const void* data, const RID& rid) {
    // deal with index file (if exists)
    vector<Attribute> attrs;
    Attribute attr;
    IXFileHandle ixfh;
    RelationManager rm;
    string indexName;
    RC rc = 0;

    void* key = malloc(PAGE_SIZE);
    if (key == NULL) return -12;
    memset(key, 0, PAGE_SIZE);
    rc = getAttributes(tableName, attrs);
    if (rc != 0) return -12;
    int length = 0, nullOffset = 0, dataOffset = attrs.size() / 8 + 1;
    for (int i = 0; i < attrs.size(); i++) {
    	attr = attrs[i];
    	createIndexName(indexName, tableName, attr.name);
    	nullOffset = i / 8;
    	char nullByte;
    	memcpy(&nullByte, (char*)data + nullOffset, 1);
    	if ((nullByte & (1 << (7 - i % 8))) == 0) {
    		if (attr.type == TypeVarChar) {
    			memcpy(&length, (char*)data + dataOffset, sizeof(int));
    			memcpy(key, (char*)data + dataOffset, length + sizeof(int));
    			dataOffset += sizeof(int) + length;
    		} else if (attr.type == TypeInt) {
    			memcpy(key, (char*)data + dataOffset, sizeof(int));
    			dataOffset += sizeof(int);
    		} else if (attr.type == TypeReal) {
    			memcpy(key, (char*)data + dataOffset, sizeof(float));
    			dataOffset += sizeof(float);
    		}

    		if (hasIndex(rm, tableName, attr.name)) {
    			ixm.openFile(indexName, ixfh);
    			if (rc != 0) return -12;
    			ixm.deleteEntry(ixfh, attr, key, rid);
    			if (rc != 0) return -12;
    			ixm.closeFile(ixfh);
    			if (rc != 0) return -12;
    		}
    	}
    }

    free(key);
	return rc;
}



RelationManager* RelationManager::_rm = 0;

RelationManager* RelationManager::instance()
{
	if (!_rm)
		_rm = new RelationManager();
	return _rm;
}

RelationManager::RelationManager()
{

}

RelationManager::~RelationManager()
{

}

RC RelationManager::createCatalog()
{
	RC rc = 0;
	// Create two elementary tables, use fh1 and fh2 to handle Table and Columns respectively.
	rc = rbfm.createFile("Tables");
	if (rc != 0) return -5;
    rc = rbfm.createFile("Columns");
	if (rc != 0) return -5;
    FileHandle fh1;
    FileHandle fh2;
    rc = rbfm.openFile("Tables", fh1);
	if (rc != 0) return -5;
    rc = rbfm.openFile("Columns", fh2);
	if (rc != 0) return -5;


    // Create recordDescriptor to insert elementary tuples into Table and Columns
    vector<Attribute> RD1, RD2;
    createRD4Tables(RD1);
    createRD4Columns(RD2);

    // Create and insert elementary tuples into Tables and Columns tuple by tuple
    RID rid;
    void* data1 = malloc(120);
    void* data2 = malloc(80);
    if (data1 == NULL || data2 == NULL) return -3;
    memset(data1, 0, 120);
    memset(data2, 0, 80);

    createData4Tables(1, "Tables", "Tables", data1);
    rc = insertTupleNoCheck(fh1, "Tables", data1, rid);
	if (rc != 0) return -5;
    createData4Tables(2, "Columns", "Columns", data1);
    rc = insertTupleNoCheck(fh1, "Tables", data1, rid);
    if (rc != 0) return -5;

    // The 0th record in "Columns" has a field of "column-position" indicating the nextTableId
    createData4Columns(0, "nextTableId", TypeInt, 4 , 3, data2);
    rc = insertTupleNoCheck(fh2, "Columns", data2, rid);
    if (rc != 0) return -5;
    createData4Columns(1, "table-id", TypeInt, 4 , 1, data2);
    rc = insertTupleNoCheck(fh2, "Columns", data2, rid);
    if (rc != 0) return -5;
    createData4Columns(1, "table-name", TypeVarChar, 50, 2, data2);
    rc = insertTupleNoCheck(fh2, "Columns", data2, rid);
    if (rc != 0) return -5;
    createData4Columns(1, "file-name", TypeVarChar, 50, 3, data2);
    rc = insertTupleNoCheck(fh2, "Columns", data2, rid);
    if (rc != 0) return -5;
    createData4Columns(2, "table-id", TypeInt, 4, 1, data2);
    rc = insertTupleNoCheck(fh2, "Columns", data2, rid);
    if (rc != 0) return -5;
	createData4Columns(2, "column-name",  TypeVarChar, 50, 2, data2);
    rc = insertTupleNoCheck(fh2, "Columns", data2, rid);
    if (rc != 0) return -5;
	createData4Columns(2, "column-type", TypeInt, 4, 3, data2);
	rc = insertTupleNoCheck(fh2, "Columns", data2, rid);
	createData4Columns(2, "column-length", TypeInt, 4, 4, data2);
	rc = insertTupleNoCheck(fh2, "Columns", data2, rid);
	if (rc != 0) return -5;
	createData4Columns(2, "column-position", TypeInt, 4, 5, data2);
	rc = insertTupleNoCheck(fh2, "Columns", data2, rid);
	if (rc != 0) return -5;

	// Close file
	free(data1);
	free(data2);

	rc = rbfm.closeFile(fh1);
	if (rc != 0) return -5;
	rc = rbfm.closeFile(fh2);
	if (rc != 0) return -5;
	return rc;
}

RC RelationManager::deleteCatalog()
{
	RC rc = 0;

    rc = rbfm.destroyFile("Tables");
    if (rc != 0) return -5;
    rc = rbfm.destroyFile("Columns");
    if (rc != 0) return -5;

	return rc;
}

RC RelationManager::createTable(const string &tableName, const vector<Attribute> &attrs)
{
	RC rc = 0;
	if (!validTable(tableName)) return -4;
	rc = rbfm.createFile(tableName);
	if (rc != 0) return -5;
    FileHandle fh1;
    FileHandle fh2;
    rc = rbfm.openFile("Tables", fh1);
    if (rc != 0) return -5;
    rc = rbfm.openFile("Columns", fh2);
    if (rc != 0) return -5;

    // Get nextTableId
    int tid;
    void* data = malloc(PAGE_SIZE);
    if (data == NULL) return -3;
    memset(data, 0, PAGE_SIZE);
    RID rid; rid.pageNum = rid.slotNum = 0;
    rc = readAttribute("Columns", rid, "column-position", data);
    if (rc != 0) return -5;
    memcpy(&tid, (char*)data + 1, sizeof(int));

    // Insert tuples to Tables and Columns
    void* data1 = malloc(120);
    void* data2 = malloc(80);
    if (data1 == NULL || data2 == NULL) return -3;
    memset(data1, 0, 120);
    memset(data2, 0, 80);
    createData4Tables(tid, tableName, tableName, data1);
    RID rid1, rid2;
    rc = insertTupleNoCheck(fh1, "Tables", data1, rid1);
    if (rc != 0) return -5;
    for (int i = 0; i < attrs.size(); i++) {
    	createData4Columns(tid, attrs[i].name, attrs[i].type, attrs[i].length, i + 1, data2);
    	rc = insertTupleNoCheck(fh2, "Columns", data2, rid2);
    	if (rc != 0) return -5;
    }

    // Update nextTableId
    tid++;
    createData4Columns(0, "nextTableId", TypeInt, 4, tid, data2);
    rc = updateTupleNoCheck(fh2, "Columns", data2, rid);
    if (rc != 0) return -5;

    free(data);
    free(data1);
    free(data2);
    rc = rbfm.closeFile(fh1);
    if (rc != 0) return -5;
    rc = rbfm.closeFile(fh2);
    if (rc != 0) return -5;
	return rc;
}

RC RelationManager::deleteTable(const string &tableName)
{
	RC rc = 0;
	// Check tableName in case of deleting "Tables" or "Columns", tid won't decrease
	if (!validTable(tableName)) return -4;
	rc = rbfm.destroyFile(tableName);
	if (rc != 0) return -5;

	vector<string> RD_id, RD_column;
	RID rid;

	// To get table-id from "Tables"
	RM_ScanIterator rm_ScanIterator;
	createRD_id(RD_id);
	int tid;
	int length = tableName.size();
	void* name = malloc(length + 4);
	if (name == NULL) return -3;
	memset(name, 0, length + 4);
	memcpy(name, &length, sizeof(int));
	int offset = sizeof(int);
	const char *cstr = tableName.c_str();
	for(int i = 0; i < length; i++)
	{
		memcpy((char*)name + offset + i, (char*)cstr + i, 1);
	}

	rc = scan("Tables", "table-name", EQ_OP, name, RD_id, rm_ScanIterator);
	if (rc != 0) return -5;
	setZero(rid);
	void* data = malloc(PAGE_SIZE);
	if (data == NULL) return -3;
	memset(data, 0, PAGE_SIZE);
	rc = rm_ScanIterator.getNextTuple(rid, data);
	if (rc != 0) return -5;
	memcpy(&tid, (char*)data+1, sizeof(int));
	rm_ScanIterator.close();
	free(name);

	// Delete tuple in "Tables"
	FileHandle fh;
	rc = rbfm.openFile("Tables", fh);
	if (rc != 0) return -5;
	rc = deleteTupleNoCheck(fh, "Tables", rid);
	if (rc != 0) return -5;
	rc = rbfm.closeFile(fh);
	if (rc != 0) return -5;

	// Collect rids from "Columns" using tid
	vector<RID> rids;
	createRD_column(RD_column);
	rc = scan("Columns", "table-id", EQ_OP, &tid, RD_column, rm_ScanIterator);
	if (rc != 0) return -5;
	setZero(rid);
	while ((rm_ScanIterator.getNextTuple(rid, data)) != RM_EOF) {
		rids.push_back(rid);
	}
	free(data);
	rm_ScanIterator.close();

	// Delete tuples in "Columns"
	for (int i = 0; i < rids.size(); i++) {
		FileHandle fh;
		rid = rids[i];
		rc = rbfm.openFile("Columns", fh);
		if (rc != 0) return -5;
		rc = deleteTupleNoCheck(fh, "Columns", rid);
		if (rc != 0) return -5;
		rc = rbfm.closeFile(fh);
		if (rc != 0) return -5;
	}


	return rc;
}

RC RelationManager::getAttributes(const string &tableName, vector<Attribute> &attrs)
{
	RC rc = 0;
	vector<string> RD_id, RD_column;
	createRD_id(RD_id);

	// To get Attributes from "Tables" or "Columns", just create the recordDescriptor
	if (tableName == "Tables") {
		createRD4Tables(attrs);
		return 0;
	} else if (tableName == "Columns") {
		createRD4Columns(attrs);
		return 0;
	}

	// To get table-id from "Tables"
	RM_ScanIterator rm_ScanIterator;
	int length = tableName.size();
	void* name = malloc(length + 4);
	if (name == NULL) return -3;
	memset(name, 0, length + 4);
	memcpy(name, &length, sizeof(int));
	int offset = 4;
	const char *cstr = tableName.c_str();
	for(int i = 0; i < length; i++)
	{
		memcpy((char*)name + offset + i, (char*)cstr + i, 1);
	}

	rc = scan("Tables", "table-name", EQ_OP, name, RD_id, rm_ScanIterator);
	if (rc != 0) {
		free(name);
		return -5;
	}
	RID rid;
	setZero(rid);
	void* data = malloc(PAGE_SIZE);
	if (data == NULL) return -3;
	memset(data, 0, PAGE_SIZE);
	rc = rm_ScanIterator.getNextTuple(rid, data);
	if (rc != 0) {
		free(data);
		free(name);
		return -5;
	}
	int tid;
	memcpy(&tid, (char*)data+1, sizeof(int));
	rm_ScanIterator.close();
	free(name);

	// Read actual attrs from "Columns" using tid
	createRD_column(RD_column);
	rc = scan("Columns", "table-id", EQ_OP, &tid, RD_column, rm_ScanIterator);
	if (rc != 0) return -5;
	setZero(rid);
	while ((rm_ScanIterator.getNextTuple(rid, data)) != RM_EOF) {
		insertAttr(data, attrs);
	}

	free(data);
	rm_ScanIterator.close();

    return rc;
}

RC RelationManager::insertTuple(const string &tableName, const void *data, RID &rid)
{
	RC rc = 0;
	FileHandle fh;

	// deal with heap file
	rc = rbfm.openFile(tableName, fh);
	if (rc != 0) return -5;
	if (!validTable(tableName)) return -4;
    rc = insertTupleNoCheck(fh, tableName, data, rid);
    if (rc != 0) return -5;
    rc = rbfm.closeFile(fh);
    if (rc != 0) return -5;
    rc = insertInIndex(tableName, data, rid);
    if (rc != 0) return -12;

    return rc;

}

RC RelationManager::deleteTuple(const string &tableName, const RID &rid)
{
	RC rc = 0;
	FileHandle fh;
	rc = rbfm.openFile(tableName, fh);
	if (rc != 0) return -5;
	if (!validTable(tableName)) return -4;

	// deal with index file (if exists)
	void* data = malloc(PAGE_SIZE);
	if (data == NULL) return -3;
	memset(data, 0, PAGE_SIZE);

	rc = readTuple(tableName, rid, data);
	if (rc != 0) return -5;
	rc = deleteInIndex(tableName, data, rid);
	if (rc != 0) return -12;

	// now can delete tuple
	rc = deleteTupleNoCheck(fh, tableName, rid);
	if (rc != 0) return -5;
    rc = rbfm.closeFile(fh);
    if (rc != 0) return -5;

    free(data);
    return rc;
}

RC RelationManager::updateTuple(const string &tableName, const void *data, const RID& rid)
{
	RC rc = 0;
	FileHandle fh;
	rc = rbfm.openFile(tableName, fh);
	if (rc != 0) return -5;
	if (!validTable(tableName)) return -4;

	// deal with index file (if exists): first delete entry with old key, then insert entry with new key
	void* oldData = malloc(PAGE_SIZE);
	if (oldData == NULL) return -3;
	memset(oldData, 0, PAGE_SIZE);

	rc = readTuple(tableName, rid, oldData);
	if (rc != 0) return -5;
	rc = deleteInIndex(tableName, oldData, rid);
	if (rc != 0) return -12;
	free(oldData);

	// insert entry with new key
	rc = insertInIndex(tableName, data, rid);

	// now can insert tuple
    rc = updateTupleNoCheck(fh, tableName, data, rid);
    if (rc != 0) return -5;
    rc = rbfm.closeFile(fh);
    if (rc != 0) return -5;

    return rc;
}

RC RelationManager::readTuple(const string &tableName, const RID &rid, void *data)
{
	RC rc = 0;
    FileHandle fh;
    rc = rbfm.openFile(tableName, fh);
    if (rc != 0) return -5;

    // Get recordDescriptor RD
    vector<Attribute> RD;
    getAttributes(tableName, RD);
    rc = rbfm.readRecord(fh, RD, rid, data);
    if (rc != 0) {
    	rbfm.closeFile(fh);
    	return -5;
    }
	rc = rbfm.closeFile(fh);
	if (rc != 0) return -5;
	return 0;
}

RC RelationManager::printTuple(const vector<Attribute> &attrs, const void *data)
{
	RC rc = 0;
	rc = rbfm.printRecord(attrs, data);
	if (rc != 0) return -5;
	return rc;
}

RC RelationManager::readAttribute(const string &tableName, const RID &rid, const string &attributeName, void *data)
{
	RC rc = 0;
    FileHandle fh;
    rc = rbfm.openFile(tableName, fh);
    if (rc != 0) return -5;
    vector<Attribute> RD;
    getAttributes(tableName, RD);
	rc = rbfm.readAttribute(fh, RD, rid, attributeName, data);
	if (rc != 0) return -5;
	rc = rbfm.closeFile(fh);
	if (rc != 0) return -5;
	return rc;
}


RC RelationManager::scan(const string &tableName,
      const string &conditionAttribute,
      const CompOp compOp,                  
      const void *value,                    
      const vector<string> &attributeNames,
      RM_ScanIterator &rm_ScanIterator)
{
	RC rc = 0;
    FileHandle fh;
    rc = rbfm.openFile(tableName, fh);
    if (rc != 0) return -5;
    vector<Attribute> RD;
    getAttributes(tableName, RD);
    rc = rbfm.scan(fh, RD, conditionAttribute, compOp, value, attributeNames, rm_ScanIterator.rbfm_ScanIterator);
    if (rc != 0) return -5;
	return rc;
}

RC RM_ScanIterator::getNextTuple(RID &rid, void *data) {
	RC rc = rbfm_ScanIterator.getNextRecord(rid, data);
	return rc;
}


RC RM_ScanIterator::close() {
	RC rc = rbfm_ScanIterator.close();
	return rc;
}

RC RM_IndexScanIterator::getNextEntry(RID &rid, void *key) {
	RC rc = ix_ScanIterator.getNextEntry(rid, key);
	return rc;
}


RC RM_IndexScanIterator::close() {
	RC rc = ix_ScanIterator.close();
	return 0;
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

RC RelationManager::createIndex(const string &tableName, const string &attrName)
{
	RC rc = 0;

	// get indexName
	string indexName;
	createIndexName(indexName, tableName, attrName);

	// create index file
	rc = ixm.createFile(indexName);
	if (rc != 0) return -12;

	// get tid, update "Tables" and "Columns"
    int tid;
    void* data = malloc(PAGE_SIZE);
    if (data == NULL) return -3;
    memset(data, 0, PAGE_SIZE);
    RID rid;
    setZero(rid);
    rc = readAttribute("Columns", rid, "column-position", data);
    if (rc != 0) return -5;
    memcpy(&tid, (char*)data + 1, sizeof(int));

    FileHandle fh1, fh2;
    rc = rbfm.openFile("Tables", fh1);
	if (rc != 0) return -5;
    rc = rbfm.openFile("Columns", fh2);
	if (rc != 0) return -5;

    void* data1 = malloc(120);
    void* data2 = malloc(80);
    if (data1 == NULL || data2 == NULL) return -3;
    memset(data1, 0, 120);
    memset(data2, 0, 80);
    createData4Tables(tid, indexName, indexName, data1);
    RID rid1;
    rc = insertTupleNoCheck(fh1, "Tables", data1, rid1);
    if (rc != 0) return -5;

    tid++;
    createData4Columns(0, "nextTableId", TypeInt, 4, tid, data2);
    rc = updateTupleNoCheck(fh2, "Columns", data2, rid);
    if (rc != 0) return -5;

    free(data);
    free(data1);
    free(data2);

    // scan existing tuples of the table and insert keys into newly created index file
    RM_ScanIterator rm_ScanIterator;
    IXFileHandle ixfh;
    void* key = malloc(PAGE_SIZE);
    if (key == NULL) return -12;
    memset(key, 0, PAGE_SIZE);
    void* entry = malloc(PAGE_SIZE);
    if (entry == NULL) return -12;
    memset(entry, 0, PAGE_SIZE);


    vector<Attribute> attrs;
    Attribute attr;
    RelationManager rm;
    vector<string> attrNames;
    attrNames.push_back(attrName);
    rc = getAttributes(tableName, attrs);
    for (int i = 0; i < attrs.size(); i++) {
    	attr = attrs[i];
    	if (attr.name == attrName)
    		break;
    }
    ixm.openFile(indexName, ixfh);
    rc = rm.scan(tableName, attrName, NO_OP, NULL, attrNames, rm_ScanIterator);
    if (rc != 0) return -12;
    setZero(rid);
    while (rm_ScanIterator.getNextTuple(rid, entry) != RM_EOF) {
    	memcpy(key, (char*)entry + 1, PAGE_SIZE - 1);
    	ixm.insertEntry(ixfh, attr, key, rid);
    }
    rc = rm_ScanIterator.close();
    if (rc != 0) return -12;

    ixm.closeFile(ixfh);
    free(entry);
    free(key);
	return rc;
}

RC RelationManager::destroyIndex(const string &tableName, const string &attributeName)
{
	RC rc = 0;

	// get indexName
	string indexName;
	createIndexName(indexName, tableName, attributeName);

	// destroy index file
	rc = ixm.destroyFile(indexName);
	if (rc != 0) return -12;

	// update Catalogs
	RM_ScanIterator rm_ScanIterator;
	vector<string> RD_id;
	createRD_id(RD_id);
	int length = indexName.size();
	void* name = malloc(length + 4);
	if (name == NULL) return -3;
	memset(name, 0, length + 4);
	memcpy(name, &length, sizeof(int));
	int offset = 4;
	const char *cstr = indexName.c_str();
	for(int i = 0; i < length; i++)
	{
		memcpy((char*)name + offset + i, (char*)cstr + i, 1);
	}

	rc = scan("Tables", "table-name", EQ_OP, name, RD_id, rm_ScanIterator);
	if (rc != 0) return -5;

	RID rid;
	setZero(rid);
	void* data = malloc(PAGE_SIZE);
	if (data == NULL) return -3;
	memset(data, 0, PAGE_SIZE);
	rc = rm_ScanIterator.getNextTuple(rid, data);
	if (rc != 0) return -5;
	rm_ScanIterator.close();

	// Delete tuple in "Tables"
	FileHandle fh;
	rc = rbfm.openFile("Tables", fh);
	if (rc != 0) return -5;
	rc = deleteTupleNoCheck(fh, "Tables", rid);
	if (rc != 0) return -5;
	rc = rbfm.closeFile(fh);
	if (rc != 0) return -5;


	free(name);
	free(data);

	return rc;
}

RC RelationManager::indexScan(const string &tableName,
                      const string &attributeName,
                      const void *lowKey,
                      const void *highKey,
                      bool lowKeyInclusive,
                      bool highKeyInclusive,
                      RM_IndexScanIterator &rm_IndexScanIterator)
{
	RC rc = 0;
	string indexName;
	createIndexName(indexName, tableName, attributeName);

	vector<Attribute> attrs;
	Attribute attr;
	rc = getAttributes(tableName, attrs);
	if (rc != 0) return -12;
	for (int i = 0; i < attrs.size(); i++) {
		attr = attrs[i];
		if (attr.name == attributeName)
			break;
	}

	IXFileHandle ixfh;
	rc = ixm.openFile(indexName, ixfh);
	if (rc != 0) return -12;
	rc = ixm.scan(ixfh, attr, lowKey, highKey, lowKeyInclusive, highKeyInclusive, rm_IndexScanIterator.ix_ScanIterator);
	if (rc != 0) return -12;

	return rc;
}
