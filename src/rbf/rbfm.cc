#include "rbfm.h"

//*****RBFM_ScanIterator*****
RBFM_ScanIterator::RBFM_ScanIterator()
{
	inRid.pageNum = 0;
	inRid.slotNum = -1;
}

RBFM_ScanIterator::~RBFM_ScanIterator()
{
}

bool RBFM_ScanIterator::compareValue(AttrType type, void *space, unsigned startAttri, unsigned endAttri, unsigned offsetTarget)
{
	bool compareResult = false;
	if(endAttri - startAttri == 0)
	{
		return false;
	}
	if(type == TypeVarChar)
	{
		// varchar
		//get condition value
		string condition = "";
		unsigned varCharLength = endAttri - startAttri;
		int valueLength = *((int *)((char *)value));
		if(valueLength == varCharLength)
		{
			bool ifNull = varCharLength == 0 ? true : false;
			if(ifNull)
			{
				// value is null
				compareResult = false;
			}
			else
			{
				for(int i = 0; i < varCharLength; i++)
				{
					condition += *((char *)value + 4 + i);
				}

				// get record value
				string record = "";
				for(int i = 0; i < varCharLength; i++)
				{
					record += *((char *)space + offsetTarget + startAttri + i);
				}

				// compare
				if((compOp == EQ_OP && condition == record) ||
						(compOp == LT_OP && condition > record) ||
						(compOp == LE_OP && condition >= record) ||
						(compOp == GT_OP && condition < record) ||
						(compOp == GE_OP && condition <= record) ||
						(compOp == NE_OP && condition != record) ||
						(compOp == NO_OP))
				{
					compareResult = true;
				}
			}
		}
	}
	else if(type == TypeInt)
	{
		// int
		int condition = *((int *)value);
		int record = *((int *)((char *)space + offsetTarget + startAttri));

		if(endAttri - startAttri == 0)
		{
			// null value
			return false;
		}
		else
		{
			// compare
			if((compOp == EQ_OP && condition == record) ||
				(compOp == LT_OP && condition > record) ||
				(compOp == LE_OP && condition >= record) ||
				(compOp == GT_OP && condition < record) ||
				(compOp == GE_OP && condition <= record) ||
				(compOp == NE_OP && condition != record) ||
				(compOp == NO_OP))
			{
				compareResult = true;
			}
		}
		
	}
	else
	{
		// real
		float condition = *((float *)value);
		float record = *((float *)((char *)space + offsetTarget + startAttri));

		if(endAttri - startAttri == 0)
		{
			return false;
		}
		// compare
		if((compOp == EQ_OP && condition == record) ||
				(compOp == LT_OP && condition > record) ||
				(compOp == LE_OP && condition >= record) ||
				(compOp == GT_OP && condition < record) ||
				(compOp == GE_OP && condition <= record) ||
				(compOp == NE_OP && condition != record) ||
				(compOp == NO_OP))
		{
			compareResult = true;
		}
	}
	return compareResult;
}

RC RBFM_ScanIterator::putPartOfAttrsInputData(void *data, void *space, RID rid)
{
	RC rc = 0;
	// info in original record
	unsigned pointerPosition = PAGE_SIZE - 2 * (rid.slotNum + 2) * USSIZE;
	unsigned offsetTarget = pointerPosition - *((unsigned *)((char *)space + pointerPosition));

	// info in new record
	unsigned attributeNum = attributeNames.size();
	int nullFieldsIndicatorActualSize = ceil((double)attributeNum / CHAR_BIT);
	unsigned char *nullFieldsIndicator = (unsigned char *)malloc(nullFieldsIndicatorActualSize);
	memset(nullFieldsIndicator, 0, nullFieldsIndicatorActualSize);
	unsigned short offsetData = nullFieldsIndicatorActualSize;

	for(int i = 0; i < attributeNum; i++)
	{
		for(int j = 0; j < recordDescriptor.size(); j++)
		{
			if(attributeNames[i] == recordDescriptor[j].name)
			{
				// one attribute found
				unsigned short offsetOldRecordData;
				unsigned attributeInRecordLength;
				if(j == 0)
				{
					offsetOldRecordData = recordDescriptor.size() * sizeof(unsigned short);
					attributeInRecordLength = *((unsigned short*)((char*)space + offsetTarget)) - offsetOldRecordData;
				}
				else
				{
					offsetOldRecordData = *((unsigned short*)((char*)space + offsetTarget + (j - 1) * sizeof(unsigned short)));
					attributeInRecordLength = *((unsigned short*)((char*)space + offsetTarget + j * sizeof(unsigned short))) - offsetOldRecordData;
				}

				// set nullindicator and input data
				if(recordDescriptor[j].type == TypeVarChar)
				{
					// varchar
					if(attributeInRecordLength != 0)
					{
						// not null
						nullFieldsIndicator[i / 8] = nullFieldsIndicator[i / 8] & (0 << (7 - i % 8));
						memcpy((char*)data + offsetData, &attributeInRecordLength, sizeof(int));
						offsetData += sizeof(int);
						memcpy((char*)data + offsetData, (char*)space + offsetTarget + offsetOldRecordData, attributeInRecordLength);
						offsetData += attributeInRecordLength;
					}
					else
					{
						nullFieldsIndicator[i / 8] = nullFieldsIndicator[i / 8] | (1 << (7 - i % 8));
					}
				}
				else
				{
					// int or real
					if(attributeInRecordLength != 0)
					{
						// not null
						nullFieldsIndicator[i / 8] = nullFieldsIndicator[i / 8] & (0 << (7 - i % 8));
						memcpy((char*)data + offsetData, (char*)space + offsetTarget + offsetOldRecordData, 4);
						offsetData += 4;
					}
					else
					{
						// null
						nullFieldsIndicator[i / 8] = nullFieldsIndicator[i / 8] | (1 << (7 - i % 8));
					}
				}
			}
		}
	}
	memcpy((char*)data, nullFieldsIndicator, nullFieldsIndicatorActualSize);
	
	free(nullFieldsIndicator);
	return rc;
}

RC RBFM_ScanIterator::getNextRecord(RID &rid, void *data)
{	
	void *space = malloc(PAGE_SIZE);
	RC rc = 0;

	// ****NO_OP condition****
	if(compOp == NO_OP)
	{
		inRid.slotNum++;
		while(true)
		{
			if(fileHandle.pageNumber <= inRid.pageNum)
			{
				free(space);
				return -1;
			}
			fileHandle.readPage(inRid.pageNum, space);
			unsigned slotNumber = *((unsigned *)((char *)space + PAGE_SIZE - 2 * USSIZE));
			if(slotNumber <= inRid.slotNum)
			{
				inRid.pageNum++;
				inRid.slotNum = 0;
				continue;
			}
			unsigned dataLength = *((unsigned *)((char *)space + PAGE_SIZE - 2 * (inRid.slotNum + 2) * USSIZE + USSIZE));
			if(dataLength == 0 || dataLength == PAGE_SIZE)
			{
				inRid.slotNum++;
				continue;
			}
			else
			{
				rc = putPartOfAttrsInputData(data, space, inRid);
				break;
			}
		}
		rid = inRid;
		free(space);
		return rc;
	}

	// ****not NO_OP****
	// get the position of target attribute
	unsigned conditionNumber = 0;
	for(conditionNumber = 0; conditionNumber < recordDescriptor.size(); conditionNumber++)
	{
		if(recordDescriptor[conditionNumber].name == this->conditionAttribute) break;
	}

	if(conditionNumber >= recordDescriptor.size())
	{
		free(space);
		return -1; // not in this kind of record
	}

	// read data from rid
	// move to the first rid satisfying requirement
	bool findInAllPages = false;
	while(true)
	{
		// search page by page
		bool findInThisPage = false;

		rc = fileHandle.readPage(inRid.pageNum, space);
		if(rc != 0)
		{
			free(space);
			return -1;
		}

		unsigned slotNumber = *((unsigned *)((char *)space + PAGE_SIZE - 2 * USSIZE));

		inRid.slotNum++;
		while(slotNumber > inRid.slotNum)
		{
			// find record in a page, search record by record
			unsigned pointerPosition = PAGE_SIZE - 2 * (inRid.slotNum + 2) * USSIZE;
			unsigned dataLengthRecord = *((unsigned *)((char *)space + pointerPosition + USSIZE));
			if(dataLengthRecord == 0 || dataLengthRecord == PAGE_SIZE)
			{
				inRid.slotNum++;
				continue; // record deleted or it is a tombstone
			}

			unsigned offsetTarget = pointerPosition - *((unsigned *)((char *)space + pointerPosition));
			unsigned startAttri, endAttri; // start and end of the record
			if(conditionNumber == 0)
			{
				startAttri = sizeof(unsigned short) * recordDescriptor.size();
				endAttri = *((unsigned short *)((char *)space + offsetTarget + sizeof(unsigned short) * conditionNumber));
			}
			else
			{
				startAttri = *((unsigned short *)((char *)space + offsetTarget + sizeof(unsigned short) * (conditionNumber - 1)));
				endAttri = *((unsigned short *)((char *)space + offsetTarget + sizeof(unsigned short) * conditionNumber));
			}
			// compare record and value
			findInThisPage = compareValue(this->recordDescriptor[conditionNumber].type, space, startAttri, endAttri, offsetTarget);

			if(findInThisPage) break; // find it
			inRid.slotNum++;
		}
		// internal loop ends

		if(findInThisPage)
		{
			// do find a record in a page
			findInAllPages = true;
			break;
		}
		else
		{
			// prepare for next search(rid)
			inRid.pageNum++;
			inRid.slotNum = -1;
		}
	}
	// external loop ends
	// finish searching in all pages

	// modify the result to satisfy the output requirement
	if(findInAllPages)
	{
		rc = putPartOfAttrsInputData(data, space, inRid); // have found rid, now update the data with rid, including nullindicator
	}
	else
	{
		rc = RBFM_EOF;
	}

	free(space);
	rid = inRid; // return current rid
	return rc;
}

RC RBFM_ScanIterator::close()
{
	fclose(fileHandle.fp);
	return 0;
}

RC RBFM_ScanIterator::getToLastTomb(FileHandle &fileHandle, unsigned &page, unsigned &slot)
{
	void *space = malloc(PAGE_SIZE);
	while(true)
	{
		if(fileHandle.readPage(page, space) != 0)
		{
			return -1;
		}
		unsigned dataLength = *((unsigned *)((char *)space + PAGE_SIZE - 2 * (slot + 2) * USSIZE + USSIZE));
		unsigned offsetTarget = PAGE_SIZE - 2 * (slot + 2) * USSIZE - *((unsigned *)((char *)space + PAGE_SIZE - 2 * (slot + 2) * USSIZE));
		// more tombstone
		if(dataLength == PAGE_SIZE)
		{
			// move to the next
			page = *((unsigned *)((char *)space + offsetTarget));
			slot = *((unsigned *)((char *)space + offsetTarget + USSIZE));
		}
		else
		{
			break; // found
		}
	}

	free(space);
	return 0;
}

//*****RecordBasedFileManager*****
RecordBasedFileManager* RecordBasedFileManager::_rbf_manager = 0;

RecordBasedFileManager* RecordBasedFileManager::instance()
{
    if(!_rbf_manager)
        _rbf_manager = new RecordBasedFileManager();

    return _rbf_manager;
}

RecordBasedFileManager::RecordBasedFileManager()
{
}

RecordBasedFileManager::~RecordBasedFileManager()
{
}

RC RecordBasedFileManager::createFile(const string &fileName) {
	return pfm->createFile(fileName);
}

RC RecordBasedFileManager::destroyFile(const string &fileName) {
	return pfm->destroyFile(fileName);
}

RC RecordBasedFileManager::openFile(const string &fileName, FileHandle &fileHandle) {
	return pfm->openFile(fileName, fileHandle);
}

RC RecordBasedFileManager::closeFile(FileHandle &fileHandle) {
	return pfm->closeFile(fileHandle);
}

RC RecordBasedFileManager::insertRecord(FileHandle &fileHandle, const vector<Attribute> &recordDescriptor, const void *data, RID &rid) {
	RC rc = 0;
	void *space = malloc(PAGE_SIZE);
	void *formatData = malloc(PAGE_SIZE);
	unsigned dataLength = ToFormatData(recordDescriptor, data, formatData); // formatData's dataLength
	unsigned freeSpace = PAGE_SIZE - 2 * USSIZE; // free space in this page

	if(fileHandle.pageNumber == 0)
	{
		// no page at all, so append a new page
		makeEmptyPage(space);

		// append page
		rc = fileHandle.appendPage(space);
		if(rc == 0) rid.pageNum = 0;
	}
	else
	{
		// it has some pages
		// check the last page first
		rc = fileHandle.readPage(fileHandle.pageNumber - 1, space); // get data from last page
		if(rc == 0)
		{
			freeSpace = *((unsigned *)((char *)space + PAGE_SIZE - USSIZE)); // get freeSpae
			if(freeSpace >= dataLength + 2 * USSIZE)
			{
				rid.pageNum = fileHandle.pageNumber - 1;
			}
			else
			{
				// find the first page with enough space except the last one
				for(int i = 0; i < fileHandle.pageNumber - 1; i++)
				{
					fileHandle.readPage(i, space);
					freeSpace = *((unsigned *)((char *)space + PAGE_SIZE - USSIZE)); // get freeSpae
					if(freeSpace >= dataLength + 2 * USSIZE)
					{
						rid.pageNum = i;
						break;
					}
				}

				// fail to find a proper page, so append a new page
				makeEmptyPage(space);
				rc = fileHandle.appendPage(space);
				if(rc == 0) rid.pageNum = fileHandle.pageNumber - 1;
			}
		}
	}
	if(rc == 0)
	{
		rc = actuallyInsertRecord(fileHandle, dataLength, formatData, rid);
	}

	free(space);
	free(formatData);
    return rc;
}

RC RecordBasedFileManager::readRecord(FileHandle &fileHandle, const vector<Attribute> &recordDescriptor, const RID &rid, void *data) {
	RC rc = 0;
	unsigned page = rid.pageNum;
	unsigned slot = rid.slotNum;
	if(getToLastTomb(fileHandle, page, slot) != 0) return -1;
	if(fileHandle.pageNumber < page + 1) return -1; // if enough pages

	void *space = malloc(PAGE_SIZE);
	fileHandle.readPage(page, space);
	unsigned slotNumber = *((unsigned *)((char *)space + PAGE_SIZE - 2 * USSIZE));
	if(slotNumber < slot + 1)
	{
		rc = -1; // not enough slots
	}
	else
	{
		unsigned offsetFromPointer = *((unsigned *)((char *)space + PAGE_SIZE - (slot + 2) * 2 * USSIZE));
		unsigned dataLength = *((unsigned *)((char *)space + PAGE_SIZE - (slot + 2) * 2 * USSIZE + USSIZE)); // format length
		if(dataLength == 0)
		{
			rc = -1;
		}
		else
		{
			void *formatData = malloc(dataLength);
			memcpy(formatData, (char *)space + PAGE_SIZE - (slot + 2) * 2 * USSIZE - offsetFromPointer, dataLength);
			FromFormatData(recordDescriptor, data, formatData);
			free(formatData);
		}
	}

	free(space);
	return rc;
}

RC RecordBasedFileManager::printRecord(const vector<Attribute> &recordDescriptor, const void *data) {
	// get dataLength
	int nullFieldsIndicatorActualSize = ceil((double) recordDescriptor.size() / CHAR_BIT);
	unsigned char *nullFieldsIndicator = (unsigned char *)malloc(nullFieldsIndicatorActualSize);
	memcpy(nullFieldsIndicator, data, nullFieldsIndicatorActualSize);

	int position = nullFieldsIndicatorActualSize; // position in data
	bool nullBit = false; // null indicator

	for(unsigned i = 0; i < recordDescriptor.size(); i++)
	{
		unsigned row = i / 8;
		unsigned col = i - row * 8;
		nullBit = nullFieldsIndicator[row] & (1 << (7 - col));
		Attribute ab = recordDescriptor[i];
		if(!nullBit)
		{
			switch(ab.type)
			{
			case TypeInt:
				int tmpInt;
				memcpy(&tmpInt, (char *)data + position, sizeof(int));
				printf("%s: %d\t", &ab.name[0], tmpInt);
				position += sizeof(int);
				break;
			case TypeReal:
				float tmpFloat;
				memcpy(&tmpFloat, (char *)data + position, sizeof(float));
				printf("%s: %.2f\t", &ab.name[0], tmpFloat);
				position += sizeof(float);
				break;
			case TypeVarChar:
				int length = *((int *)((char *)data + position));
				position += sizeof(int);
				printf("%s: ", &ab.name[0]);
				for(int j = 0; j < length; j++)
				{
					printf("%c", *((char *)data + position));
					position++;
				}
				printf("\t");
			}
		}
		else
		{
			printf("%s: NULL\t", &ab.name[0]);
		}
	}
	printf("\n");

	free(nullFieldsIndicator);

	return 0;
}

RC RecordBasedFileManager::actuallyInsertRecord(FileHandle &fileHandle, const unsigned dataLength, const void *formatData, RID &rid)
{
	void *space = malloc(PAGE_SIZE);
	memset(space, 0, PAGE_SIZE);

	fileHandle.readPage(rid.pageNum, space);
	unsigned slotNumber = *((unsigned *)((char *)space + PAGE_SIZE - 2 * USSIZE)); // get slot number
	unsigned freeSpace = *((unsigned *)((char *)space + PAGE_SIZE - USSIZE)); // get free space

	// first check if there are records deleted
	bool find = false;
	for(int i = 0; i < slotNumber; i++)
	{
		unsigned deleteRecord = *((unsigned *)((char *)space + PAGE_SIZE - USSIZE - 2 * USSIZE * (i + 1)));
		if(deleteRecord == 0)
		{
			rid.slotNum = i;
			find = true;
			break;
		}
	}

	if(!find)
	{
		rid.slotNum = slotNumber; // update slotNum in rid
		slotNumber++;
		memcpy((char *)space + PAGE_SIZE - 2 * USSIZE, &slotNumber, USSIZE); // update slot number
		freeSpace -= (dataLength + 2 * USSIZE);
	}
	else
	{
		freeSpace -= dataLength; // use the old slotNum
	}

	unsigned offsetTarget = PAGE_SIZE - 2 * (slotNumber + 1) * USSIZE - freeSpace - dataLength;
	unsigned offsetFromPointer = freeSpace + dataLength + 2 * (slotNumber - rid.slotNum - 1) * USSIZE; // get offset from pointer
	memcpy((char *)space + PAGE_SIZE - USSIZE, &freeSpace, USSIZE); // update free space
	memcpy((char *)space + offsetTarget, formatData, dataLength); // save data
	memcpy((char *)space + PAGE_SIZE - 1 * USSIZE - (rid.slotNum + 1) * 2 * USSIZE, &dataLength, USSIZE); // save dataLength
	memcpy((char *)space + PAGE_SIZE - 2 * USSIZE - (rid.slotNum + 1) * 2 * USSIZE, &offsetFromPointer, USSIZE); // save offsetFromPointer

	fileHandle.writePage(rid.pageNum, space);
	free(space);
	return 0;
}


unsigned RecordBasedFileManager::ToFormatData(const vector<Attribute> &recordDescriptor, const void *origin, void *formatData)
{
	void *tmp = malloc(PAGE_SIZE);

	// get null indicator
	int nullFieldsIndicatorActualSize = ceil((double) recordDescriptor.size() / CHAR_BIT);
	unsigned char *nullFieldsIndicator = (unsigned char *)malloc(nullFieldsIndicatorActualSize);
	memcpy(nullFieldsIndicator, origin, nullFieldsIndicatorActualSize);

	// in origin
	unsigned originLength = nullFieldsIndicatorActualSize;
	bool nullBit = false;

	// in formatData
	unsigned short offsetDir = 0; // directory
	unsigned short offsetData = sizeof(unsigned short) * recordDescriptor.size();// end of each data

	for(int j = 0; j < recordDescriptor.size(); j++)
	{
		// null indicators
		nullBit = nullFieldsIndicator[j / 8] & (1 << (7 - j % 8));

		if(!nullBit)
		{
			if(recordDescriptor[j].type == TypeVarChar)
			{
				int length = *((int *)((char *)origin + originLength)); // varchar length

				if(length != 0)
				{
					originLength += sizeof(int);
					memcpy((char *)tmp + offsetData, (char *)origin + originLength, length); // convert varchar data
					offsetData += length;

					memcpy((char *)tmp + offsetDir, &offsetData, sizeof(unsigned short)); // convert data's offset
					offsetDir += sizeof(unsigned short);

					originLength += length;
				}
				else
				{
					originLength += sizeof(int);
					char emptyString = '\0';
					memcpy((char *)tmp + offsetData, &emptyString, 1); // convert varchar data (length == 0)
					offsetData += 1;

					memcpy((char *)tmp + offsetDir, &offsetData, sizeof(unsigned short)); // convert data's offset
					offsetDir += sizeof(unsigned short);

					originLength += 0;
				}
			}
			else
			{
				memcpy((char *)tmp + offsetData, (char *)origin + originLength, 4); // convert data
				offsetData += 4;
				memcpy((char *)tmp + offsetDir, &offsetData, sizeof(unsigned short)); // convert offset
				offsetDir += sizeof(unsigned short);

				originLength += 4; // length of int or float
			}
		}
		else
		{
			// null
			memcpy((char *)tmp + offsetDir, &offsetData, sizeof(unsigned short)); // put data's offset into memory, the same with last one
			offsetDir += sizeof(unsigned short);
		}
	}
	memcpy(formatData, tmp, offsetData); // save formatted data

	free(nullFieldsIndicator);
	free(tmp);
	return offsetData >= 9 ? offsetData : 9; // dataLength at least 9
}

unsigned RecordBasedFileManager::FromFormatData(const vector<Attribute> &recordDescriptor, void *origin, const void *formatData)
{
	int nullFieldsIndicatorActualSize = ceil((double) recordDescriptor.size() / CHAR_BIT);

	void *tmp = malloc(PAGE_SIZE);

	unsigned short offsetorigin = nullFieldsIndicatorActualSize; // after null indicator
	unsigned short offsetDir = 0; // start of formatData
	unsigned short offsetData = sizeof(unsigned short) * recordDescriptor.size(); // start of actual data

	unsigned short nextOffsetData = sizeof(unsigned short) * recordDescriptor.size();
	// add the check to null indicator
	int last = 0;

	unsigned char *nullFieldsIndicator = (unsigned char *)malloc(nullFieldsIndicatorActualSize);
	memset(nullFieldsIndicator, 0, nullFieldsIndicatorActualSize);

	for(int j = 0; j < recordDescriptor.size(); j++)
	{
		last = nextOffsetData;
		memcpy(&nextOffsetData, (char*)formatData + offsetDir, sizeof(unsigned short));
		if(nextOffsetData <= last)
		{
			// null
			nullFieldsIndicator[j / 8] = nullFieldsIndicator[j / 8] | (1 << (7 - j % 8));
			offsetDir += sizeof(unsigned short);
		}
		else
		{
			// not null
			int length = 0;
			bool ifZeroExist = false;

			if(recordDescriptor[j].type == TypeVarChar)
			{
				length = nextOffsetData - offsetData;

				if(length == 1)
				{
					char zeroLength = ' ';
					memcpy(&zeroLength, (char *)formatData + offsetData, length); // if the original length is 0

					if(zeroLength == '\0')
					{
						length = 0;
						ifZeroExist = true;
					}
				}

				memcpy((char *)tmp + offsetorigin, &length, sizeof(int)); // save varchar length
				offsetorigin += sizeof(int);
				memcpy((char *)tmp + offsetorigin, (char *)formatData + offsetData, length); // save varchar
			}
			else
			{
				length = 4;
				memcpy((char *)tmp + offsetorigin, (char *)formatData + offsetData, sizeof(int)); // sizeof(int) or sizeof(float), which is 4
			}

			if(ifZeroExist) offsetData += 1;
			else offsetData += length;

			offsetorigin += length;

			offsetDir += sizeof(unsigned short);
		}
	}

	memcpy(origin, tmp, offsetorigin);
	memcpy(origin, nullFieldsIndicator, nullFieldsIndicatorActualSize);

	free(nullFieldsIndicator);
	free(tmp);
	return offsetorigin;
}

RC RecordBasedFileManager::deleteRecord(FileHandle &fileHandle, const vector<Attribute> &recordDescriptor, const RID &rid)
{
	return deleteUntilLastTomb(fileHandle, rid);
}

RC RecordBasedFileManager::deleteUntilLastTomb(FileHandle &fileHandle, RID rid)
{
	RC rc = 0;
	// get information of the page
	void *space = malloc(PAGE_SIZE);
	if(fileHandle.readPage(rid.pageNum, space) != 0)
	{
		free(space);
		return -1;
	}
	unsigned slotNumber = *((unsigned *)((char *)space + PAGE_SIZE - 2 * USSIZE));
	unsigned freeSpace = *((unsigned *)((char *)space + PAGE_SIZE - USSIZE));
	if(slotNumber <= rid.slotNum)
	{
		free(space);
		return -1;
	}
	// get information of the target record
	unsigned offsetPointer = PAGE_SIZE - 2 * (rid.slotNum + 2) * USSIZE;
	unsigned dataLength = *((unsigned *)((char *)space + offsetPointer + USSIZE));
	if(dataLength == 0)
	{
		free(space);
		return -1;
	}
	unsigned offsetFromPointer = *((unsigned *)((char *)space + offsetPointer));
	unsigned offsetTarget = offsetPointer - offsetFromPointer;
	unsigned endOfData = PAGE_SIZE - 2 * (slotNumber + 1) * USSIZE - freeSpace; // end of all records

	// to get the distance of records behind to move, which is the length of target record
	if(dataLength == PAGE_SIZE)
	{
		// it has tombstone
		unsigned minRecordAfterTarget = endOfData; // end of target record
		RID newRid;
		newRid.pageNum = *((unsigned *)((char *)space + offsetTarget));
		newRid.slotNum = *((unsigned *)((char *)space + offsetTarget + USSIZE));
		rc = deleteUntilLastTomb(fileHandle, newRid);
		if(rc == -1)
		{
			free(space);
			return -1;
		}

		// if 4096, find the record after target one, which is the closest
		for(int i = 0; i < slotNumber; i++)
		{
			unsigned tmpOffsetPointer = PAGE_SIZE - 2 * (i + 2) * USSIZE;
			unsigned tmpOffsetFromPointer = *((unsigned *)((char *)space + tmpOffsetPointer));
			unsigned offsetOthers = tmpOffsetPointer - tmpOffsetFromPointer;
			unsigned tmpDataLengthOthers = *((unsigned *)((char *)space + PAGE_SIZE - 2 * (i + 2) * USSIZE + USSIZE));
			if(offsetOthers > offsetTarget && tmpDataLengthOthers > 0 && tmpDataLengthOthers <= PAGE_SIZE)
			{
				minRecordAfterTarget = min(minRecordAfterTarget, offsetOthers);
			}
		}
		// true dataLength of target record
		dataLength = minRecordAfterTarget - offsetTarget;
	}

	// update other slots' offsets
	for(int i = 0; i < slotNumber; i++)
	{
		// make other slots after target one, move pointers ahead
		unsigned tmpOffsetPointer = PAGE_SIZE - 2 * (i + 2) * USSIZE;
		unsigned tmpOffsetFromPointer = *((unsigned *)((char *)space + tmpOffsetPointer));
		unsigned offsetOthers = tmpOffsetPointer - tmpOffsetFromPointer;
		unsigned tmpDataLengthOthers = *((unsigned *)((char *)space + PAGE_SIZE - 2 * (i + 2) * USSIZE + USSIZE));
		if(offsetOthers > offsetPointer - offsetFromPointer && tmpDataLengthOthers > 0)
		{
			unsigned tmpUpdate4records = *((unsigned *)((char *)space + PAGE_SIZE - 2 * (i + 2) * USSIZE)) + dataLength;
			memcpy((char *)space + PAGE_SIZE - 2 * (i + 2) * USSIZE, &tmpUpdate4records, USSIZE);
		}
	}
	// compact
	unsigned lengthAfterTarget = endOfData - offsetTarget - dataLength;
	void *tmpSpace = malloc(lengthAfterTarget);
	memcpy(tmpSpace, (char *)space + offsetTarget + dataLength, lengthAfterTarget);
	memcpy((char *)space + offsetTarget, tmpSpace, lengthAfterTarget);
	free(tmpSpace);

	// set rid's length to 0
	unsigned zeroLength = 0;
	memcpy((char *)space + offsetPointer + USSIZE, &zeroLength, USSIZE);

	// update freeSpace
	freeSpace += dataLength;
	memcpy((char *)space + PAGE_SIZE - USSIZE, &freeSpace, USSIZE);

	fileHandle.writePage(rid.pageNum, space);
	free(space);
	return 0;
}

// Assume the RID does not change after an update
RC RecordBasedFileManager::updateRecord(FileHandle &fileHandle, const vector<Attribute> &recordDescriptor, const void *data, const RID &rid)
{
	void *space = malloc(PAGE_SIZE);
	memset(space, 0, PAGE_SIZE);
	fileHandle.readPage(rid.pageNum, space);

	// *****locate the old record*****
	unsigned slotNumber = *((unsigned *)((char *)space + PAGE_SIZE - 2 * USSIZE));
	unsigned freeSpace = *((unsigned *)((char *)space + PAGE_SIZE - USSIZE));
	if(slotNumber <= rid.slotNum)
	{
		free(space);
		return -1;
	}

	// get info of old record
	unsigned offsetPointer = PAGE_SIZE - 2 * (rid.slotNum + 2) * USSIZE; // offset of old record's pointer
	unsigned dataLength = *((unsigned *)((char *)space + offsetPointer + USSIZE)); // dataLength of old record
	unsigned offsetTargetFromPointer = *((unsigned *)((char *)space + offsetPointer)); // offset from pointer to the old record
	unsigned offsetTarget = offsetPointer - offsetTargetFromPointer; // offset of target from start of the page

	int offsetAfterTarget = offsetTargetFromPointer - 2 * (slotNumber - 1 - rid.slotNum) * USSIZE - freeSpace - dataLength; // total length of records after old record, may be negative
	unsigned lengthAfterTarget = abs(offsetAfterTarget); // total length of records after old records

	// get formatData of new record
	void *formatDataNewRecord = malloc(PAGE_SIZE);
	unsigned dataLengthNewRecord = ToFormatData(recordDescriptor, data, formatDataNewRecord); // formatData's dataLength

	// start updating!
	if(dataLength == 0)
	{
		free(formatDataNewRecord);
		free(space);
		return -1;
	}
	else if(dataLength == PAGE_SIZE)
	{
		// tombstone exists at the old record
		unsigned page = *((unsigned *)((char *)space + offsetTarget)); // page in the tombstone
		unsigned slot = *((unsigned *)((char *)space + offsetTarget + USSIZE)); // slot in the tombstone
		if(getToLastTomb(fileHandle, page, slot) != 0) return -1; // go to the pointed page

		void *spaceLast = malloc(PAGE_SIZE);
		fileHandle.readPage(page, spaceLast);
		RID newRid;
		if(findPlaceToPutRecord(fileHandle, dataLengthNewRecord, page, newRid) != 0)
		{
			free(formatDataNewRecord);
			free(space);
			return -1; // find it
		}
		if(actuallyInsertRecord(fileHandle, dataLengthNewRecord, formatDataNewRecord, newRid) != 0)
		{
			free(formatDataNewRecord);
			free(space);
			return -1; // insert it
		}

		// update last one's tombstone
		unsigned pageSizeDatalength = PAGE_SIZE;
		memcpy((char *)spaceLast + offsetPointer + USSIZE, &pageSizeDatalength, USSIZE); // update dataLength to PAGE_SIZE
		memcpy((char *)spaceLast + offsetTarget, &newRid.pageNum, USSIZE);
		memcpy((char *)spaceLast + offsetTarget + USSIZE, &newRid.slotNum, USSIZE);

		fileHandle.writePage(rid.pageNum, spaceLast);
		free(spaceLast);
	}
	else
	{
		// tombstone not exists
		if(dataLengthNewRecord <= dataLength + freeSpace)
		{
			// space enough for new record, update data, dataLength, freeSpace, no tombstone
			// compact and update offsets of others, that are after the target record

			// compact
			void *tmpSpace = malloc(lengthAfterTarget);
			memcpy(tmpSpace, (char *)space + offsetTarget + dataLength, lengthAfterTarget);
			memcpy((char *)space + offsetTarget + dataLengthNewRecord, tmpSpace, lengthAfterTarget);
			free(tmpSpace);

			memcpy((char *)space + offsetTarget, formatDataNewRecord, dataLengthNewRecord); // save new record
			memcpy((char *)space + offsetPointer + USSIZE, &dataLengthNewRecord, USSIZE); // update dataLength
			// update freeSpace
			freeSpace -= dataLengthNewRecord - dataLength;
			memcpy((char *)space + PAGE_SIZE - USSIZE, &freeSpace, USSIZE);

			// update pointers of others
			for(int i = 0; i < slotNumber; i++)
			{
				unsigned targetPosition = PAGE_SIZE - 2 * USSIZE - 2 * (i + 1) * USSIZE - *((unsigned *)((char *)space + PAGE_SIZE - 2 * USSIZE - 2 * (i + 1) * USSIZE));
				if(targetPosition > offsetTarget)
				{
					unsigned tmpPointer = *((unsigned *)((char *)space + PAGE_SIZE - 2 * (i + 2) * USSIZE));
					tmpPointer += dataLength - dataLengthNewRecord;
					memcpy((char *)space + PAGE_SIZE - 2 * (i + 2) * USSIZE, &tmpPointer, USSIZE);
				}
			}
		}
		else
		{
			// not enough space for new record in original page, so find a new page and use tombstone
			RID newRid;
			if(findPlaceToPutRecord(fileHandle, dataLengthNewRecord, rid.pageNum, newRid) != 0)
			{
				free(formatDataNewRecord);
				free(space);
				return -1;
			}
			if(actuallyInsertRecord(fileHandle, dataLengthNewRecord, formatDataNewRecord, newRid) != 0)
			{
				free(formatDataNewRecord);
				free(space);
				return -1;
			}

			memcpy((char *)space + offsetTarget, &newRid.pageNum, USSIZE); // put new pageNum into tombstone
			memcpy((char *)space + offsetTarget + USSIZE, &newRid.slotNum, USSIZE); // put new slotNum into tombstone
			unsigned pageSizeDatalength = PAGE_SIZE;
			memcpy((char *)space + offsetPointer + USSIZE, &pageSizeDatalength, USSIZE); // put PAGE_SIZE into record's length
		}
	}

	fileHandle.writePage(rid.pageNum, space); // update information of the page of the old record
	free(formatDataNewRecord);
	free(space);
	return 0;
}

RC RecordBasedFileManager::findPlaceToPutRecord(FileHandle &fileHandle, unsigned dataLength, unsigned oldPageNum, RID &rid)
{
	void *space = malloc(PAGE_SIZE);
	bool pageFound = false;

	for(int i = 0; i < fileHandle.pageNumber; i++)
	{
		if(i != oldPageNum) // ignore original page
		{
			fileHandle.readPage(i, space);
			unsigned slotNumber = *((unsigned *)((char *)space + PAGE_SIZE - 2 * USSIZE));
			unsigned freeSpace = *((unsigned *)((char *)space + PAGE_SIZE - USSIZE));
			if(freeSpace >= dataLength + 2 * USSIZE)
			{
				pageFound = true;
				rid.pageNum = i;
				rid.slotNum = slotNumber;
				break;
			}
		}
	}
	if(!pageFound)
	{
		// append a new page
		makeEmptyPage(space);
		fileHandle.appendPage(space);
		rid.pageNum = fileHandle.pageNumber - 1;
		rid.slotNum = 0;
	}

	free(space);
	return 0;
}

RC RecordBasedFileManager::makeEmptyPage(void *space)
{
	unsigned slotNumber = 0;
	unsigned freeSpace = PAGE_SIZE - 2 * USSIZE;
	memcpy((char *)space + PAGE_SIZE - 2 * USSIZE, &slotNumber, USSIZE);
	memcpy((char *)space + PAGE_SIZE - 1 * USSIZE, &freeSpace, USSIZE);
	memset((char *)space, 0, PAGE_SIZE - 2 * USSIZE);

	return 0;
}

RC RecordBasedFileManager::getToLastTomb(FileHandle &fileHandle, unsigned &page, unsigned &slot)
{
	RC rc = 0;
	void *space = malloc(PAGE_SIZE);
	while(true)
	{
		if(fileHandle.readPage(page, space) != 0)
		{
			rc = -1;
			break;
		}
		unsigned dataLength = *((unsigned *)((char *)space + PAGE_SIZE - 2 * (slot + 2) * USSIZE + USSIZE));
		unsigned offsetTarget = PAGE_SIZE - 2 * (slot + 2) * USSIZE - *((unsigned *)((char *)space + PAGE_SIZE - 2 * (slot + 2) * USSIZE));
		// more tombstone
		if(dataLength == PAGE_SIZE)
		{
			// move to the next
			page = *((unsigned *)((char *)space + offsetTarget));
			slot = *((unsigned *)((char *)space + offsetTarget + USSIZE));
		}
		else
		{
			break; // found
		}
	}

	free(space);
	return rc;
}

RC RecordBasedFileManager::readAttribute(FileHandle &fileHandle, const vector<Attribute> &recordDescriptor, const RID &rid, const string &attributeName, void *data)
{
	// get the true rid (tombstone)
	void *space = malloc(PAGE_SIZE);
	unsigned page = rid.pageNum;
	unsigned slot = rid.slotNum;
	if(getToLastTomb(fileHandle, page, slot) != 0)
	{
		free(space);
		return -1;
	}
	if(fileHandle.readPage(page, space) != 0)
	{
		free(space);
		return -1;
	}

	// get slot and record positions
	unsigned slotPosition = PAGE_SIZE - 2 * (slot + 2) * USSIZE;
	unsigned recordPosition = slotPosition - *((unsigned *)((char *)space + slotPosition));

	// *****get the value*****
	unsigned attributePosition = PAGE_SIZE;
	unsigned fieldNumber = recordDescriptor.size();
	bool attributeIsVarChar = false;
	for(int i = 0; i < fieldNumber; i++)
	{
		if(recordDescriptor[i].name == attributeName)
		{
			attributePosition = i;
			if(recordDescriptor[i].type == TypeVarChar)
			{
				attributeIsVarChar = true;
			}
			break;
		}
	}

	if(attributePosition == PAGE_SIZE)
	{
		free(space);
		return -1; // attribute not found
	}

	// get dataLength
	unsigned short dataLength = 0;
	if(attributePosition == 0)
	{
		dataLength = *((unsigned short *)((char *)space + recordPosition)) - fieldNumber * sizeof(unsigned short);
	}
	else
	{
		dataLength = *((unsigned short *)((char *)space + recordPosition + attributePosition * sizeof(unsigned short))) - *((unsigned short *)((char *)space + recordPosition + (attributePosition - 1) * sizeof(unsigned short)));
	}

	int nullFieldsIndicatorActualSize = 1;
	unsigned char *nullFieldsIndicator = (unsigned char *)malloc(nullFieldsIndicatorActualSize);

	if(dataLength == 0)
	{
		// set null value
		nullFieldsIndicator[0] = nullFieldsIndicator[0] | (1 << 7);
		memcpy(data, nullFieldsIndicator, 1);
	}
	else
	{
		unsigned short attributeOffsetFromRecordPosition = (attributePosition == 0) ? sizeof(unsigned short) * fieldNumber : *((unsigned short*)((char *)space + recordPosition + sizeof(unsigned short) * (attributePosition - 1)));
		nullFieldsIndicator[0] = nullFieldsIndicator[0] & (0 << 7);
		memcpy(data, nullFieldsIndicator, 1);
		if(attributeIsVarChar)
		{
			int intDataLength = dataLength;
			memcpy((char *)data + 1, &intDataLength, 4); // put int before string
			memcpy((char *)data + 5, (char *)space + recordPosition + attributeOffsetFromRecordPosition, dataLength); // put varchar attribute
		} else {
			memcpy((char *)data + 1, (char *)space + recordPosition + attributeOffsetFromRecordPosition, dataLength); // put non-varchar attribute
		}
	}

	free(nullFieldsIndicator);
	free(space);
	return 0;
}

RC RecordBasedFileManager::scan(FileHandle &fileHandle,
    const vector<Attribute> &recordDescriptor,
    const string &conditionAttribute,
    const CompOp compOp,                  // comparison type such as "<" and "="
    const void *value,                    // used in the comparison
    const vector<string> &attributeNames, // a list of projected attributes
    RBFM_ScanIterator &rbfm_ScanIterator)
{
	rbfm_ScanIterator.setFH(fileHandle);
	rbfm_ScanIterator.setRecordDescriptor(recordDescriptor);
	rbfm_ScanIterator.setConditionAttribute(conditionAttribute);
	rbfm_ScanIterator.setCompOp(compOp);
	rbfm_ScanIterator.setAttributeNames(attributeNames);
	rbfm_ScanIterator.setValue(value);

	return 0;
}
