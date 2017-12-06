#include "ix.h"

bool IndexManager::isEmpty(IXFileHandle& ixfh) {
	if (ixfh.getNumberOfPages() == 0)
		return true;
	else
		return false;
}

short IndexManager::getFreeSpace(const void* page) {
	short freeSpace;
	memcpy(&freeSpace, (char*) page + PAGE_SIZE - 2 * sizeof(short),
			sizeof(short));
	if (!validFreeSpace(freeSpace))
		return -8;
	else
		return freeSpace;
}

RC IndexManager::setFreeSpace(short freeSpace, void* page) {
	if (!validFreeSpace(freeSpace))
		return -8;
	else
		memcpy((char*) page + PAGE_SIZE - 2 * sizeof(short), &freeSpace,
				sizeof(short));
	return 0;
}

short IndexManager::getNumberOfKeys(const void* page) {
	short numberOfKeys;
	memcpy(&numberOfKeys, (char*) page + PAGE_SIZE - 3 * sizeof(short),
			sizeof(short));
	return numberOfKeys;
}

RC IndexManager::setNumberOfKeys(short numberOfKeys, void* page) {
	RC rc = 0;
	memcpy((char*) page + PAGE_SIZE - 3 * sizeof(short), &numberOfKeys,
			sizeof(short));
	return rc;
}

bool IndexManager::isLeaf(const void* page) {
	short flag = getFlag(page);
	if (flag == 2)
		return true;
	else
		return false;
}

short IndexManager::getKeyLength(short keyNum, const void* page) {
	if (!validKeyNum(keyNum, page))
		return -7;
	else {
		short keyLength;
		memcpy(&keyLength,
				(char*) page + PAGE_SIZE - 4 * sizeof(short)
						- 2 * keyNum * sizeof(short), sizeof(short));
		return keyLength;
	}
}

RC IndexManager::setKeyLength(short keyLength, short keyNum, void* page) {
	if (keyLength == 0)
		cerr << "Invalid KeyLength!" << endl;
	memcpy(
			(char*) page + PAGE_SIZE - 4 * sizeof(short)
					- 2 * keyNum * sizeof(short), &keyLength,
			sizeof(short));
	return 0;
}

short IndexManager::getKeyOffset(short keyNum, const void* page) {
	if (keyNum == 0)
		return sizeof(int);
	short keyOffset;
	if (keyNum == getNumberOfKeys(page))
		return getTotalOffset(page);
	memcpy(&keyOffset,
			(char*) page + PAGE_SIZE - 5 * sizeof(short)
					- 2 * keyNum * sizeof(short), sizeof(short));
	return keyOffset;
}

RC IndexManager::setKeyOffset(short keyOffset, short keyNum, void* page) {
	if (!validKeyOffset(keyOffset))
		return -8;
	else {
		memcpy(
				(char*) page + PAGE_SIZE - 5 * sizeof(short)
						- 2 * keyNum * sizeof(short), &keyOffset,
				sizeof(short));
		return 0;
	}
}

bool IndexManager::validKeyNum(short keyNum, const void* page) {
	short numberOfKeys = getNumberOfKeys(page);
	if (keyNum != 0 && keyNum >= numberOfKeys) {
		cerr << "keyNum exceeds # of keys in page" << endl;
		return false;
	} else
		return true;
}


bool IndexManager::validFreeSpace(short freeSpace) {
	if (freeSpace >= 0 && freeSpace < PAGE_SIZE)
		return true;
	else
		return false;
}

// output the total bytes occupied by entries, including the last pointer if it exists, at least 4 (sibling pointer for leaf and first pointer for internal node)
short IndexManager::getTotalOffset(const void* page) {
	short numberOfKeys = getNumberOfKeys(page), totalOffset = 0, lastKeyLength,
			lastKeyOffset;
	if (numberOfKeys == 0)
		return sizeof(int);
	lastKeyLength = getKeyLength(numberOfKeys - 1, page);
	lastKeyOffset = getKeyOffset(numberOfKeys - 1, page);
	if (isLeaf(page))
		totalOffset = lastKeyOffset + lastKeyLength;
	else
		totalOffset = lastKeyOffset + lastKeyLength + sizeof(int);

	return totalOffset;

}

// set the pointer right to the key in internal nodes
RC IndexManager::setPointer(int pageNum, int keyNum, void* page) {
	RC rc = 0;
	if (isLeaf(page))
		return -9;
	short keyOffset = getKeyOffset(keyNum, page);
	short keyLength = getKeyLength(keyNum, page);
	memcpy((char*) page + keyOffset + keyLength, &pageNum, sizeof(int));

	return rc;
}

// set the sibling pointer in leaf node, or the first pointer in internal nodes
RC IndexManager::setLeftMostPointer(int pageNum, void* page) {
	memcpy((char*) page, &pageNum, sizeof(int));
	return 0;
}

bool IndexManager::validKeyOffset(short keyOffset) {
	if (keyOffset >= 0 && keyOffset < PAGE_SIZE)
		return true;
	else
		return false;
}

short IndexManager::getFlag(const void* page) {
	short flag;
	memcpy(&flag, (char*) page + PAGE_SIZE - sizeof(short), sizeof(short));
	return flag;
}

RC IndexManager::setFlag(short flag, void* page) {
	RC rc = 0;
	memcpy((char*) page + PAGE_SIZE - sizeof(short), &flag, sizeof(short));
	return rc;
}

RC IndexManager::key2Int(int& value, RID& rid, const void* entry) {
	RC rc = 0;
	unsigned pageNum, slotNum;
	memcpy(&value, (char*) entry, sizeof(int));
	memcpy(&pageNum, (char*) entry + sizeof(int), sizeof(unsigned));
	memcpy(&slotNum, (char*) entry + sizeof(int) + sizeof(unsigned),
			sizeof(unsigned));
	rid.pageNum = pageNum;
	rid.slotNum = slotNum;

	return rc;
}

RC IndexManager::key2Real(float& value, RID& rid, const void* entry) {
	RC rc = 0;
	unsigned pageNum, slotNum;
	memcpy(&value, (char*) entry, sizeof(float));
	memcpy(&pageNum, (char*) entry + sizeof(int), sizeof(unsigned));
	memcpy(&slotNum, (char*) entry + sizeof(int) + sizeof(unsigned),
			sizeof(unsigned));
	rid.pageNum = pageNum;
	rid.slotNum = slotNum;

	return rc;
}

RC IndexManager::getLeftPointer(int& pageNum, short slotNum, const void* page) {
	RC rc = 0;
	if (!validKeyNum(slotNum, page))
		return -8;
	short offset = getKeyOffset(slotNum, page);
	offset -= sizeof(int);
	memcpy(&pageNum, (char*) page + offset, sizeof(int));
	return rc;
}

RC IndexManager::getRightPointer(int& pageNum, short slotNum,
		const void* page) {
	RC rc = 0;
	if (isLeaf(page))
		return -9;
	if (!validKeyNum(slotNum, page))
		return -8;
	short offset = getKeyOffset(slotNum, page);
	short length = getKeyLength(slotNum, page);
	memcpy(&pageNum, (char*) page + offset + length, sizeof(int));
	return rc;
}

// to make room for a new entry to be inserted, leave compactOffset room and a directory
short IndexManager::compact(short slotNum, short compactOffset, void* page) {
	short numberOfKeys = getNumberOfKeys(page);
	if (slotNum == numberOfKeys)
		return getTotalOffset(page);
	short freeSpace = getFreeSpace(page);
	if (!validFreeSpace(freeSpace)
			|| freeSpace < compactOffset + 2 * sizeof(short))
		return -8;
	short startOffset = getKeyOffset(slotNum, page);
	short endOffset = getTotalOffset(page);
	void* newPage = malloc(PAGE_SIZE);
	if (newPage == NULL)
		return -3;
	memset(newPage, 0, PAGE_SIZE);

	memcpy((char*) newPage, (char*) page, startOffset);
	memcpy((char*) newPage + startOffset + compactOffset,
			(char*) page + startOffset, endOffset - startOffset);

	// update compacted keys' directory
	short keyOffset;
	for (int i = slotNum; i < numberOfKeys; i++) {
		keyOffset = getKeyOffset(i, page);
		setKeyOffset(keyOffset + compactOffset, i, page);
	}

	// copy proceeding keys' directory, leave room for inserted one's directory;
	short directoryOffset = PAGE_SIZE
			- (numberOfKeys * 2 * sizeof(short) + 3 * sizeof(short));
	short directoryEndOffset = directoryOffset
			+ 2 * sizeof(short) * (numberOfKeys - slotNum);
	memcpy((char*) newPage + directoryOffset - 2 * sizeof(short),
			(char*) page + directoryOffset,
			directoryEndOffset - directoryOffset);
	memcpy((char*) newPage + directoryEndOffset,
			(char*) page + directoryEndOffset, PAGE_SIZE - directoryEndOffset);

	memcpy((char*) page, (char*) newPage, PAGE_SIZE);

	free(newPage);
	return startOffset;

}


// insert composite key to a specified page, freeSpace should be enough, pointer not set!
RC IndexManager::insertCompositeKey(IXFileHandle& ixfh, RID& rid, POP& pop, vector<POP>& pops, void* entry) {
	void* page = malloc(PAGE_SIZE);
	if (page == NULL) return -3;
	memset(page, 0, PAGE_SIZE);
	void* newPage = malloc(PAGE_SIZE);
	if (newPage == NULL) return -3;
	memset(newPage, 0, PAGE_SIZE);

	ixfh.readPage(rid.pageNum, page);
	POP newPop;

	// dealing with root node
	if (rid.pageNum == ixfh.rootNum) {
		// Leaf Root Node
		if (isLeaf(page)) {
			short spaceRequired = pop.entryLength + 2 * sizeof(short);
			short freeSpace = getFreeSpace(page);

			// leaf node and enough space
			if (freeSpace >= spaceRequired) {
				short keyOffset = compact(rid.slotNum, pop.entryLength, page);
				memcpy((char*)page + keyOffset, (char*)(pop.entry), pop.entryLength);
				setNumberOfKeys(getNumberOfKeys(page) + 1, page);
				setFreeSpace(getFreeSpace(page) - spaceRequired, page);
				setKeyOffset(keyOffset, rid.slotNum, page);
				setKeyLength(pop.entryLength, rid.slotNum, page);
				ixfh.writePage(rid.pageNum, page);
				free(newPage);
			}

			// leaf node and not enough space
			else {
				newPop.pid = ixfh.pageNumber;
				short numberOfKeys = getNumberOfKeys(page);
				short shiftNumber = numberOfKeys - rid.slotNum;
				if (shiftNumber < 0)
					cerr << "insert slot exceeds slot limit!" << endl;


				// insert as middle
				if (shiftNumber != 1 && shiftNumber != 0) {
					short entryAfterOffset = getKeyOffset(rid.slotNum, page);
					short entryAfterEndOffset = getTotalOffset(page);
					memcpy(newPage, page, sizeof(int));
					memcpy((char*)newPage + sizeof(int), (char*)page + entryAfterOffset, entryAfterEndOffset - entryAfterOffset);
					short entryAfterDirectoryOffset = PAGE_SIZE - sizeof(short) * (3 + 2 * numberOfKeys);
					short entryAfterDirectoryEndOffset = entryAfterDirectoryOffset + sizeof(short) * 2 * (shiftNumber);
					memcpy((char*)newPage + PAGE_SIZE - sizeof(short) * (3 + 2 * shiftNumber), (char*)page + entryAfterDirectoryOffset, entryAfterDirectoryEndOffset - entryAfterDirectoryOffset);

					setFlag(2, newPage);
					setNumberOfKeys(shiftNumber, newPage);
					setFreeSpace(PAGE_SIZE - sizeof(int) - (entryAfterEndOffset - entryAfterOffset) - sizeof(short) * (3 + 2 * shiftNumber), newPage);

					// modify entries' offset
					setKeyOffset(sizeof(int), 0, newPage);
					for (int i = 1; i < shiftNumber; i++) {
						short shiftKeyOffset = getKeyOffset(i, newPage);
						shiftKeyOffset -= entryAfterOffset - sizeof(int);
						setKeyOffset(shiftKeyOffset, i, newPage);
					}

					ixfh.appendPage(newPage);

					// deal with original page
					memset((char*)page + entryAfterOffset, 0, entryAfterDirectoryEndOffset - entryAfterOffset);
					memcpy((char*)page + entryAfterOffset, pop.entry, pop.entryLength);
					setNumberOfKeys(rid.slotNum + 1, page);
					setLeftMostPointer(ixfh.pageNumber - 1, page);
					setFreeSpace(PAGE_SIZE - entryAfterOffset - pop.entryLength - sizeof(short) * (3 + 2 * (rid.slotNum + 1)), page);
					setKeyLength(pop.entryLength, rid.slotNum, page);
					setKeyOffset(entryAfterOffset, rid.slotNum, page);
					ixfh.writePage(rid.pageNum, page);

					// deal with newPop
					memcpy((char*)entry, (char*)newPage + sizeof(int), getKeyLength(0, newPage));
					newPop.pid = ixfh.pageNumber - 1;
					newPop.entry = entry;
					newPop.entryLength = getKeyLength(0, newPage);

					free(newPage);
				}

				// insert as last, no shift, pop itself(with diff pid but same entryLength)
				else if (shiftNumber == 0){
					memcpy((char*)newPage, (char*)page, sizeof(int));
					memcpy((char*)newPage + sizeof(int), (char*)(pop.entry), pop.entryLength);
					setFlag(2, newPage);
					setNumberOfKeys(1, newPage);
					setFreeSpace(PAGE_SIZE - sizeof(int) - pop.entryLength - sizeof(short) * 5, newPage);
					setKeyLength(pop.entryLength, 0, newPage);
					setKeyOffset(sizeof(int), 0, newPage);

					newPop.pid = ixfh.pageNumber;
					newPop.entryLength = pop.entryLength;
					newPop.entry = entry;
					memcpy((char*)entry, (char*)newPage + sizeof(int), pop.entryLength);
					setLeftMostPointer(ixfh.pageNumber, page);
					setFlag(2, page);

					ixfh.writePage(rid.pageNum, page);
					ixfh.appendPage(newPage);

					free(newPage);
				}

				// insert as last but one, shift itself and last to newPage, pop itself
				else if (shiftNumber == 1) {
					memcpy(newPage, page, sizeof(int));
					memcpy((char*)newPage + sizeof(int), (char*)(pop.entry), pop.entryLength);
					short entryAfterOffset = getKeyOffset(rid.slotNum, page);
					short entryAfterLength = getKeyLength(rid.slotNum, page);
					memcpy((char*)newPage + sizeof(int) + pop.entryLength, (char*)page + entryAfterOffset, entryAfterLength);

					setFlag(2, newPage);
					setFreeSpace(PAGE_SIZE - sizeof(int) - pop.entryLength - entryAfterLength - sizeof(short) * (3 + 2 * 2), newPage);
					setNumberOfKeys(2, newPage);
					setKeyOffset(sizeof(int), 0, newPage);
					setKeyOffset(sizeof(int) + pop.entryLength, 1, newPage);
					setKeyLength(pop.entryLength, 0, newPage);
					setKeyLength(entryAfterLength, 1, newPage);

					ixfh.appendPage(newPage);

					// deal with original page
					setLeftMostPointer(ixfh.pageNumber - 1, page);
					setNumberOfKeys(getNumberOfKeys(page) - 1, page);
					memset((char*)page + entryAfterOffset, 0, entryAfterLength + getFreeSpace(page) + 2 * sizeof(short));
					setFreeSpace(getFreeSpace(page) + entryAfterLength + 2 * sizeof(short), page);
					ixfh.writePage(rid.pageNum, page);

					// deal with newPop
					newPop.pid = ixfh.pageNumber - 1;
					newPop.entry = entry;
					newPop.entryLength = pop.entryLength;
					memcpy((char*)entry, (char*)newPage + sizeof(int), pop.entryLength);

					free(newPage);

				}

				// extra work with newRoot
				void* newRoot = malloc(PAGE_SIZE);
				if (newRoot == NULL) return -3;
				memset(newRoot, 0, PAGE_SIZE);

				setLeftMostPointer(rid.pageNum, newRoot);
				memcpy((char*)newRoot + sizeof(int), (char*)(newPop.entry), newPop.entryLength);

				setFlag(0, newRoot);
				setFreeSpace(PAGE_SIZE - 2 * sizeof(int) - newPop.entryLength - 5 * sizeof(short), newRoot);
				setNumberOfKeys(1, newRoot);
				setKeyOffset(sizeof(int), 0, newRoot);
				setKeyLength(newPop.entryLength, 0, newRoot);
				setPointer(newPop.pid, 0, newRoot);

				ixfh.appendPage(newRoot);

				// deal with ixfh
				ixfh.rootNum = ixfh.pageNumber - 1;
				free(newRoot);
			}
		}
		// Internal root node!
		else {
			short spaceRequired = pop.entryLength + 2 * sizeof(short) + sizeof(int);
			short freeSpace = getFreeSpace(page);

			// internal node and enough space
			if (freeSpace >= spaceRequired) {
				short keyOffset = compact(rid.slotNum, pop.entryLength + sizeof(int), page);
				memcpy((char*)page + keyOffset, (char*)(pop.entry), pop.entryLength);
				int a = pop.pid;
				memcpy((char*)page + keyOffset + pop.entryLength, &a, sizeof(int));
				setNumberOfKeys(getNumberOfKeys(page) + 1, page);
				setFreeSpace(getFreeSpace(page) - spaceRequired, page);
				setKeyOffset(keyOffset, rid.slotNum, page);
				setKeyLength(pop.entryLength, rid.slotNum, page);
				ixfh.writePage(rid.pageNum, page);
				free(newPage);
			}

			// internal node and not enough space
			else {
				newPop.pid = ixfh.pageNumber;
				short numberOfKeys = getNumberOfKeys(page);
				short shiftNumber = numberOfKeys - rid.slotNum;
				if (shiftNumber < 0)
					cerr << "insert slot exceeds slot limit!" << endl;


				// insert as middle
				if (shiftNumber != 1 && shiftNumber != 0) {

					// pop entryAfter, but shift (entryAfter + 1)
					shiftNumber--;

					// pop entryAfter
					short entryAfterLength = getKeyLength(rid.slotNum, page);
					short entryAfterOffset = getKeyOffset(rid.slotNum, page);

					// use tmp to hold the newPoped item
					void* tmp = malloc(PAGE_SIZE);
					if (tmp == NULL) return -3;
					memset(tmp, 0, PAGE_SIZE);
					memcpy((char*)tmp, (char*)page + entryAfterOffset, entryAfterLength);

					// split (entryAfter + 1)
					short entryAfterPlusOffset = getKeyOffset(rid.slotNum + 1, page) - sizeof(int);
					short entryAfterPlusEndOffset = getTotalOffset(page);
					memcpy((char*)newPage, (char*)page + entryAfterPlusOffset, entryAfterPlusEndOffset - entryAfterPlusOffset);
					short entryAfterPlusDirectoryOffset = PAGE_SIZE - sizeof(short) * (3 + 2 * numberOfKeys);
					short entryAfterDirectoryPlusEndOffset = entryAfterPlusDirectoryOffset + sizeof(short) * 2 * (numberOfKeys - rid.slotNum - 1);
					memcpy((char*)newPage + PAGE_SIZE - sizeof(short) * (3 + 2 * shiftNumber), (char*)page + entryAfterPlusDirectoryOffset, entryAfterDirectoryPlusEndOffset - entryAfterPlusDirectoryOffset);

					setFlag(1, newPage);
					setNumberOfKeys(shiftNumber, newPage);
					setFreeSpace(PAGE_SIZE - (entryAfterPlusEndOffset - entryAfterPlusOffset) - sizeof(short) * (3 + 2 * shiftNumber), newPage);

					// modify entries' offset
					setKeyOffset(sizeof(int), 0, newPage);
					for (int i = 1; i < shiftNumber; i++) {
						short shiftKeyOffset = getKeyOffset(i, newPage);
						shiftKeyOffset -= entryAfterPlusOffset;
						setKeyOffset(shiftKeyOffset, i, newPage);
					}

					ixfh.appendPage(newPage);

					// deal with original page
					setFlag(1, page);
					memset((char*)page + entryAfterOffset, 0, entryAfterDirectoryPlusEndOffset - entryAfterOffset);
					memcpy((char*)page + entryAfterOffset, (char*)(pop.entry), pop.entryLength);
					setNumberOfKeys(rid.slotNum + 1, page);
					setFreeSpace(PAGE_SIZE - entryAfterOffset - pop.entryLength - sizeof(int) - sizeof(short) * (3 + 2 * (rid.slotNum + 1)), page);
					setKeyLength(pop.entryLength, rid.slotNum, page);
					setKeyOffset(entryAfterOffset, rid.slotNum, page);
					setPointer(pop.pid, rid.slotNum, page);
					ixfh.writePage(rid.pageNum, page);

					// deal with newPop
					memcpy((char*)entry, (char*)tmp, entryAfterLength);
					newPop.pid = ixfh.pageNumber - 1;
					newPop.entry = entry;
					newPop.entryLength = entryAfterLength;

					free(newPage);
					free(tmp);
				}

				// if insert as last but one, erase and split last, pop itself, set leftPointer to pop.pid, rightPointer to original last pointer
				else if (shiftNumber == 1) {

					// deal with newPage
					setLeftMostPointer(pop.pid, newPage);
					memcpy((char*)newPage + sizeof(int), (char*)page + getKeyOffset(rid.slotNum, page), getKeyLength(rid.slotNum, page) + sizeof(int));
					setFlag(1, newPage);
					setNumberOfKeys(1, newPage);
					setFreeSpace(PAGE_SIZE - 2 * sizeof(int) - getKeyLength(rid.slotNum, page) - 5 * sizeof(short), newPage);
					setKeyOffset(sizeof(int), 0, newPage);
					setKeyLength(getKeyLength(rid.slotNum, page), 0, newPage);
					ixfh.appendPage(newPage);

					// deal with original page
					setFlag(1, page);
					setFreeSpace(getFreeSpace(page) + getKeyLength(rid.slotNum, page) + sizeof(int) + 2 * sizeof(short), page);
					setNumberOfKeys(getNumberOfKeys(page) - 1, page);
					memset((char*)page + getKeyOffset(rid.slotNum, page), 0, getFreeSpace(page));
					ixfh.writePage(rid.pageNum, page);

					// deal with newPop
					newPop.entry = pop.entry;
					newPop.entryLength = pop.entryLength;
					newPop.pid = ixfh.pageNumber - 1;

					free(newPage);
				}

				// if insert as last, erase and pop last, split itself, set its left pointer to last pointer in original page, right pointer to pop.pid
				else if (shiftNumber == 0) {

					// deal with newPage
					memcpy((char*)newPage, (char*)page + getTotalOffset(page) - sizeof(int), sizeof(int));
					memcpy((char*)newPage + sizeof(int), (char*)(pop.entry), pop.entryLength);
					memcpy((char*)newPage + sizeof(int) + pop.entryLength, &(pop.pid), sizeof(int));
					setFlag(1, newPage);
					setNumberOfKeys(1, newPage);
					setFreeSpace(PAGE_SIZE - 2 * sizeof(int) - pop.entryLength - 5 * sizeof(short), newPage);
					setKeyOffset(sizeof(int), 0, newPage);
					setKeyLength(pop.entryLength, 0, newPage);
					ixfh.appendPage(newPage);

					// deal with newPop
					short offset = getKeyOffset(rid.slotNum - 1, page);
					short length = getKeyLength(rid.slotNum - 1, page);
					memcpy((char*)entry, (char*)page + offset, length);
					newPop.entry = entry;
					newPop.entryLength = length;
					newPop.pid = ixfh.pageNumber - 1;

					// deal with original page
					setFlag(1, page);
					setFreeSpace(getFreeSpace(page) + length + sizeof(int) + 2* sizeof(short), page);
					memset((char*)page + offset, 0, getFreeSpace(page));
					setNumberOfKeys(getNumberOfKeys(page) - 1, page);
					ixfh.writePage(rid.pageNum, page);

					free(newPage);
				}

				// extra work with newRoot
				void* newRoot = malloc(PAGE_SIZE);
				if (newRoot == NULL) return -3;
				memset(newRoot, 0, PAGE_SIZE);

				setLeftMostPointer(rid.pageNum, newRoot);
				memcpy((char*)newRoot + sizeof(int), (char*)(newPop.entry), newPop.entryLength);

				setFlag(0, newRoot);
				setFreeSpace(PAGE_SIZE - 2 * sizeof(int) - newPop.entryLength - 5 * sizeof(short), newRoot);
				setNumberOfKeys(1, newRoot);
				setKeyOffset(sizeof(int), 0, newRoot);
				setKeyLength(newPop.entryLength, 0, newRoot);
				setPointer(newPop.pid, 0, newRoot);

				ixfh.appendPage(newRoot);

				// deal with ixfh
				ixfh.rootNum = ixfh.pageNumber - 1;
				free(newRoot);

			}
		}

	}


	// not dealing with root node
	else {
		// Leaf node!
		if (isLeaf(page)) {
			short spaceRequired = pop.entryLength + 2 * sizeof(short);
			short freeSpace = getFreeSpace(page);

			// leaf node and enough space
			if (freeSpace >= spaceRequired) {
				short keyOffset = compact(rid.slotNum, pop.entryLength, page);
				memcpy((char*)page + keyOffset, (char*)(pop.entry), pop.entryLength);
				setNumberOfKeys(getNumberOfKeys(page) + 1, page);
				setFreeSpace(getFreeSpace(page) - spaceRequired, page);
				setKeyOffset(keyOffset, rid.slotNum, page);
				setKeyLength(pop.entryLength, rid.slotNum, page);
				ixfh.writePage(rid.pageNum, page);
				free(newPage);
			}

			// leaf node and not enough space
			else {
				newPop.pid = ixfh.pageNumber;
				short numberOfKeys = getNumberOfKeys(page);
				short shiftNumber = numberOfKeys - rid.slotNum;
				if (shiftNumber < 0)
					cerr << "insert slot exceeds slot limit!" << endl;


				// insert as middle, pop entryAfter, shift entry succeeding
				if (shiftNumber != 1 && shiftNumber != 0) {
					short entryAfterOffset = getKeyOffset(rid.slotNum, page);
					short entryAfterEndOffset = getTotalOffset(page);
					memcpy(newPage, page, sizeof(int));
					memcpy((char*)newPage + sizeof(int), (char*)page + entryAfterOffset, entryAfterEndOffset - entryAfterOffset);
					short entryAfterDirectoryOffset = PAGE_SIZE - sizeof(short) * (3 + 2 * numberOfKeys);
					short entryAfterDirectoryEndOffset = entryAfterDirectoryOffset + sizeof(short) * 2 * shiftNumber;
					memcpy((char*)newPage + PAGE_SIZE - sizeof(short) * (3 + 2 * shiftNumber), (char*)page + entryAfterDirectoryOffset, entryAfterDirectoryEndOffset - entryAfterDirectoryOffset);

					setFlag(2, newPage);
					setNumberOfKeys(shiftNumber, newPage);
					setFreeSpace(PAGE_SIZE - sizeof(int) - (entryAfterEndOffset - entryAfterOffset) - sizeof(short) * (3 + 2 * shiftNumber), newPage);

					// modify entries' offset
					setKeyOffset(sizeof(int), 0, newPage);
					for (int i = 1; i < shiftNumber; i++) {
						short shiftKeyOffset = getKeyOffset(i, newPage);
						shiftKeyOffset -= entryAfterOffset - sizeof(int);
						setKeyOffset(shiftKeyOffset, i, newPage);
					}

					ixfh.appendPage(newPage);

					// deal with original page
					memset((char*)page + entryAfterOffset, 0, entryAfterDirectoryEndOffset - entryAfterOffset);
					memcpy((char*)page + entryAfterOffset, (char*)(pop.entry), pop.entryLength);
					setNumberOfKeys(rid.slotNum + 1, page);
					setLeftMostPointer(ixfh.pageNumber - 1, page);
					setFreeSpace(PAGE_SIZE - entryAfterOffset - pop.entryLength - sizeof(short) * (3 + 2 * (rid.slotNum + 1)), page);
					setKeyLength(pop.entryLength, rid.slotNum, page);
					setKeyOffset(entryAfterOffset, rid.slotNum, page);
					ixfh.writePage(rid.pageNum, page);

					// deal with newPop
					memcpy((char*)entry, (char*)newPage + sizeof(int), getKeyLength(0, newPage));
					newPop.pid = ixfh.pageNumber - 1;
					newPop.entry = entry;
					newPop.entryLength = getKeyLength(0, newPage);
					pops.push_back(newPop);

					free(newPage);
				}

				// insert as last, no shift, pop itself(with diff pid but same entryLength)
				else if (shiftNumber == 0){

					// deal with newPage
					memcpy(newPage, page, sizeof(int));
					memcpy((char*)newPage + sizeof(int), (char*)(pop.entry), pop.entryLength);
					setFlag(2, newPage);
					setNumberOfKeys(1, newPage);
					setFreeSpace(PAGE_SIZE - sizeof(int) - pop.entryLength - sizeof(short) * 5, newPage);
					setKeyLength(pop.entryLength, 0, newPage);
					setKeyOffset(sizeof(int), 0, newPage);

					// deal with newPop
					newPop.pid = ixfh.pageNumber;
					newPop.entryLength = pop.entryLength;
					newPop.entry = entry;
					memcpy((char*)entry, (char*)newPage + sizeof(int), pop.entryLength);
					pops.push_back(newPop);

					// deal with original page
					setLeftMostPointer(ixfh.pageNumber, page);

					ixfh.writePage(rid.pageNum, page);
					ixfh.appendPage(newPage);

					free(newPage);
				}

				// insert as last but one, shift itself and last to newPage, pop itself
				else if (shiftNumber == 1) {

					// deal with newPage
					memcpy(newPage, page, sizeof(int));
					memcpy((char*)newPage + sizeof(int), (char*)(pop.entry), pop.entryLength);
					short entryAfterOffset = getKeyOffset(rid.slotNum, page);
					short entryAfterLength = getKeyLength(rid.slotNum, page);
					memcpy((char*)newPage + sizeof(int) + pop.entryLength, (char*)page + entryAfterOffset, entryAfterLength);

					setFlag(2, newPage);
					setFreeSpace(PAGE_SIZE - sizeof(int) - pop.entryLength - entryAfterLength - sizeof(short) * (3 + 2 * 2), newPage);
					setNumberOfKeys(2, newPage);
					setKeyOffset(sizeof(int), 0, newPage);
					setKeyOffset(sizeof(int) + pop.entryLength, 1, newPage);
					setKeyLength(pop.entryLength, 0, newPage);
					setKeyLength(entryAfterLength, 1, newPage);

					ixfh.appendPage(newPage);

					// deal with original page
					setLeftMostPointer(ixfh.pageNumber - 1, page);
					memset((char*)page + entryAfterOffset, 0, entryAfterLength + getFreeSpace(page) + 2 * sizeof(short));
					setFreeSpace(getFreeSpace(page) + entryAfterLength + 2 * sizeof(short), page);
					setNumberOfKeys(getNumberOfKeys(page) - 1, page);
					ixfh.writePage(rid.pageNum, page);

					// deal with newPop
					newPop.pid = ixfh.pageNumber - 1;
					newPop.entry = entry;
					newPop.entryLength = pop.entryLength;
					memcpy((char*)entry, (char*)newPage + sizeof(int), pop.entryLength);
					pops.push_back(newPop);

					free(newPage);

				}
			}
		}

		// Internal node!
		else {
			short spaceRequired = pop.entryLength + 2 * sizeof(short) + sizeof(int);
			short freeSpace = getFreeSpace(page);

			// internal node and enough space
			if (freeSpace >= spaceRequired) {
				short keyOffset = compact(rid.slotNum, pop.entryLength + sizeof(int), page);
				memcpy((char*)page + keyOffset, (char*)(pop.entry), pop.entryLength);
				memcpy((char*)page + keyOffset + pop.entryLength, &(pop.pid), sizeof(int));
				setNumberOfKeys(getNumberOfKeys(page) + 1, page);
				setFreeSpace(getFreeSpace(page) - spaceRequired, page);
				setKeyOffset(keyOffset, rid.slotNum, page);
				setKeyLength(pop.entryLength, rid.slotNum, page);
				ixfh.writePage(rid.pageNum, page);
				free(newPage);
			}

			// internal node and not enough space
			else {
				newPop.pid = ixfh.pageNumber;
				short numberOfKeys = getNumberOfKeys(page);
				short shiftNumber = numberOfKeys - rid.slotNum;
				if (shiftNumber < 0)
					cerr << "insert slot exceeds slot limit!" << endl;


				// insert as middle, pop entryAfter, split (entryAfter + 1) and succeeding
				if (shiftNumber != 1 && shiftNumber != 0) {

					// pop entryAfter, but shift (entryAfter + 1)
					shiftNumber--;

					// pop entryAfter
					short entryAfterLength = getKeyLength(rid.slotNum, page);
					short entryAfterOffset = getKeyOffset(rid.slotNum, page);
					void* tmp = malloc(PAGE_SIZE);
					if (tmp == NULL) return -3;
					memset(tmp, 0, PAGE_SIZE);
					memcpy((char*)tmp, (char*)page + entryAfterOffset, entryAfterLength);

					// split (entryAfter + 1)
					short entryAfterPlusOffset = getKeyOffset(rid.slotNum + 1, page) - sizeof(int);
					short entryAfterPlusEndOffset = getTotalOffset(page);
					memcpy((char*)newPage, (char*)page + entryAfterPlusOffset, entryAfterPlusEndOffset - entryAfterPlusOffset);
					short entryAfterPlusDirectoryOffset = PAGE_SIZE - sizeof(short) * (3 + 2 * numberOfKeys);
					short entryAfterDirectoryPlusEndOffset = entryAfterPlusDirectoryOffset + sizeof(short) * 2 * shiftNumber;
					memcpy((char*)newPage + PAGE_SIZE - sizeof(short) * (3 + 2 * shiftNumber), (char*)page + entryAfterPlusDirectoryOffset, entryAfterDirectoryPlusEndOffset - entryAfterPlusDirectoryOffset);

					setFlag(1, newPage);
					setNumberOfKeys(shiftNumber, newPage);
					setFreeSpace(PAGE_SIZE - (entryAfterPlusEndOffset - entryAfterPlusOffset) - sizeof(short) * (3 + 2 * shiftNumber), newPage);

					// modify entries' offset
					setKeyOffset(sizeof(int), 0, newPage);
					for (int i = 1; i < shiftNumber; i++) {
						short shiftKeyOffset = getKeyOffset(i, newPage);
						shiftKeyOffset -= entryAfterPlusOffset;
						setKeyOffset(shiftKeyOffset, i, newPage);
					}

					ixfh.appendPage(newPage);

					// deal with original page
					memset((char*)page + entryAfterOffset, 0, entryAfterDirectoryPlusEndOffset - entryAfterOffset);
					memcpy((char*)page + entryAfterOffset, (char*)(pop.entry), pop.entryLength);
					setNumberOfKeys(rid.slotNum + 1, page);
					setFreeSpace(PAGE_SIZE - entryAfterOffset - pop.entryLength - sizeof(int) - sizeof(short) * (3 + 2 * (rid.slotNum + 1)), page);
					setKeyLength(pop.entryLength, rid.slotNum, page);
					setKeyOffset(entryAfterOffset, rid.slotNum, page);
					setPointer(pop.pid, rid.slotNum, page);
					ixfh.writePage(rid.pageNum, page);

					// deal with newPop
					memcpy((char*)entry, (char*)tmp, entryAfterLength);
					newPop.pid = ixfh.pageNumber - 1;
					newPop.entry = entry;
					newPop.entryLength = entryAfterLength;
					pops.push_back(newPop);

					free(tmp);
					free(newPage);
				}

				// if insert as last but one, erase and split last, pop itself, set leftPointer to pop.pid, rightPointer to original last pointer
				else if (shiftNumber == 1) {

					// deal with newPage
					setLeftMostPointer(pop.pid, newPage);
					memcpy((char*)newPage + sizeof(int), (char*)page + getKeyOffset(rid.slotNum, page), getKeyLength(rid.slotNum, page) + sizeof(int));
					setFlag(1, newPage);
					setNumberOfKeys(1, newPage);
					setFreeSpace(PAGE_SIZE - 2 * sizeof(int) - getKeyLength(rid.slotNum, page) - 5 * sizeof(short), newPage);
					setKeyOffset(sizeof(int), 0, newPage);
					setKeyLength(getKeyLength(rid.slotNum, page), 0, newPage);
					ixfh.appendPage(newPage);

					// deal with original page
					setFlag(1, page);
					setFreeSpace(getFreeSpace(page) + getKeyLength(rid.slotNum, page) + sizeof(int) + 2 * sizeof(short), page);
					setNumberOfKeys(getNumberOfKeys(page) - 1, page);
					memset((char*)page + getKeyOffset(rid.slotNum, page), 0, getFreeSpace(page));
					ixfh.writePage(rid.pageNum, page);

					// deal with newPop
					newPop.pid = ixfh.pageNumber - 1;
					newPop.entry = pop.entry;
					newPop.entryLength = pop.entryLength;
					pops.push_back(newPop);

					free(newPage);
				}

				// if insert as last, erase and pop last, split itself, set its left pointer to last pointer in original page, right pointer to pop.pid
				else if (shiftNumber == 0) {
					// deal with newPage
					memcpy((char*)newPage, (char*)page + getTotalOffset(page) - sizeof(int), sizeof(int));
					memcpy((char*)newPage + sizeof(int), (char*)(pop.entry), pop.entryLength);
					memcpy((char*)newPage + sizeof(int) + pop.entryLength, &(pop.pid), sizeof(int));
					setFlag(1, newPage);
					setNumberOfKeys(1, newPage);
					setFreeSpace(PAGE_SIZE - 2 * sizeof(int) - pop.entryLength - 5 * sizeof(short), newPage);
					setKeyOffset(sizeof(int), 0, newPage);
					setKeyLength(pop.entryLength, 0, newPage);
					ixfh.appendPage(newPage);

					// deal with newPop
					short offset = getKeyOffset(rid.slotNum - 1, page);
					short length = getKeyLength(rid.slotNum - 1, page);
					memcpy((char*)entry, (char*)page + offset, length);
					newPop.pid = ixfh.pageNumber - 1;
					newPop.entry = entry;
					newPop.entryLength = length;
					pops.push_back(newPop);

					// deal with original page
					setFlag(1, page);
					setFreeSpace(getFreeSpace(page) + length + sizeof(int) + 2* sizeof(short), page);
					memset((char*)page + offset, 0, getFreeSpace(page));
					setNumberOfKeys(getNumberOfKeys(page) - 1, page);
					ixfh.writePage(rid.pageNum, page);

					free(newPage);
				}

			}
		}

	}
	free(page);

	return 0;
}

// search an entry in pageNum, push the rid it belongs to to rids
RC IndexManager::searchEntry(void* page, vector<RID>& rids, IXFileHandle& ixfh, int& pageNum,
		const void* newEntry, short& newEntryLength) {
	RC rc = 0;
	if (isEmpty(ixfh))
		return -9;

	RID entryRid;
	entryRid.pageNum = pageNum;
	entryRid.slotNum = 0;
	bool find = false;
	short slotNumber = getNumberOfKeys(page);
	if (slotNumber == 0) {
		entryRid.slotNum = 0;
		rids.push_back(entryRid);
		return 0;
	} else {
		AttrType type = ixfh.type;
		int i = 0;
		void* entry = malloc(PAGE_SIZE);
		if (entry == NULL)
			return -3;
		memset(entry, 0, PAGE_SIZE);

		for (i = 0; i < slotNumber; i++) {
			short entryLength = getKeyLength(i, page);
			short entryOffset = getKeyOffset(i, page);
			memcpy((char*)entry, (char*)page + entryOffset, entryLength);

			if (compareEntry(type, entry, newEntry, entryLength,
					newEntryLength)) {
				find = true;
				break;
			}
		}

		if (isLeaf(page)) {
			entryRid.slotNum = i;
			rids.push_back(entryRid);
			free(entry);
			return rc;
		} else {
			int newPageNum;
			if (find) {
				rc = getLeftPointer(newPageNum, i, page);
				if (rc != 0)
					return -6;
			} else {
				rc = getRightPointer(newPageNum, i - 1, page);
				if (rc != 0)
					return -6;
			}
			entryRid.slotNum = i;
			rids.push_back(entryRid);
			entryRid.pageNum = newPageNum;
			pageNum = newPageNum;
		}
		free(entry);
	}

	return rc;

}


RC IndexManager::searchExactEntry(IXFileHandle ixfh, int pageNum,
		const void* entry, RID& rid, short& entryLength) {
	RC rc;
	void* page = malloc(PAGE_SIZE);
	if (page == NULL)
		return -3;
	memset(page, 0, PAGE_SIZE);
	rc = ixfh.readPage(pageNum, page);
	if (rc != 0)
		return -6;

	vector<RID> rids;
	while (!isLeaf(page)) {
		searchEntry(page, rids, ixfh, (int&)rid.pageNum, entry, entryLength);
		rc = ixfh.readPage(rid.pageNum, page);
		if (rc != 0)
			return -6;
	}

	void* key = malloc(PAGE_SIZE);
	if (key == NULL)
		return -3;
	memset(key, 0, PAGE_SIZE);
	short slotNumber = getNumberOfKeys(page);
	short keyLength, keyOffset;
	for (int i = 0; i < slotNumber; i++) {
		keyLength = getKeyLength(i, page);
		keyOffset = getKeyOffset(i, page);
		memcpy((char*)key, (char*)page + keyOffset, keyLength);

		if (compareExactEntry(ixfh.type, entry, key, entryLength, keyLength)) {
			rid.slotNum = i;
			free(page);
			free(key);
			return 0;
		}
	}
	free(page);
	free(key);
	return -1;
}

// compare 2 entries according to both key and rid, true if the former is greater
bool IndexManager::compareEntry(AttrType& type, const void* entry1,
		const void* entry2, short& length1, short& length2) {
	if (type == TypeVarChar) {
		string str1 = "", str2 = "";
		RID rid1, rid2;
		char ch;
		for (int i = 0; i < length1 - 2 * sizeof(unsigned); i++) {
			memcpy(&ch, (char*) entry1 + i, sizeof(char));
			str1 += ch;
		}
		for (int i = 0; i < length2 - 2 * sizeof(unsigned); i++) {
			memcpy(&ch, (char*) entry2 + i, sizeof(char));
			str2 += ch;
		}

		unsigned pageNum, slotNum;
		memcpy(&pageNum, (char*) entry1 + length1 - 2 * sizeof(unsigned),
				sizeof(unsigned));
		memcpy(&slotNum, (char*) entry1 + length1 - 1 * sizeof(unsigned),
				sizeof(unsigned));
		rid1.pageNum = pageNum;
		rid1.slotNum = slotNum;

		memcpy(&pageNum, (char*) entry2 + length2 - 2 * sizeof(unsigned),
				sizeof(unsigned));
		memcpy(&slotNum, (char*) entry2 + length2 - 1 * sizeof(unsigned),
				sizeof(unsigned));
		rid2.pageNum = pageNum;
		rid2.slotNum = slotNum;

		// comparison between 2 strings and 2 rids
		if (str1 > str2)
			return true;
		else if (str1 == str2 && compareRid(rid1, rid2))
			return true;
		else
			return false;

	} else if (type == TypeInt) {
		int value1, value2;
		RID rid1, rid2;
		key2Int(value1, rid1, entry1);
		key2Int(value2, rid2, entry2);

		// comparison between 2 values and 2 rids
		if (value1 > value2)
			return true;
		else if (value1 == value2 && compareRid(rid1, rid2))
			return true;
		else
			return false;

	} else if (type == TypeReal) {
		float value1, value2;
		RID rid1, rid2;
		key2Real(value1, rid1, entry1);
		key2Real(value2, rid2, entry2);

		// comparison between 2 values and 2 rids
		if (value1 > value2)
			return true;
		else if (value1 == value2 && compareRid(rid1, rid2))
			return true;
		else
			return false;
	}
}

// compare 2 rids, true if the former is greater
bool IndexManager::compareRid(const RID& rid1, const RID& rid2) {
	if (rid1.pageNum > rid2.pageNum)
		return true;
	else if (rid1.pageNum == rid2.pageNum && rid1.slotNum > rid2.slotNum)
		return true;
	else
		return false;
}

bool IndexManager::compareExactEntry(AttrType& type, const void* entry1,
		const void* entry2, short& length1, short& length2) {
	if (type == TypeVarChar) {
		string str1 = "", str2 = "";
		RID rid1, rid2;
		char ch;
		for (int i = 0; i < length1 - 2 * sizeof(unsigned); i++) {
			memcpy(&ch, (char*) entry1 + i, sizeof(char));
			str1 += ch;
		}
		for (int i = 0; i < length2 - 2 * sizeof(unsigned); i++) {
			memcpy(&ch, (char*) entry2 + i, sizeof(char));
			str2 += ch;
		}

		unsigned pageNum, slotNum;
		memcpy(&pageNum, (char*) entry1 + length1 - 2 * sizeof(unsigned),
				sizeof(unsigned));
		memcpy(&slotNum, (char*) entry1 + length1 - 1 * sizeof(unsigned),
				sizeof(unsigned));
		rid1.pageNum = pageNum;
		rid1.slotNum = slotNum;

		memcpy(&pageNum, (char*) entry2 + length2 - 2 * sizeof(unsigned),
				sizeof(unsigned));
		memcpy(&slotNum, (char*) entry2 + length2 - 1 * sizeof(unsigned),
				sizeof(unsigned));
		rid2.pageNum = pageNum;
		rid2.slotNum = slotNum;

		// comparison between 2 strings and 2 rids
		if (str1 == str2 && compareExactRid(rid1, rid2))
			return true;
		else
			return false;

	} else if (type == TypeInt) {
		int value1, value2;
		RID rid1, rid2;
		key2Int(value1, rid1, entry1);
		key2Int(value2, rid2, entry2);

		// comparison between 2 values and 2 rids
		if (value1 == value2 && compareExactRid(rid1, rid2))
			return true;
		else
			return false;

	} else if (type == TypeReal) {
		float value1, value2;
		RID rid1, rid2;
		key2Real(value1, rid1, entry1);
		key2Real(value2, rid2, entry2);

		// comparison between 2 values and 2 rids
		if (value1 == value2 && compareExactRid(rid1, rid2))
			return true;
		else
			return false;
	}
}

bool IndexManager::compareExactRid(RID& rid1, RID& rid2) {
	if (rid1.pageNum == rid2.pageNum && rid1.slotNum == rid2.slotNum)
		return true;
	else
		return false;
}

short IndexManager::buildEntry(AttrType type, const void* key, const RID& rid, void* entry) {
	int keyLength;

	if (type == TypeVarChar) {
		// build entry, str is the actual key
		int stringLength;
		memcpy(&stringLength, (char*) key, sizeof(int));
		keyLength = stringLength + 2 * sizeof(unsigned);
		unsigned pageNum = rid.pageNum, slotNum = rid.slotNum;
		char ch;
		string str = "";
		for (int i = 0; i < stringLength; i++) {
			memcpy(&ch, (char*) key + i, sizeof(char));
			str += ch;
		}
		memcpy((char*) entry, (char*) key + sizeof(int), stringLength);
		memcpy((char*) entry + stringLength, &pageNum, sizeof(unsigned));
		memcpy((char*) entry + stringLength + sizeof(unsigned), &slotNum,
				sizeof(unsigned));

	} else if (type == TypeInt) {

		// bulid entry, value is the actual key
		int value;
		keyLength = sizeof(int) + 2 * sizeof(unsigned);
		unsigned pageNum = rid.pageNum, slotNum = rid.slotNum;
		memcpy(&value, key, sizeof(int));
		memcpy((char*) entry, &value, sizeof(int));
		memcpy((char*) entry + sizeof(int), &pageNum, sizeof(unsigned));
		memcpy((char*) entry + sizeof(int) + sizeof(unsigned), &slotNum,
				sizeof(unsigned));
	}

	else {

		// build entry, value is the actual key
		float value;
		keyLength = sizeof(float) + 2 * sizeof(unsigned);
		unsigned pageNum = rid.pageNum, slotNum = rid.slotNum;
		memcpy(&value, key, sizeof(float));
		memcpy((char*) entry, &value, sizeof(float));
		memcpy((char*) entry + sizeof(float), &pageNum, sizeof(unsigned));
		memcpy((char*) entry + sizeof(float) + sizeof(unsigned), &slotNum,
				sizeof(unsigned));
	}

	return keyLength;
}


IndexManager* IndexManager::_index_manager = 0;

IndexManager* IndexManager::instance() {
	if (!_index_manager)
		_index_manager = new IndexManager();

	return _index_manager;
}

IndexManager::IndexManager() {
}

IndexManager::~IndexManager() {
}

RC IndexManager::createFile(const string &fileName) {
	RC rc = 0;
	rc = ixpfm->createIXFile(fileName);
	return rc;
}

RC IndexManager::destroyFile(const string &fileName) {
	RC rc = 0;
	rc = ixpfm->destroyIXFile(fileName);
	return rc;
}

RC IndexManager::openFile(const string &fileName, IXFileHandle &ixfileHandle) {
	RC rc = 0;
	rc = ixpfm->openIXFile(fileName, ixfileHandle);
	return rc;
}

RC IndexManager::closeFile(IXFileHandle &ixfileHandle) {
	RC rc = 0;
	rc = ixpfm->closeIXFile(ixfileHandle);
	return rc;
}

RC IndexManager::insertEntry(IXFileHandle &ixfileHandle, const Attribute &attribute, const void *key, const RID &rid) {
	if (isEmpty(ixfileHandle)) {
		// ixfh work
		ixfileHandle.type = attribute.type;
		ixfileHandle.rootNum = 0;

		// root page work
		void* root = malloc(PAGE_SIZE);
		if (root == NULL)
			return -3;
		memset(root, 0, PAGE_SIZE);
		void* tmp = malloc(PAGE_SIZE);
		if (tmp == NULL)
			return -3;
		memset(tmp, 0, PAGE_SIZE);

		short keyLength = buildEntry(attribute.type, key, rid, tmp);
		setLeftMostPointer(-1, root);
		memcpy((char*)root + sizeof(int), (char*)tmp, keyLength);
		setFlag(2, root);
		setNumberOfKeys(1, root);
		setFreeSpace(PAGE_SIZE - sizeof(int) - keyLength - 5 * sizeof(short), root);
		setKeyOffset(sizeof(int), 0, root);
		setKeyLength(keyLength, 0, root);

		ixfileHandle.appendPage(root);
		free(tmp);
		free(root);
	}

	// B+ Tree is not empty
	else {
		void* page = malloc(PAGE_SIZE);
		if (page == NULL)
			return -3;
		memset(page, 0, PAGE_SIZE);
		void* entry = malloc(PAGE_SIZE);
		if (entry == NULL)
			return -3;
		memset(entry, 0, PAGE_SIZE);

		short entryLength = buildEntry(ixfileHandle.type, key, rid, entry);
		int pid = ixfileHandle.rootNum;
		vector<RID> rids;
		vector<POP> pops;
		do {
			ixfileHandle.readPage(pid, page);
			searchEntry(page, rids, ixfileHandle, pid, entry,
					entryLength);
		} while (!isLeaf(page));

		POP pop;
		pop.pid = rids.back().pageNum;
		pop.entryLength = entryLength;
		pop.entry = entry;
		pops.push_back(pop);

		RID Rid;
		while (!pops.empty()) {
			Rid = rids.back(); rids.pop_back();
			pop = pops.back(); pops.pop_back();

			insertCompositeKey(ixfileHandle, Rid, pop, pops, entry);
		}

		free(page);
		free(entry);
	}
	return 0;
}

RC IndexManager::deleteEntry(IXFileHandle &ixfileHandle, const Attribute &attribute, const void *key, const RID &rid) {
	RC rc;
	void* entry = malloc(PAGE_SIZE);
	if (entry == NULL)
		return -3;
	memset(entry, 0, PAGE_SIZE);
	short entryLength = buildEntry(attribute.type, key, rid, entry);
	void* page = malloc(PAGE_SIZE);
	if (page == NULL)
		return -3;
	memset(page, 0, PAGE_SIZE);

	RID entryRid;
	entryRid.pageNum = ixfileHandle.rootNum;
	entryRid.slotNum = 0;
	rc = searchExactEntry(ixfileHandle, entryRid.pageNum, entry, entryRid, entryLength);
	if (rc != 0) {
		free(page);
		free(entry);
		return rc;

	}
	free(page);
	free(entry);

	// delete the entry
	rc = trueDelete(ixfileHandle, attribute, key, entryRid, rid);

	return rc;
}

int IndexManager::trueDelete(IXFileHandle &ixfileHandle, const Attribute &attribute, const void *key, RID &entryRID, const RID &rid) {
	RC rc = 0;
	void *space = malloc(PAGE_SIZE);
	rc = ixfileHandle.readPage(entryRID.pageNum, space);
	if(rc != 0) {
		free(space);
		return rc;
	}
	short freeSpace = *((short*) ((char*) space + PAGE_SIZE - 2 * sizeof(short)));
	short keyNumber = *((short*) ((char*) space + PAGE_SIZE - 3 * sizeof(short)));

	// compact keys
	short totalKeysCompact = *((short*) ((char*) space + PAGE_SIZE - (2 + 2 * (entryRID.slotNum + 1)) * sizeof(short)));
	short compactKeyStartPosition = *((short*) ((char*) space + PAGE_SIZE - (3 + 2 * (entryRID.slotNum + 1)) * sizeof(short)));
	short compactKeyRemainLength = 0;
	for (unsigned i = entryRID.slotNum + 1; i < keyNumber; i++) {
		short currentLength = *((short*) ((char*)space + PAGE_SIZE - (2 + 2 * (i + 1)) * sizeof(short)));
		compactKeyRemainLength += currentLength;
		// update offset
		short tempKeyOffset = *((short*)((char*)space + PAGE_SIZE - sizeof(short) * (3 + 2 * (i + 1))));
		tempKeyOffset -= totalKeysCompact;
		memcpy((char*)space + PAGE_SIZE - sizeof(short) * (3 + 2 * (i + 1)), &tempKeyOffset, sizeof(short));
	}

	void *compactKeySpace = malloc(compactKeyRemainLength);
	memcpy(compactKeySpace, (char*) space + compactKeyStartPosition + totalKeysCompact, compactKeyRemainLength);
	memcpy((char*) space + compactKeyStartPosition, compactKeySpace, compactKeyRemainLength);
	free(compactKeySpace);


	// compact directory
	short totalDirectoryCompact = 2 * sizeof(short);
	short compactStartPosition = PAGE_SIZE - (2 * keyNumber + 3) * sizeof(short);
	short compactRemainLength = (keyNumber - entryRID.slotNum - 1) * 2 * sizeof(short);

	void *compactDirectorySpace = malloc(compactRemainLength);
	memcpy(compactDirectorySpace, (char*) space + compactStartPosition, compactRemainLength);
	memcpy((char*) space + compactStartPosition + totalDirectoryCompact, compactDirectorySpace, compactRemainLength);
	free(compactDirectorySpace);

	// update keyNumber and freeSpace
	keyNumber -= 1;
	memcpy((char*) space + PAGE_SIZE - 3 * sizeof(short), &keyNumber, sizeof(short));
	freeSpace -= (totalKeysCompact + totalDirectoryCompact);
	memcpy((char*) space + PAGE_SIZE - 2 * sizeof(short), &freeSpace, sizeof(short));

	ixfileHandle.writePage(entryRID.pageNum, space);
	free(space);
	return 0;
}

RC IndexManager::scan(IXFileHandle &ixfileHandle, const Attribute &attribute,
		const void *lowKey, const void *highKey, bool lowKeyInclusive,
		bool highKeyInclusive, IX_ScanIterator &ix_ScanIterator) {

	if (ixfileHandle.fileName == "") {
		return -1;
	}

	ix_ScanIterator.prepare(ixfileHandle,attribute,lowKey,highKey,lowKeyInclusive, highKeyInclusive);

	return 0;
}

void IndexManager::printBtree(IXFileHandle &ixfileHandle, const Attribute &attribute) const {
	// first traverse then print
	printBtreeHelper(ixfileHandle.rootNum, 0, ixfileHandle, attribute);
}

string IndexManager::Other2string(int type, unsigned unsignedValue, int intValue, float floatValue) const {
	// type: 0 for unsigned, 1 for int, 2 for float
	stringstream ss;
	ss.str("");
	if(type == 0) {
		ss << unsignedValue;
	} else if(type == 1) {
		ss << intValue;
	} else {
		ss << floatValue;
	}
	return ss.str();
}

int IndexManager::printBtreeHelper(unsigned currentNum, unsigned level, IXFileHandle &ixfileHandle, const Attribute &attribute) const {
	RC rc = 0;
	void *space = malloc(PAGE_SIZE);
	rc = ixfileHandle.readPage(currentNum, space);
	if(rc != 0) {
		free(space);
		return -1;
	}

	short leafFlag = *((short*) ((char*) space + PAGE_SIZE - sizeof(short))); // check node type: 0 for root, 1 for internal, 2 for leaf
	short keyNumber = *((short*) ((char*) space + PAGE_SIZE - 3 * sizeof(short))); // get number of keys

	// set key array, rid array and next page array
	string *keys = new string[keyNumber];
	string *rid_page = new string[keyNumber];
	string *rid_slot = new string[keyNumber];
	unsigned *nextPageArray = new unsigned[keyNumber + 1]; // pointers to next pages, only used by internal nodes

	// *****time to get key and nextPage arrays*****
	unsigned offsetDirectory = PAGE_SIZE - 5 * sizeof(short);
	if (attribute.type == TypeVarChar) {
		// varchar
		// non-leaf
		if(leafFlag == 1 || (leafFlag == 0 && ixfileHandle.pageNumber > 1)) {
			int nextPage = *((int*)((char*)space));
			nextPageArray[0] = nextPage;
		}
		for (int i = 0; i < keyNumber; i++) {
			short keyStart = *((short*) ((char*) space + offsetDirectory));
			short keyLength = *((short*) ((char*) space + offsetDirectory + sizeof(short))) - 2 * USSIZE;
			// deal with key
			for (int j = 0; j < keyLength; j++) {
				keys[i] += *((char*) space + keyStart + j);
			}
			// deal with rid
			rid_page[i] = Other2string(0, *((unsigned*) ((char*) space + keyStart + keyLength)), 1, 1.0);
			rid_slot[i] = Other2string(0, *((unsigned*) ((char*) space + keyStart + keyLength + USSIZE)), 1, 1.0);
			// deal with pointer, if it is a non-leaf
			if(leafFlag == 1 || (leafFlag == 0 && ixfileHandle.pageNumber != 1)) {
				nextPageArray[i + 1] = *((unsigned*) ((char*) space + keyStart + keyLength + 2 * USSIZE));
			}
			offsetDirectory -= 2 * sizeof(short);
		}
	} else if (attribute.type == TypeInt) {
		// int
		// non-leaf
		if(leafFlag == 1 || (leafFlag == 0 && ixfileHandle.pageNumber > 1)) {
			int nextPage = *((int*)((char*)space));
			nextPageArray[0] = nextPage;
		}
		for (int i = 0; i < keyNumber; i++) {
			short keyStart = *((short*) ((char*) space + offsetDirectory));
			// deal with key
			keys[i] = Other2string(1, 1, *((int*) ((char*) space + keyStart)), 1.0);
			// deal with rid
			rid_page[i] = Other2string(0, *((unsigned*) ((char*) space + keyStart + USSIZE)), 1, 1.0);
			rid_slot[i] = Other2string(0, *((unsigned*) ((char*) space + keyStart + 2 * USSIZE)), 1, 1.0);
			// deal with pointer
			if(leafFlag == 1 || (leafFlag == 0 && ixfileHandle.pageNumber != 1)) {
				nextPageArray[i + 1] = *((unsigned*) ((char*) space + keyStart + 3 * USSIZE));
			}
			offsetDirectory -= 2 * sizeof(short);
		}
	} else {
		// real
		// non-leaf
		if(leafFlag == 1 || (leafFlag == 0 && ixfileHandle.pageNumber > 1)) {
			int nextPage = *((int*)((char*)space));
			nextPageArray[0] = nextPage;
		}
		for (int i = 0; i < keyNumber; i++) {
			short keyStart = *((short*) ((char*) space + offsetDirectory));
			// deal with key
			keys[i] = Other2string(2, 1, 1, *((float*) ((char*) space + keyStart)));
			// deal with rid
			rid_page[i] = Other2string(0, *((unsigned*) ((char*) space + keyStart + USSIZE)), 1, 1.0);
			rid_slot[i] = Other2string(0, *((unsigned*) ((char*) space + keyStart + 2 * USSIZE)), 1, 1.0);
			// deal with pointer
			if(leafFlag == 1 || (leafFlag == 0 && ixfileHandle.pageNumber != 1)) {
				nextPageArray[i + 1] = *((unsigned*) ((char*) space + keyStart + 3 * USSIZE));
			}
			offsetDirectory -= 2 * sizeof(short);
		}
	}

	// ***** time to print*****
	// set up indentation
	string indent = "";
	for (int i = 0; i < level; i++) {
		indent += "\t";
	}
	if ((leafFlag == 0 && ixfileHandle.pageNumber > 1) || leafFlag == 1) {
		// non-leaf
		// key line
		string keyLine = indent + "{\"keys\":[";
		if (keyNumber != 0) {
			for (int i = 0; i < keyNumber - 1; i++) {
				keyLine += "\"" + keys[i] + "\",";
			}
			keyLine += "\"" + keys[keyNumber - 1] + "\"";
		}
		keyLine += "],";
		cout << keyLine << endl;

		// children line
		string childrenLine = indent + "\"children\":[";
		cout << childrenLine << endl;

		// children
		for (int i = 0; i < keyNumber; i++) {
			rc = printBtreeHelper(nextPageArray[i], level + 1, ixfileHandle, attribute);
			cout << "," << endl;
		}
		printBtreeHelper(nextPageArray[keyNumber], level + 1, ixfileHandle, attribute);

		// end line
		string endLine = indent + "]}";
		cout << endl << endLine << endl;
	} else {
		// leaf
		string line = indent + "{\"keys\":[";
		cout << line;
		map<string, string> keyMap;
		map<string, string>::iterator iter;
		for (int i = 0; i < keyNumber; i++) {
			keyMap[keys[i]] += "(" + rid_page[i] + "," + rid_slot[i] + "),";
		}

		for (iter = keyMap.begin(); iter != keyMap.end();) {
			line = "\"" + iter->first + ":[" + iter->second.substr(0, iter->second.size() - 1) + "]\"";
			iter++;
			if(iter != keyMap.end()) {
				line += ",";
			}
			cout << line;
		}
		line = "]}";
		cout << line;
	}

	free(space);
	delete[] nextPageArray;
	delete[] keys;
	delete[] rid_page;
	delete[] rid_slot;

	return 0;
}



IX_ScanIterator::IX_ScanIterator() {
	pageNum = 0;
	slotNum = 0;
	firstSearch = true;
	lastRID.pageNum = 0;
	lastRID.slotNum = 0;
}

IX_ScanIterator::~IX_ScanIterator() {
}

RC IX_ScanIterator::getNextEntry(RID &rid, void *key) {
	if(pageNum == -1) {
		pageNum = 0;
		slotNum = 0;
		firstSearch = true;
		lastRID.pageNum = 0;
		lastRID.slotNum = 0;
		return IX_EOF;
	}
	RC rc = 0;
	void *space = malloc(PAGE_SIZE);
	if (firstSearch) {
		firstSearch = false;
		int targetLeafPage = getToLeafPage(ixfileHandle.rootNum);
		if(targetLeafPage == -1) {
			free(space);
			return -1;
		}
		rc = ixfileHandle.readPage(targetLeafPage, space);
		if(rc != 0) {
			free(space);
			return rc; // if readpage correct
		}
		pageNum = targetLeafPage;
		slotNum = 0;
	} else {
		// check if professor deletes during scanning
		// check current key's rid
		ixfileHandle.readPage(pageNum, space);
		if(slotNum != 0) {
			short keyStart = *((short*)((char*)space + PAGE_SIZE - sizeof(short) * (3 + 2 * slotNum)));
			short keyLength = *((short*)((char*)space + PAGE_SIZE - sizeof(short) * (2 + 2 * slotNum))) - 2 * USSIZE;
			RID tmpRID;
			memcpy(&tmpRID, (char*)space + keyStart + keyLength, 2 * USSIZE);
			if(tmpRID.pageNum != lastRID.pageNum || tmpRID.slotNum != lastRID.slotNum) {
				slotNum--; // deleting
			}
		}
	}

	short keyOffset = 0;
	short keyLength = 0;
	int i = 0;
	int keyFlag = 0; // 0 for find a key, 1 for no key left
	short keyNumber = *((short*) ((char*) space + PAGE_SIZE - 3 * sizeof(short)));
	while (true) {
		if (slotNum >= keyNumber) {
			int siblingNum = *((int*) ((char*) space));
			if (siblingNum == -1) {
				keyFlag = 1;
				break;
			} else {
				rc = ixfileHandle.readPage(siblingNum, space);
				if(rc != 0) {
					free(space);
					return rc; // if readpage correct
				}
				pageNum = siblingNum;
				slotNum = 0;
				keyNumber = *((short*) ((char*) space + PAGE_SIZE - 3 * sizeof(short)));
			}
		} else {
			bool findFirstKey = false;
			bool searchFinish = false;
			for (i = slotNum; i < keyNumber; i++) {
				keyOffset = *((short*) ((char*) space + PAGE_SIZE - sizeof(short) * ((i + 1) * 2 + 3)));
				keyLength = *((short*) ((char*) space + PAGE_SIZE - sizeof(short) * ((i + 1) * 2 + 2))) - 2 * USSIZE;

				// compare
				int lowResult = 0;
				int highResult = 0;
				iterCompare(space, keyOffset, keyLength, lowResult, highResult);
				memcpy(key, (char*)space + keyOffset, keyLength);

				// when should scan end
				if(highResult > 0 || (highResult == 0 && !highKeyInclusive)) {
					// finish
					searchFinish = true;
					break;
				}
				if(lowResult >= 0) {
					// current >= low key
					if(lowResult > 0 || lowKeyInclusive) {
						// inclusive, find one
						slotNum = i;
						findFirstKey = true;
						break;
					}
				}
			}
			if (searchFinish) {
				// searching finish
				keyFlag = 1;
				break;
			}
			if (findFirstKey) {
				// find one
				keyFlag = 0;
				if(slotNum == keyNumber - 1) {
					// slotNum inside class should go to the next
					int sibling = *((int*)((char*)space));
					pageNum = sibling;
					slotNum = 0;
				} else {
					slotNum++;
				}
				break;
			}
			// fail to find one at this page
			slotNum = keyNumber;
		}
	}

	// prepare rid to return
	rc = 0;
	if (keyFlag == 0) {
		rid.pageNum = *((unsigned*) ((char*) space + keyOffset + keyLength));
		rid.slotNum = *((unsigned*) ((char*) space + keyOffset + keyLength + USSIZE));
		lastRID = rid;
	} else {
		pageNum = 0;
		slotNum = 0;
		firstSearch = true;
		lastRID.pageNum = 0;
		lastRID.slotNum = 0;
		rc = IX_EOF; // searching finish finally
	}

	free(space);
	return rc;
}

int IX_ScanIterator::getToLeafPage(unsigned rootPage) {
	unsigned currentPage = rootPage;
	void *space = malloc(PAGE_SIZE);
	while (true) {
		RC rc = ixfileHandle.readPage(currentPage, space);
		if (rc != 0) {
			free(space);
			return rc;
		}
		short ifLeaf = *((short*) ((char*) space + PAGE_SIZE - sizeof(short)));
		if (ifLeaf == 2) {
			break; // it is a leaf page
		}
		bool goToNextPage = false;
		short keyNumber = *((short*) ((char*) space + PAGE_SIZE - 3 * sizeof(short)));

		for (int i = 0; i < keyNumber; i++) {
			if (goToNextPage) {
				break;
			}
			short keyOffset = *((short*) ((char*) space + PAGE_SIZE - sizeof(short) * (3 + (i + 1) * 2)));
			short keyLength = *((short*) ((char*) space + PAGE_SIZE - sizeof(short) * (2 + (i + 1) * 2))) - 2 * USSIZE;

			// get comparison results of low key and high key
			int lowResult = 0;
			int highResult = 0;
			iterCompare(space, keyOffset, keyLength, lowResult, highResult);

			if(lowResult == 1) {
				// find it, current key > low key
				currentPage = *((int*) ((char*) space + keyOffset - USSIZE));
				goToNextPage = true;
			} else if(lowResult == 0) {
				// duplicate situation
				RID tempRID;
				tempRID.pageNum = *((unsigned*)((char*)space + keyOffset + keyLength));
				tempRID.slotNum = *((unsigned*)((char*)space + keyOffset + keyLength + USSIZE));
				if(tempRID.pageNum != 0 || tempRID.slotNum != 0) {
					currentPage = *((int*) ((char*) space + keyOffset - USSIZE));
					goToNextPage = true;
				}
			}
		}
		// right-most, fail to find one in keyNumber loops
		if(!goToNextPage) {
			short lastKeyOffset = *((short*) ((char*) space + PAGE_SIZE - sizeof(short) * (3 + keyNumber * 2)));
			short lastEntryLength = *((short*) ((char*) space + PAGE_SIZE - sizeof(short) * (2 + keyNumber * 2)));
			currentPage = *((int*)((char*)space + lastKeyOffset + lastEntryLength));
			goToNextPage = true;
		}
	}

	free(space);
	return currentPage;
}

void IX_ScanIterator::iterCompare(void *space, short keyOffset, short keyLength, int &lowResult, int &highResult) { // -1 for smaller, 0 for each, 1 for larger than key for comparison
	if(attribute.type == TypeVarChar) {
		// varchar
		string currentKeyValue = "";
		for (int j = 0; j < keyLength; j++) {
			currentKeyValue += *((char*) space + keyOffset + j);
		}
		lowResult = (lowKey == NULL || currentKeyValue > lowVarchar) ? 1 : (currentKeyValue == lowVarchar ? 0 : -1); // low
		highResult = (highKey == NULL || currentKeyValue < highVarchar) ? -1 : (currentKeyValue == highVarchar ? 0 : 1); // high
	} else if (attribute.type == TypeInt){
		//int
		int currentKeyValue = *((int*) ((char*) space + keyOffset));
		lowResult = (lowKey == NULL || currentKeyValue > lowInt) ? 1 : (currentKeyValue == lowInt ? 0 : -1); // low
		highResult = (highKey == NULL || currentKeyValue < highInt) ? -1 : (currentKeyValue == highInt ? 0 : 1); // high
	} else {
		// real
		float currentKeyValue = *((float*) ((char*) space + keyOffset));
		lowResult = (lowKey == NULL || currentKeyValue > lowReal) ? 1 : (currentKeyValue == lowReal ? 0 : -1); // low
		highResult = (highKey == NULL || currentKeyValue < highReal) ? -1 : (currentKeyValue == highReal? 0 : 1); // high
	}
}

RC IX_ScanIterator::prepare(IXFileHandle &ixfh, const Attribute &attribute,
		const void *lowKey, const void *highKey, bool lowKeyInclusive,
		bool highKeyInclusive) {
	RC rc = 0;
    ixfileHandle = ixfh;
    this->attribute = attribute;
    this->lowKey = lowKey;
    this->highKey = highKey;
    this->lowKeyInclusive = lowKeyInclusive;
    this->highKeyInclusive = highKeyInclusive;

    // make lowKey and highKey
    if(attribute.type == TypeVarChar) {
    		// varchar
    		if(lowKey != NULL) {
    			lowVarchar = "";
    			int lowKeyLength = *((int*) ((char*) lowKey));
			for (int j = 0; j < lowKeyLength; j++) {
				lowVarchar += *((char*) lowKey + USSIZE + j);
			}
    		}
    		if(highKey != NULL) {
    			highVarchar = "";
    			int highKeyLength = *((int*) ((char*) highKey));
			for (int j = 0; j < highKeyLength; j++) {
				highVarchar += *((char*) highKey + USSIZE + j);
			}
    		}

    } else if(attribute.type == TypeInt) {
    		// int
    		if(lowKey != NULL) {
    			lowInt = *((int*) ((char*) lowKey));
    		}
    		if(highKey != NULL) {
    			highInt = *((int*) ((char*) highKey));
    		}
    } else {
    		// real
    		if(lowKey != NULL) {
			lowReal = *((float*) ((char*) lowKey));
    		}
    		if(highKey != NULL) {
    			highReal = *((float*) ((char*) highKey));
    		}
    }

    return rc;
}

RC IX_ScanIterator::close() {
	fclose(ixfileHandle.fp);
	return 0;
}

IXFileHandle::IXFileHandle() {
	pageNumber = 0;
}

IXFileHandle::~IXFileHandle() {
}

RC IXFileHandle::readPage(PageNum pageNum, void *data)
{
	if(pageNum + 1 > pageNumber)
	{
		return -1; // page not exists
	}

	// find the position of target page from the beginning of file
	fseek(fp, COUNTERS_SPACE + pageNum * PAGE_SIZE, SEEK_SET);
	fread(data, 1, PAGE_SIZE, fp);
	readPageCounter++;
	return 0;
}


RC IXFileHandle::writePage(PageNum pageNum, const void *data)
{
	if(pageNum > pageNumber - 1)
	{
		return -1; // page not exists
	}
	fseek(fp, COUNTERS_SPACE + pageNum * PAGE_SIZE, SEEK_SET);
	if(fwrite(data, 1, PAGE_SIZE, fp) != 0)
	{
		writePageCounter++;
		return 0; // success
	}
	else
	{
		return -1; // failure
	}
}


RC IXFileHandle::appendPage(const void *data)
{
	if(fp == NULL) {
		return -1; // file not exists
	}

	// append a page(PAGE SIZE)
	fseek(fp, 0, SEEK_END); // find the end of all pages
	fwrite(data, 1, PAGE_SIZE, fp);
	appendPageCounter++;
	pageNumber++;

	return 0;
}


unsigned IXFileHandle::getNumberOfPages()
{
    return pageNumber;
}

RC IXFileHandle::collectCounterValues(unsigned &readPageCount,
		unsigned &writePageCount, unsigned &appendPageCount) {
	readPageCount = this->readPageCounter;
	writePageCount = this->writePageCounter;
	appendPageCount = this->appendPageCounter;
	return 0;
}

IXPagedFileManager* IXPagedFileManager::_ix_pf_manager = 0;

IXPagedFileManager* IXPagedFileManager::instance() {
	if (!_ix_pf_manager)
		_ix_pf_manager = new IXPagedFileManager();

	return _ix_pf_manager;
}

IXPagedFileManager::IXPagedFileManager() {

}

IXPagedFileManager::~IXPagedFileManager() {

}

RC IXPagedFileManager::createIXFile(const string &fileName) {
	FILE *fp = fopen(fileName.c_str(), "rb+");

	if (fp != NULL) {
		fclose(fp);
		return -1; // already exist
	}
	if (fp == NULL) {
		fp = fopen(fileName.c_str(), "wb+");
		// make default counters and some empty space, PAGEZ_SIZE in all
		unsigned counters[6] = { 0, 0, 0, 0, 0, 0 };
		fwrite(counters, sizeof(unsigned), 6, fp);

		void *space = malloc(PAGE_SIZE - 6 * sizeof(unsigned));
		memset(space, 0, PAGE_SIZE - 6 * sizeof(unsigned));
		fwrite(space, PAGE_SIZE - 6 * sizeof(unsigned), 1, fp);

		fclose(fp);
		free(space);
		return 0; // success
	}
	return -1;
}

RC IXPagedFileManager::openIXFile(const string &fileName, IXFileHandle &ixfileHandle) {
	ixfileHandle.fp = fopen(fileName.c_str(), "rb+");
	if (ixfileHandle.fp == NULL) {
		return -1; // file not exists
	} else {
		if (false) {
			return -1; // file still opened
		} else {
			// put counters into memory
			ixfileHandle.fileName = fileName;
			unsigned counters[6];
			fseek(ixfileHandle.fp, 0, SEEK_SET);
			fread(counters, USSIZE, 6, ixfileHandle.fp);

			AttrType tempType;
			if (counters[5] == 0)
				tempType = TypeInt;
			else if (counters[5] == 1)
				tempType = TypeReal;
			else
				tempType = TypeVarChar;

			ixfileHandle.readPageCounter = counters[0];
			ixfileHandle.writePageCounter = counters[1];
			ixfileHandle.appendPageCounter = counters[2];
			ixfileHandle.pageNumber = counters[3];
			ixfileHandle.rootNum = counters[4];
			ixfileHandle.type = tempType;

			return 0; // success
		}
	}
}

RC IXPagedFileManager::closeIXFile(IXFileHandle &ixfileHandle) {
	if (ixfileHandle.fp == NULL) {
		return -1; // fp not exists
	} else {
		// put counters into disk
		fseek(ixfileHandle.fp, 0, SEEK_SET);
		unsigned counters[5] =
				{ ixfileHandle.readPageCounter, ixfileHandle.writePageCounter,
						ixfileHandle.appendPageCounter, ixfileHandle.pageNumber,
						ixfileHandle.rootNum};

		fwrite(counters, sizeof(int), 5, ixfileHandle.fp);
		fwrite(&ixfileHandle.type, sizeof(int), 1, ixfileHandle.fp);
		ixfileHandle.fileName = "";
		fclose(ixfileHandle.fp);

		return 0; // success
	}
}

RC IXPagedFileManager::destroyIXFile(const string &fileName) {
	RC ret = remove(fileName.c_str());
	return ret == 0 ? 0 : -1;
}
