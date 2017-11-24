#include "pfm.h"
#include "errno.h"

PagedFileManager* PagedFileManager::_pf_manager = 0;

PagedFileManager* PagedFileManager::instance()
{
    if(!_pf_manager)
        _pf_manager = new PagedFileManager();

    return _pf_manager;
}


PagedFileManager::PagedFileManager()
{
}

PagedFileManager::~PagedFileManager()
{
}

RC PagedFileManager::createFile(const string &fileName)
{
	FILE *fp = fopen(fileName.c_str(), "rb+");

	if(fp != NULL)
	{
		fclose(fp);
		return -1; // failure
	}
	if(fp == NULL)
	{
		fp = fopen(fileName.c_str(), "wb+");
		// make default counters and some empty space, PAGEZ_SIZE in all
		unsigned counters[4] = {0, 0, 0, 0};
		fwrite(counters, sizeof(unsigned), 4, fp);

		void *space = malloc(PAGE_SIZE - 4 * sizeof(unsigned));
		memset(space, 0, PAGE_SIZE - 4 * sizeof(unsigned));
		fwrite(space, PAGE_SIZE - 4 * sizeof(unsigned), 1, fp);

		fclose(fp);
		free(space);
		return 0; // success
	}
	return -1;
}

RC PagedFileManager::destroyFile(const string &fileName)
{
	RC ret = remove(fileName.c_str());
	return ret == 0 ? 0 : -1;
}


RC PagedFileManager::openFile(const string &fileName, FileHandle &fileHandle)
{
	fileHandle.fp = fopen(fileName.c_str(), "rb+");
	if(fileHandle.fp == NULL)
	{
		return -1; // file not exists
	}
	else
	{
		if(!fileHandle.fileName.empty() && fileHandle.fileName.compare(fileName) != 0)
		{
			fclose(fileHandle.fp);
			return -1; // this handle is for another
		}
		else
		{
			// put counters into memory
			fileHandle.fileName = fileName;
			unsigned counters[4];
			fseek(fileHandle.fp, 0, SEEK_SET);
			fread(counters, sizeof(unsigned), 4, fileHandle.fp);

			fileHandle.readPageCounter = counters[0];
			fileHandle.writePageCounter = counters[1];
			fileHandle.appendPageCounter = counters[2];
			fileHandle.pageNumber = counters[3];

			return 0; // success
		}
	}
}


RC PagedFileManager::closeFile(FileHandle &fileHandle)
{
	if(fileHandle.fp == NULL)
	{
		return -1; // fp not exists
	}
	else
	{
		// put counters into disk
		fseek(fileHandle.fp, 0, SEEK_SET);
		unsigned counters[4] = {fileHandle.readPageCounter,
				fileHandle.writePageCounter,
				fileHandle.appendPageCounter,
				fileHandle.pageNumber};

		fwrite(counters, sizeof(unsigned), 4, fileHandle.fp);
		fclose(fileHandle.fp);

		return 0; // success
	}
}


FileHandle::FileHandle()
{
}


FileHandle::~FileHandle()
{
}


RC FileHandle::readPage(PageNum pageNum, void *data)
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


RC FileHandle::writePage(PageNum pageNum, const void *data)
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


RC FileHandle::appendPage(const void *data)
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


unsigned FileHandle::getNumberOfPages()
{
    return pageNumber;
}


RC FileHandle::collectCounterValues(unsigned &readPageCount, unsigned &writePageCount, unsigned &appendPageCount)
{
	readPageCount = this->readPageCounter;
	writePageCount = this->writePageCounter;
	appendPageCount = this->appendPageCounter;

    return 0;
}
