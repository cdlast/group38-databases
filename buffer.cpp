/**
 * @author See Contributors.txt for code contributors and overview of BadgerDB.
 *
 * @section LICENSE
 * Copyright (c) 2012 Database Group, Computer Sciences Department, University of Wisconsin-Madison.
 */

#include <memory>
#include <iostream>
#include "buffer.h"
#include "exceptions/buffer_exceeded_exception.h"
#include "exceptions/page_not_pinned_exception.h"
#include "exceptions/page_pinned_exception.h"
#include "exceptions/bad_buffer_exception.h"
#include "exceptions/hash_not_found_exception.h"
#include "exceptions/invalid_page_exception.h"
#include "exceptions/hash_already_present_exception.h"
#include "exceptions/hash_table_exception.h"

namespace badgerdb { 

// Constructor for BufMgr
BufMgr::BufMgr(std::uint32_t bufs)
	: numBufs(bufs) {

  bufDescTable = new BufDesc[bufs];

  for (FrameId i = 0; i < bufs; i++) 
  {
  	bufDescTable[i].frameNo = i;
  	bufDescTable[i].valid = false;
  }

  bufPool = new Page[bufs];

  int htsize = ((((int) (bufs * 1.2))*2)/2)+1;
  hashTable = new BufHashTbl (htsize);  // allocate the buffer hash table

  clockHand = bufs - 1;
}

// Destructor for BufMgr
BufMgr::~BufMgr()
{
  // flush all dirty pages to file
	for (unsigned int i = 0; i < numBufs; i++) {
	    BufDesc *bf = &this->bufDescTable[i];
	    flushFile(bf->file);
	}
	
	// delete all other allocated memory
    delete hashTable;
    delete bufPool;
    delete bufDescTable;
}


// ********
// PRIVATE METHODS
// ********

// Advances clock to next frame in the buffer pool
void BufMgr::advanceClock()
{
  // FrameId (uint32_t) clockHand
  
  // Check if clockHand has reached numBufs and revert to 0
  clockHand++;

  if (clockHand >= numBufs) {
    clockHand = 0;
  }
}

// Allocate a free frame
// Called from end of flowchart after we determine which frame to use...
// frame is the return value
void BufMgr::allocBuf(FrameId & frame) 
{
	unsigned int numPinnedFrames = 0;
	
	// Implemented flow chart as in the project pdf
	while(true) {
		advanceClock();
		BufDesc *bf = &this->bufDescTable[this->clockHand];

		// check if valid is set
		if (bf->valid) {
			// check if refbit is set
			if (bf->refbit) {
				// clear and continue
				bf->refbit = false;
				continue;
			} else {
				// check if page is pinned
				if (bf->pinCnt > 0) {
					
					// throw exception if all frames in the buffer are pinned to show that the buffer is full
					if (numBufs == ++numPinnedFrames)
						throw BufferExceededException();
					continue;

				} else {
					// check dirty bit
					if (bf->dirty) {
						// flush page to disk
						try {
							bf->file->writePage(bf->file->readPage(bf->pageNo));

							// remove from hashtable
							try {
								this->hashTable->remove(bf->file, bf->pageNo);
							} catch (HashNotFoundException &e) {
								//do nothing
							}

							bf->dirty = false;

						} catch (InvalidPageException &e) {
							// do nothing
						}

						// call set on the frame
						bf->Set(bf->file, bf->pageNo);
						frame = this->clockHand;
						return;

					} else {
						// call set on the frame
						bf->Set(bf->file, bf->pageNo);
						frame = this->clockHand;
						return;
					}
				}
			}

		} else {
			// if not valid, call set on frame
			bf->Set(bf->file, bf->pageNo);
			frame = this->clockHand;
			return;
		}
	}

}




// ********
// PUBLIC METHODS
// ********


// Reads the given page from the file into a frame and returns the pointer to page
// If the requested page is already present in the buffer pool,
//   pointer to that frame is returned
// Else a new frame is allocated from the buffer pool for reading the page
void BufMgr::readPage(File* file, const PageId pageNo, Page*& page)
{
	FrameId frameNo;
	try {
		// if page is already in buffer pool, do this
		this->hashTable->lookup(file, pageNo, frameNo);


	std::cout << "frameNo:" << frameNo << ":" << file->filename() << std::endl;


		BufDesc *bf = &this->bufDescTable[frameNo];

	std::cout << "frameNo:" << frameNo << ":" << bf->file->filename() << std::endl;

		/**if(!(bf->file->filename() == file->filename())){
		throw HashNotFoundException(file->filename(), pageNo);
		}*/
	std::cout << "HERE1" << std::endl;


		bf->refbit = true;
		bf->pinCnt++;

		// return address to page in buffer pool
		page = &(this->bufPool[frameNo]);
	} catch (HashNotFoundException &e) {

std::cout << "HERE2" << std::endl;
		// if page is not in the buffer pool, do this


		// read page from disk
		Page pageRead = file->readPage(pageNo);
		allocBuf(frameNo);
		this->bufPool[frameNo] = pageRead;


		// remove OLD data from hashtable
		BufDesc *bff = &this->bufDescTable[frameNo];
		this->hashTable->remove(bff->file, bff->pageNo);


		try {
			this->hashTable->insert(file, pageNo, frameNo);
		} catch (HashAlreadyPresentException &e) {
			// do nothing
		} catch (HashTableException &e) {
			// still do nothing
		}

		// set description bits for new page in the buffer description
		BufDesc *bf = &this->bufDescTable[frameNo];

std::cout << "file: " << file << std::endl;
		bf->Set(file, pageNo);
std::cout << "file: " << bf->file << std::endl;

		// return address to page in buffer pool
		page = &(this->bufPool[frameNo]);
std::cout << "Here 3" << std::endl;

	}
}

// Unpin a page from memory since it is no longer required for it to remain in memory
void BufMgr::unPinPage(File* file, const PageId pageNo, const bool dirty)
{
  FrameId fid;

  // look up the frame id in the hashtable using the file and page number
  try {
    this->hashTable->lookup(file, pageNo, fid);
  } catch (HashNotFoundException &e) {
    // do nothing...
    return;
  }

  BufDesc *bf = &this->bufDescTable[fid];

  // do not try to unpin if the pincount is already 0
  if (bf->pinCnt == 0) {
    throw PageNotPinnedException(file->filename(), pageNo, fid);
  }

  bf->dirty = dirty;
  bf->pinCnt--;
}

void BufMgr::flushFile(const File* file) 
{
  // scan twice so we don't write pages from file with a unpinned page later down the line
	if (file == NULL) {
		// won't do anything
		return;
	}

  // first scan ensures that there are no errors present in the buffer pool
  for (unsigned int i = 0; i < numBufs; i++) {
    BufDesc *bf = &this->bufDescTable[i];

    // only operate on bufDesc entries related to our file input
    if (bf->file->filename() == file->filename()) {

      // check if valid
      if (!bf->valid) throw BadBufferException(bf->frameNo, bf->dirty, bf->valid, bf->refbit);

      // consider this bufDesc
      if (bf->pinCnt > 0) throw PagePinnedException(file->filename(), bf->pageNo, bf->frameNo);
	}
  }
  
  // flushes all dirty files from the buffer pool
  for (unsigned int i = 0; i < numBufs; i++) {
    BufDesc *bf = &this->bufDescTable[i];
    if (bf->file->filename() == file->filename()) {

      if (bf->dirty) {
        // write out page of pageNum to File
    	try {
          bf->file->writePage(bf->file->readPage(bf->pageNo));
    	} catch (InvalidPageException &e) {
    		// do nothing
    	}
        bf->dirty = false;
      }

	  // removes flushed files from the hashtable
      try {
    	  this->hashTable->remove(bf->file, bf->pageNo);
      } catch (HashNotFoundException &e) {
    	  //do nothing
      }
      bf->Clear();
    }
  }
}

// Allocates a new, empty page in the file and returns the Page object
// The new page is also assigned a frame in the buffer pool

void BufMgr::allocPage(File* file, PageId &pageNo, Page*& page) 
{
  Page newPage = file->allocatePage();
  pageNo = newPage.page_number();

  FrameId frameNo;

  //obtain frame for buffer pool
  allocBuf(frameNo);

  BufDesc *bf = &this->bufDescTable[frameNo];

  // entry inserted into hash table and set
  try {
  	this->hashTable->insert(file, pageNo, frameNo);
  	bf->Set(file, pageNo);
  } catch (HashAlreadyPresentException &e) {
  	// do nothing
  } catch (HashTableException &e) {
  	// still do nothing
  }
  
  // return address to page in buffer pool
  page = &(this->bufPool[frameNo]);
}




// Delete a page from file and also from buffer pool if present
// Don't need to check if page is dirty
void BufMgr::disposePage(File* file, const PageId PageNo)
{
  for (unsigned int i = 0; i < numBufs; i++) {
	    BufDesc *bf = &this->bufDescTable[i];

	    // only operate on bufDesc entries related to our file input
	  if (bf->file == file) {
		  // remove page from file and buffer file
		  file->deletePage(PageNo);

		  // remove from hashtable
		  try {
			  this->hashTable->remove(bf->file, bf->pageNo);
		  } catch (HashNotFoundException &e) {
			  //do nothing
		  }
		  bf->Clear();
	  }
  }
}

// Print member variable values
// Don't change this
void BufMgr::printSelf(void) 
{
  BufDesc* tmpbuf;
  int validFrames = 0;
  
  for (std::uint32_t i = 0; i < numBufs; i++)
  {
    tmpbuf = &(bufDescTable[i]);
    std::cout << "FrameNo:" << i << " ";
    tmpbuf->Print();

    if (tmpbuf->valid == true)
      validFrames++;
  }
  
  std::cout << "Total Number of Valid Frames:" << validFrames << "\n";
}

}
