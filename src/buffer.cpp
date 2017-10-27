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
	for (int i = 0; i < numBufs - 1; i++) {
	    BufDesc bf = this->bufDescTable[i];
	    flushFile(bf.file);
	}
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
  
  // Check if clockHand has reached numBufs - 1
  if (clockHand == numBufs - 1) {
    clockHand = 0;
  } else {
    clockHand++;
  }
}

// Allocate a free frame
// Called from end of flowchart after we determine which frame to use...
// frame is the return value
void BufMgr::allocBuf(FrameId & frame) 
{
	int numPinnedFrames = 0;
	// Implement flow chart here!
	while(true) {
		advanceClock();
		BufDesc bf = this->bufDescTable[this->clockHand];

		// check if valid is set
		if (bf.valid) {
			// check if refbit is set
			if (bf.refbit) {
				// clear and continue ^^^
				bf.refbit = false;
				continue;

			} else {
				// check if page is pinned
				if (bf.pinCnt > 0) {
					if (numBufs == ++numPinnedFrames)
						throw BufferExceededException;
					continue;

				} else {
					// check dirty bit
					if (bf.dirty) {
						// flush page to disk
						try {
							bf.file->writePage(bf.file->readPage(bf.pageNo));

							// remove from hashtable
							try {
								this->hashTable->remove(bf.file, bf.pageNo);
							} catch (HashNotFoundException &e) {
								//do nothing
							}

							bf.dirty = false;

						} catch (InvalidPageException &e) {
							// do nothing
						}

						// call set on the frame
						bf.Set(bf.frameNo);
						frame = bf.frameNo;
						return;

					} else {
						// call set on the frame
						bf.Set(bf.frameNo);
						frame = bf.frameNo;
						return;
					}
				}
			}

		} else {
			// if not valid, call set on frame
			bf.Set(bf.frameNo);
			frame = bf.frameNo;
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

// PUBLIC
void BufMgr::readPage(File* file, const PageId pageNo, Page*& page)
{
	FrameId frameNo;
	try {
		// if page is already in buffer pool, do this
		this->hashTable->lookup(file, pageNo, frameNo);
		BufDesc bf = this->bufDescTable[frameNo];

		bf.refbit = true;
		bf.pinCnt++;

		// return address to page in buffer pool
		page = &(this->bufPool[frameNo]);
		return;
	} catch (HashNotFoundException &e) {
		// if page is not in the buffer pool, do this
		allocBuf(frameNo);
		try {
			// read page from disk
			Page pageRead = file->readPage(pageNo, true); // second param prevents reading unused pages if false
			this->bufPool[frameNo] = pageRead;

			try {
				this->hashTable->insert(file, pageNo, frameNo);

			} catch (HashAlreadyPresentException &e) {
				// do nothing
			} catch (HashTableException &e) {
				// still do nothing
			}

			// set description bits for new page in the buffer description
			BufDesc bf = this->bufDescTable[frameNo];
			bf.Set(file, pageNo);

			// return address to page in buffer pool
			page = &(this->bufPool[frameNo]);
			return;
		} catch (InvalidPageException &e) {
			// do nothing
		}

	}
}

// Unpin a page from memory since it is no longer required for it to remain in memory
void BufMgr::unPinPage(File* file, const PageId pageNo, const bool dirty)
{
  FrameId fid;

  try {
    this->hashTable->lookup(file, pageNo, fid);
  } catch (HashNotFoundException &e) {
    // do nothing...
    return;
  }

  BufDesc bf = this->bufDescTable[fid];

  if (bf.pinCnt == 0) {
    throw PageNotPinnedException;
  }

  bf.dirty = dirty;
  bf.pinCnt--;
}

void BufMgr::flushFile(const File* file) 
{
  // TODO: should we scan twice so we don't write pages from file with a unpinned page later down the line????
	if (file == NULL) {
		// won't do anything
		return;
	}

  for (int i = 0; i < numBufs - 1; i++) {
    BufDesc bf = this->bufDescTable[i];

    // only operate on bufDesc entries related to our file input
    if (bf.file == file) {

      // check if valid
      if (!bf.valid) throw BadBufferException;

      // consider this bufDesc
      if (bf.pinCnt > 0) throw PagePinnedException;

      if (bf.dirty) {
        // write out page of pageNum to File
    	try {
          bf.file->writePage(bf.file->readPage(bf.pageNo));
    	} catch (InvalidPageException &e) {
    		// do nothing
    	}
        bf.dirty = false;
      }

      try {
    	  this->hashTable->remove(bf.file, bf.pageNo);
      } catch (HashNotFoundException &e) {
    	  //do nothing
      }
      bf.Clear();
    }
  }
}

// Allocates a new, empty page in the file and returns the Page object
// The new page is also assigned a frame in the buffer pool



// PUBLIC

// TODO: this is the master function that the client calls to get a page.
// Steps:
// 1) look for the requested pageNo

void BufMgr::allocPage(File* file, PageId &pageNo, Page*& page) 
{
  Page newPage = file->allocatePage();
  pageNo = newPage.page_number();

  FrameId frameNo;

  //obtain frame for buffer pool
  allocBuf(frameNo);


  BufDesc bf = this->bufDescTable[frameNo];

  // entry inserted into hash table and set
  try {
  	this->hashTable->insert(file, pageNo, frameNo);
  	bf.Set(file, pageNo);
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

// PRIVATE
void BufMgr::disposePage(File* file, const PageId PageNo)
{
  for (int i = 0; i < numBufs - 1; i++) {
	    BufDesc bf = this->bufDescTable[i];

	    // only operate on bufDesc entries related to our file input
	  if (bf.file == file) {
		  // remove page from file and buffer file
		  file->deletePage(PageNo);

		  // remove from hashtable
		  try {
			  this->hashTable->remove(bf.file, bf.pageNo);
		  } catch (HashNotFoundException &e) {
			  //do nothing
		  }
		  bf.Clear();
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
