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
  
}

  // Advances clock to next frame in the buffer pool
void BufMgr::advanceClock()
{

}

  // Allocate a free frame
void BufMgr::allocBuf(FrameId & frame) 
{
  
}

  // Reads the given page from the file into a frame and returns the pointer to page
  // If the requested page is already present in the buffer pool,
  //   pointer to that frame is returned
  // Else a new frame is allocated from the buffer pool for reading the page
void BufMgr::readPage(File* file, const PageId pageNo, Page*& page)
{
  
}

  // Unpin a page from memory since it is no longer required for it to remain in memory
void BufMgr::unPinPage(File* file, const PageId pageNo, const bool dirty) 
{
  
}

  // Writes out all dirty pages of the file to disk
  // All frames assigned to the file need to be unpinned from buffer pool before this works
  // Else ERROR
void BufMgr::flushFile(const File* file) 
{
  
}

  // Allocates a new, empty page in the file and returns the Page object
  // The new page is also assigned a frame in the buffer pool
void BufMgr::allocPage(File* file, PageId &pageNo, Page*& page) 
{
  
}

  // Delete a page from file and also from buffer pool if present
  // Don't need to check if page is dirty
void BufMgr::disposePage(File* file, const PageId PageNo)
{
    
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
