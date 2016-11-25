/*---------------------------------------------------------------------------  
fileName:         buffer_mgr.c                                             
Authors:          Niharika Karia, Shiva Naveen Ravi, Shiv Ram Krishna       
Purpose:          To implement the functions of a Buffer manager.    	     
Language:         C                                                         
Created On:       08 September,2015                                           
Last Modified On: 06 October, 2015                                       
---------------------------------------------------------------------------*/
/*Headers Declaration*/
#include "buffer_mgr.h"
#include "storage_mgr.h"
#include "dberror.h"
#include <stdio.h>
#include <stdlib.h>
#include <string.h>

typedef struct frameMetadata
{
int pageNum;                   //this represents a particular page number of a file
int fixCounts;		      //this represents the holds through page request of a file
bool dirtyFlag;		     //flag to represent that a page has been modified and needs file write back
char *pageFrame;	    //Pointer that points to our page handle containing the data
struct frameMetadata *next;//Pointer to create logical links between pages of file 
} frameMetadata;

typedef struct bufferPoolStat        //Structure for Statistics Interface
{	
SM_FileHandle fh;                  // File Handle for the file in context
PageNumber *oFrameContents;       //represents the page number for the array of page numbers.
bool *oDirtyFlags;               //pointer to represent that a page is modified and needs writeback.
int *oFixCounts;                //Number of clients holding a page through requests
int *oReadIO;		       //pointer for Number of reads for pages of a file
int *oWriteIO;		      //pointer for Number of Writes for pages of a file
int rear;		     //integer for the FIFO and LRU Page Replacement Termination  Strategy
int *LeastRecentlyUsed;     //Pointer for the LRU Page Replacement strategy
struct frameMetadata *next;//Pointer to create logical links between pages of file
} bufferPoolStat;

int counter,flag=0;      //Global variables
frameMetadata *looper;  //Global Pointer to loop thrugh the pages.
/*------------------------------initBufferPool--------------------------------
Function:   initBufferPool                                               
Purpose:    To create a new buffer pool with page-frames using a page 
	    replacement strategy
Parameters: IN =  bm :Buffer Pool of a file in context	
	    IN = pageFileName : Name of the file in context	
            IN = strategy: Strategy to Replace pages with when buffer is full. 
	    IN =stratData: Used for strategy based parameters  
 	    IN =numPages: No of pages to initialize the buffer pool with        
            OUT = RC_OK    :after successfull completion         		   
Returns:    RC_OK: completion status of Intialization of BufferPool                        
         		                                                        
------------------------------------------------------------------------------*/
RC initBufferPool(BM_BufferPool *const bm, const char *const pageFileName, 
		  const int numPages, ReplacementStrategy strategy, 
		  void *stratData) 
{
bm->pageFile = (char*) malloc(strlen(pageFileName) + sizeof(char));
strcpy(bm->pageFile,pageFileName);			   //Assign file'sfilename for the bufferpool's File
bm->numPages=numPages; 			       		  //Assign File's number of total pages to bufferPool's capacity
bm->strategy=strategy;                       		 //Assign Strategy to follow for Page Replacement if needed.
bm->mgmtData= malloc(sizeof(bufferPoolStat));		//We point the mgmtData of bufferpool to Statistics Struture for the FIle
((bufferPoolStat *)bm->mgmtData)->oFrameContents=(PageNumber *)malloc(sizeof(PageNumber)*bm->numPages);//Allocate space for pagenumber of all pages
((bufferPoolStat *)bm->mgmtData)->oDirtyFlags=(bool *)malloc(sizeof(bool)*bm->numPages);//Allocate Space for Dirty Status of all pages
((bufferPoolStat *)bm->mgmtData)->oFixCounts=(int *)malloc(sizeof(int)*bm->numPages);  //Allocate Space for Fixcounts of all pages. 
((bufferPoolStat *)bm->mgmtData)->oReadIO=(int *)malloc(sizeof(int));   //Allocate Space for disk access Countthrough read 
*(((bufferPoolStat *)bm->mgmtData)->oReadIO)=0;                        //Set read count to 0 intitally.
((bufferPoolStat *)bm->mgmtData)->oWriteIO=(int *)malloc(sizeof(int));//Allocate space count of disc access through write. 
*(((bufferPoolStat *)bm->mgmtData)->oWriteIO)=0;                                              //Set write count to 0 initially. 
((bufferPoolStat *)bm->mgmtData)->next=(frameMetadata*) malloc(sizeof(frameMetadata));       //Allocate Space for the next Page
((bufferPoolStat *)bm->mgmtData)->LeastRecentlyUsed=(int *)malloc(sizeof(int)*bm->numPages);//Allocate Space for LRU Page Replacement frame positions
for (counter=0;counter<(bm->numPages);counter++)                                           //Iterate through all pages of pool
{
((bufferPoolStat *)bm->mgmtData)->LeastRecentlyUsed[counter]=counter;//Assign the frame numbers to the pages
}
((bufferPoolStat *)bm->mgmtData)->rear=-1;                         //intitally rear is -1
looper=((bufferPoolStat *)bm->mgmtData)->next;		          //Point to the first page of the pool                   
for (counter=0;counter<numPages;counter++)                       //Iterate through all pages of pool
{	
looper->pageNum=NO_PAGE;                                       //Intial representation for a from through no_page=-1                                      
looper->fixCounts=0;                                            //Represents page not requested yet, since pool is intitalized
looper->dirtyFlag=false;
looper->pageFrame= (char *) malloc(PAGE_SIZE);  	      //Allocate space for the Page to be pointed to 
looper->next=(frameMetadata*) malloc(sizeof(frameMetadata)); //Create space for the new page to be created                  
looper=looper->next;                                        //advance looper to the next page.
}       
openPageFile(bm->pageFile,&(((bufferPoolStat *)bm->mgmtData)->fh)); //Open the File in context of the bufferpool   
return RC_OK;
}
/*------------------------------shutdownBufferPool-------------------------------------------
Function: 	shutdownBufferPool                                               
Purpose:  	to destroy the bufferpool created for file and free all associated resources
Parameters: 	IN =  bm :Buffer Pool of a file in context	
	    	OUT = RC_OK    :after successfull completion         		   
Returns:    	RC_OK: completion status of ShutDown of BufferPool                        
         		                                                        
--------------------------------------------------------------------------------------------*/

RC shutdownBufferPool(BM_BufferPool *const bm)
{
forceFlushPool(bm);                                  //write back all dirty pages into the disk.
looper=((bufferPoolStat *)bm->mgmtData)->next;      //Point to the first page of pool.
flag=0;
for(counter=0;counter<bm->numPages;counter++)     //iterate through all pages
{
if(looper->fixCounts!=0)                        //Check if a client is holding the page
{
flag=1;	                                      //Set flag for it.
}
looper=looper->next;                        // Set looper to next page
}
if(flag==1)                                // If flag is not 0
return RC_POOL_IN_USE;                    // Client has a hold over a page.
else
{
closePageFile(&(((bufferPoolStat *)bm->mgmtData)->fh)); //Close the file referenced by the pool
looper=((bufferPoolStat *)bm->mgmtData)->next;         //set looper to first page
for(flag=0;flag<bm->numPages;flag++)	              	
{
for(counter=0;counter<(bm->numPages-flag)-1;counter++)
{	
looper=looper->next;                                //iterate over through pages
}
free(looper->pageFrame);                          //First free the pageFrame pointer to the page's data
free(looper->next);                              // Break the link between the next page.
looper=((bufferPoolStat *)bm->mgmtData)->next;  // point over to the first page.
}
free(((bufferPoolStat *)bm->mgmtData)->next);               //Free the pointer for the link between the pages
free(((bufferPoolStat *)bm->mgmtData)->oFrameContents);    //free the array containig the page numbers of page
free(((bufferPoolStat *)bm->mgmtData)->oDirtyFlags);      //Free the array containing the Dirty status of pages
free(((bufferPoolStat *)bm->mgmtData)->oFixCounts);	 //Free the array containing the fix counts of pages
free(((bufferPoolStat *)bm->mgmtData)->oReadIO);	//Free the array containing the read count for the file
free(((bufferPoolStat *)bm->mgmtData)->oWriteIO);       //Free the array containing the write count for the file
free(((bufferPoolStat *)bm->mgmtData)->LeastRecentlyUsed);//Free the pointer for LRU Pages.
free(bm->mgmtData);                                      // Free the link for the Bufferpool and the Pages
free(bm->pageFile);
return RC_OK;                       			//Return the success status.
}
}
/*------------------------------forceFlushPool--------------------------------------------------------
Function: 	forceFlushPool                                               
Purpose:  	to cause all dirty pages (with fix count 0) from the buffer pool to be written to disk.
Parameters: 	IN =  bm :Buffer Pool of a file in context	
	    	OUT = RC_OK :after successfull completion         		   
Returns:    	RC_OK: completion status of ShutDown of BufferPool                        
         		                                                        
------------------------------------------------------------------------------------------------------*/
RC forceFlushPool(BM_BufferPool *const bm)
{	
looper=((bufferPoolStat *)bm->mgmtData)->next;                                              //set looper to first page
for(counter=0;counter<bm->numPages;counter++)						   //iterate over through pages
{
if(looper->dirtyFlag==true & looper->fixCounts==0)					 //if a page is dirty and not held by any client
{
ensureCapacity(looper->pageNum+1,&(((bufferPoolStat *)bm->mgmtData)->fh));             //ensure for the capacity for specified no of pages.
writeBlock(looper->pageNum,&(((bufferPoolStat *)bm->mgmtData)->fh),looper->pageFrame);//Write the page that is dirty back to the disk
(*(((bufferPoolStat *)bm->mgmtData)->oWriteIO))++;                                   //Increase the write count of the file
looper->dirtyFlag=false;                                                            //Page written back so no more is dirty
}
looper=looper->next;                                                              //Iterate through all pages similarly
}
return RC_OK;                                                                   //Return successfull completion status
}

/*------------------------------fLeastRecentlyUsed--------------------------------------------------------
Function: 	fLeastRecentlyUsed                                               
Purpose:  	to return the first element from the LRU Array and Updating accordingly
Parameters: 	IN =  bm :Buffer Pool of a file in context	
	    	OUT = first :integer that represents the first element in the LRU array        		   
Returns:    	first: first element in the LRU Array                        
         		                                                        
------------------------------------------------------------------------------------------------------*/

int fLeastRecentlyUsed(BM_BufferPool *const bm)
{	
int first=((bufferPoolStat *)bm->mgmtData)->LeastRecentlyUsed[0];            //Set the least recently used array for the first page.
for(counter=0;counter<(bm->numPages-1);counter++)                           //Iterate through all the pages 
{
((bufferPoolStat *)bm->mgmtData)->LeastRecentlyUsed[counter]=((bufferPoolStat *)bm->mgmtData)->LeastRecentlyUsed[counter+1]; 
}
((bufferPoolStat *)bm->mgmtData)->LeastRecentlyUsed[bm->numPages-1]=first;         //Implementing the replacing logic  
return first;	                                                                  //Return the page 
}
/*------------------------------updateLRUArray--------------------------------------------------------
Function: 	updateLRUArray                                               
Purpose:  	Uodates the Lru Array based on the availability of the Pages in the Buffer pool itself
Parameters: 	IN =  bm :Buffer Pool of a file in context
                IN =  position : Pages Positon in context	
	    	OUT = void       		   
Returns:    	Void                      
         		                                                        
------------------------------------------------------------------------------------------------------*/

void updateLRUArray(BM_BufferPool *const bm, int position)
{
counter=-1;                                                                         //Loop to advance within pages.
do
{
 counter++;                                                                      
}
while(((bufferPoolStat *)bm->mgmtData)->LeastRecentlyUsed[counter]!=position);     //Iterate till the last page of the pool
while(counter<(bm->numPages-1)){
((bufferPoolStat *)bm->mgmtData)->LeastRecentlyUsed[counter]=((bufferPoolStat *)bm->mgmtData)->LeastRecentlyUsed[counter+1];// advance the position
counter++;
};
((bufferPoolStat *)bm->mgmtData)->LeastRecentlyUsed[bm->numPages-1]=position; //Set the LeastRecently used array with the position 
}
/*------------------------------pinPage----------------------------------------
Function:   pinPage                                               
Purpose:    to pin the client requested page by a particular pagenum of a file
Parameters: IN =  bm :Buffer Pool of a file in context	
	    IN = page : Instance of the page handle	
            IN = pageNum: Page number of the page to be pinned 
	    OUT = RC_OK    :after successfull completion of pinning a page.         		   
Returns:    RC_OK: completion status of pinning a page.  
            RC_ALL_POOL_PAGE_OCCUPIED: If all pages are held by client                     
         		                                                        
------------------------------------------------------------------------------*/
RC pinPage (BM_BufferPool *const bm, BM_PageHandle *const page, 
	    const PageNumber pageNum) 
{
int position;
looper=((bufferPoolStat *)bm->mgmtData)->next;               //set looper to first page
flag=0;
for(counter=0;counter<bm->numPages;counter++)              //iterate over through all pages
{  
if(looper->pageNum==pageNum)                             //if the current page has the required page number , i.e found in pool only
{
looper->fixCounts=looper->fixCounts+1;                 // then increase the page's fixcounts by 1
page->pageNum=looper->pageNum;                        //set currents page pagenumber with the new page's pagenumber                    
page->data=looper->pageFrame;                        //Change the data of the page to the new pages data.
flag=1;                                             // flag to indicate page found in the buffer pool 
updateLRUArray(bm,counter);                        //The LRU array would be updated after the new page replacement
break;
}
looper=looper->next;                            //iterate through all pages with above conditions                        
}
if(flag==0)                                   	         	   //if page is not in the buffer pool then
{
switch (bm->strategy)                      		 	 // Determine the Replacement strategy
{
case RS_FIFO:                              		       // IF the strategy is FIFo then
looper=((bufferPoolStat *)bm->mgmtData)->next;                // set looper to the first page
if(++(((bufferPoolStat *)bm->mgmtData)->rear)<bm->numPages)  // iterate till rear is less than the total pages in the pool 
{	
for(counter=0;counter<(((bufferPoolStat *)bm->mgmtData)->rear);counter++)
{
looper=looper->next;                                      //advance the looper to the next page
}
if(looper->fixCounts==0 & looper->dirtyFlag==false)      //If the page is not occupied by client and not dirty then start replacing
{
looper->pageNum=pageNum;                                //set page number of the page to new pages pagenum
looper->fixCounts=looper->fixCounts+1;                 //increase the fixcount of the page
page->pageNum=pageNum;                                //new page's pagenumber will be updated to the bufferpool
readBlock(pageNum,&(((bufferPoolStat *)bm->mgmtData)->fh),looper->pageFrame); //read the pagenumber from the disk and point it to by Pageframe 
(*(((bufferPoolStat *)bm->mgmtData)->oReadIO))++;     			     //increament the read count by 1
page->data=looper->pageFrame;                                               //point to the page's data from the current bufferpools Pageframe. 
}
else if(looper->fixCounts==0 & looper->dirtyFlag==true)                    // IF the page is not held by any client and not modified
{
ensureCapacity((looper->pageNum)+1,&(((bufferPoolStat *)bm->mgmtData)->fh));  // first ensure for the total pages availability through ensurecapacity
writeBlock (looper->pageNum,&(((bufferPoolStat *)bm->mgmtData)->fh),looper->pageFrame);// then write back the latest modified data into the disk.
(*(((bufferPoolStat *)bm->mgmtData)->oWriteIO))++;                                    //increase the write count of the file's page
looper->pageNum=pageNum;                                                             //Set pagenumber of the BufferPools Page to pinned page's pagenumber
looper->fixCounts=looper->fixCounts+1;                                              //increase fixcount as the page is currently requested.
page->pageNum=pageNum;                                                             //set pagenumber of the page
readBlock(pageNum,&(((bufferPoolStat *)bm->mgmtData)->fh),looper->pageFrame);     //Read the Page from the disk
(*(((bufferPoolStat *)bm->mgmtData)->oReadIO))++;                                //increase the read count
page->data=looper->pageFrame;                                                   //set the data field to the bufferpools's pageframe
}
else
{
if(--(((bufferPoolStat *)bm->mgmtData)->LeastRecentlyUsed[bm->numPages-1])>=0)  //Checking the initial base case checkup for pinning the page
pinPage(bm,page,pageNum);                                                      //Recurse for above condition completion
else
{ 
((bufferPoolStat *)bm->mgmtData)->LeastRecentlyUsed[bm->numPages-1]=bm->numPages-1;// Condition to ensure no page is currently free to be replaced.
return RC_ALL_POOL_PAGE_OCCUPIED;                                                 //return the error status.                                   
}
}	
}
else
{
if(--(((bufferPoolStat *)bm->mgmtData)->LeastRecentlyUsed[bm->numPages-1])>=0)//Checking the initial base case checkup for pinning the page
{
(((bufferPoolStat *)bm->mgmtData)->rear)=-1;
pinPage(bm,page,pageNum);                                                   //Recurse for above condition completion                 
}
else
{ 
((bufferPoolStat *)bm->mgmtData)->LeastRecentlyUsed[bm->numPages-1]=bm->numPages-1; //  Condition to ensure no page is currently free to be replaced.
return RC_ALL_POOL_PAGE_OCCUPIED;                                                  //return the error status.
}
}
(((bufferPoolStat *)bm->mgmtData)->LeastRecentlyUsed[bm->numPages-1])=bm->numPages-1; 
return RC_OK;                                                                    	  //Return Successfull completion
break;
case RS_LRU:                                                                		 //if Strategy for replacement is LRU
position=
fLeastRecentlyUsed(bm);
looper=((bufferPoolStat *)bm->mgmtData)->next;                              		//Point over to the first page
for(counter=0;counter<position;counter++)                                              //Iterate over through all pages. 
{
looper=looper->next;
}
if(looper->fixCounts==0 & looper->dirtyFlag==false)                                 //if the page is not held by client and unmodifed then start replacing 
{
looper->pageNum=pageNum;                                                           //set the pool page's pagenumber            
looper->fixCounts=looper->fixCounts+1;
page->pageNum=pageNum;                                             		 //set the pagenumber of the page handle
readBlock(pageNum,&(((bufferPoolStat *)bm->mgmtData)->fh),looper->pageFrame);   //read the page from the disk
(*(((bufferPoolStat *)bm->mgmtData)->oReadIO))++;                              // increament the read count of the page
page->data=looper->pageFrame;                                                 //Update the page data in the bufferpool
} 
else if(looper->fixCounts==0 & looper->dirtyFlag==true)                                   //IF the page is not client held but modified
{
ensureCapacity((looper->pageNum)+1,&(((bufferPoolStat *)bm->mgmtData)->fh));            //Ensure capacity for total number of pages before pinning
writeBlock(looper->pageNum,&(((bufferPoolStat *)bm->mgmtData)->fh),looper->pageFrame); //Write back the modified page back into the disk
(*(((bufferPoolStat *)bm->mgmtData)->oWriteIO))++;                                    //increase the write count of the page
looper->pageNum=pageNum;                                                             //Set the pagenumber for the current page of bufferpool
looper->fixCounts=looper->fixCounts+1;                                              //increase fixcount since it is currently requested page
page->pageNum=pageNum;                                                             // Set the pagenumber of the pageHandle
readBlock(pageNum,&(((bufferPoolStat *)bm->mgmtData)->fh),looper->pageFrame);     //read from the disk the above pagenumber
(*(((bufferPoolStat *)bm->mgmtData)->oReadIO))++;                                //Increament the readcount 
page->data=looper->pageFrame;                                                   //Pagehandle data will be the bufferpools data of the current page 
}
else
{
if((((bufferPoolStat *)bm->mgmtData)->rear)++<bm->numPages)                 //Iterate through all the pages of the Buffer pool through Recursion
pinPage(bm,page,pageNum);                                                  // try pinning page if base condition is not reached
else
{ 
(((bufferPoolStat *)bm->mgmtData)->rear)=-1;                           //Set back the rear to -1 ,  since no page is free to be replaced 
return RC_ALL_POOL_PAGE_OCCUPIED;
}
}
(((bufferPoolStat *)bm->mgmtData)->rear)=-1;                       //Rear set to initial condition.
return RC_OK;                                                     //Return with successfull completion status
break;
default: break;                                                 // Break when no strategy specified
}
} 
else {
return RC_OK;                                             //Return with successfull completion status
}
}
/*------------------------------markDirty----------------------------------------
Function:   markDirty                                               
Purpose:    to Mark the requested page of a file as dirty(Page needs writeback)
Parameters: IN =  bm :Buffer Pool of a file in context	
	    IN = page : Instance of the page handle	
            OUT = RC_OK :after successfull completion of marking a page as dirty.         		   
Returns:    RC_OK: completion status of marking a page as dirty.                        
         		                                                        
------------------------------------------------------------------------------*/
RC markDirty (BM_BufferPool *const bm, BM_PageHandle *const page)
{	
looper=((bufferPoolStat *)bm->mgmtData)->next;//set looper pointer to first page OF Bufferpool.
for(counter=0;counter<bm->numPages;counter++)//Iterate through all pages of BufferPool
{
if(page->pageNum==looper->pageNum)//Check through pagenumber for the page to be marked dirty
{
looper->dirtyFlag=true;      //Set DirtyFlag as true to indicate file write back needed
}
looper=looper->next;	   //Advance looper pointer to the next page of pool
}
return RC_OK;		//return with success status after a page is dirty
}
/*------------------------------unpinPage----------------------------------------
Function:   unpinPage                                               
Purpose:    to unpin the requested page of a file
Parameters: IN =  bm :Buffer Pool of a file in context	
	    IN = page : Instance of the page handle	
            OUT = RC_OK    :after successfull completion of unpinning a page.         		   
Returns:    RC_OK: completion status of unpinning a page.                        
         		                                                        
--------------------------------------------------------------------------------*/
RC unpinPage (BM_BufferPool *const bm, BM_PageHandle *const page)
{
looper=((bufferPoolStat *)bm->mgmtData)->next;	//set looper pointer to first page OF Bufferpool.
for(counter=0;counter<bm->numPages;counter++)//Iterate through all pages of BufferPool
{	
if(page->pageNum==looper->pageNum)//Check through pagenumber for the page to be Unpinned
{
looper->fixCounts=looper->fixCounts-1;//Decrease fixCount by 1
break;//Page unPinned so breaking loop
}
looper=looper->next;//Advance looper pointer to the next page of pool
}
switch(bm->strategy)
{ 
case RS_LRU:                 // IF the LRU Strategy is used
if(looper->fixCounts==0)    //If the page is not held by any client then 
updateLRUArray(bm,counter);// The LRu Pages would be updated.
break;
default:break;           //break when no strategy
}
return RC_OK;  	       //Return succesfull completion status
}
/*------------------------------forcePage----------------------------------------
Function:   forcePage                                               
Purpose:    to write the current content of the page back to the page file on disk.
Parameters: IN =  bm :Buffer Pool of a file in context	
	    IN = page : Instance of the page handle	
            OUT = RC_OK    :after successfull completion of a page writeBack.         		   
Returns:    RC_OK: completion status of a page writeBack.                     
         		                                                        
--------------------------------------------------------------------------------*/
RC forcePage (BM_BufferPool *const bm, BM_PageHandle *const page)
{
looper=((bufferPoolStat *)bm->mgmtData)->next; 					            //set looper pointer to first page.
for(counter=0;counter<bm->numPages;counter++)						   //Iterate through all pages of BufferPool
{
if(page->pageNum==looper->pageNum)                                                       //Check through pagenumber for the page to be written
{
ensureCapacity((looper->pageNum)+1,&(((bufferPoolStat *)bm->mgmtData)->fh));	       //ensuring the requested page's existence in file.
writeBlock(looper->pageNum,&(((bufferPoolStat *)bm->mgmtData)->fh),looper->pageFrame);// Write latest content to the page in disk
(*(((bufferPoolStat *)bm->mgmtData)->oWriteIO))++; //increase the write count of the page
looper->dirtyFlag=false;			  //Page is no more dirty.
}
looper=looper->next;				//Advance looper pointer to the next page of pool
}
return RC_OK;				      //Return Success status
}
/*------------------------------getFrameContents----------------------------------------
Function:   getFrameContents                                               
Purpose:    returns an array of PageNumbers (of size numPages) where the ith element 
	    is the number of the page stored in the ith page frame
Parameters: IN =  bm :Buffer Pool of a file in context	
	    OUT = an array of PageNumbers       		   
Returns:    returns an array of PageNumbers 
         		                                                        
---------------------------------------------------------------------------------------*/

PageNumber *getFrameContents (BM_BufferPool *const bm)
{
looper=((bufferPoolStat *)bm->mgmtData)->next;				     //set looper pointer to first page.
for(counter=0;counter<bm->numPages;counter++)				    //Iterate through all pages of BufferPool
{
((bufferPoolStat *)bm->mgmtData)->oFrameContents[counter]=looper->pageNum;//Assign Page's pagenumber as the array element
looper=looper->next;					          //Advance looper pointer to the next page of pool
}
return ((bufferPoolStat *)bm->mgmtData)->oFrameContents;	//Return array containing the Page numbers
}
/*------------------------------getDirtyFlags-------------------------------------------
Function:   getDirtyFlags                                               
Purpose:    returns an array that indicate the edited status of a page in bufferpool
Parameters: IN =  bm :Buffer Pool of a file in context	
	    OUT = an array to determine edited status of pages       		   
Returns:    returns an array of Page edited status 
         		                                                        
---------------------------------------------------------------------------------------*/ 
bool *getDirtyFlags (BM_BufferPool *const bm)
{
looper=((bufferPoolStat *)bm->mgmtData)->next;                               //set looper pointer to first page OF Bufferpool.
for(counter=0;counter<bm->numPages;counter++)				    //Iterate through all pages of BufferPool
{
((bufferPoolStat *)bm->mgmtData)->oDirtyFlags[counter]=looper->dirtyFlag;//get dirtyflag status of current page in current array element 
looper=looper->next;				        //Advance looper pointer to the next page of pool
}
return ((bufferPoolStat *)bm->mgmtData)->oDirtyFlags;//returns the array of dirtyflag status for the pages of the file in the BufferPool.
}
/*------------------------------getFixCounts-------------------------------------------
Function:   getFixCounts                                               
Purpose:    returns an array that indicate the Fixcount of all page in bufferpool
Parameters: IN =  bm :Buffer Pool of a file in context	
	    OUT = an array to determine Fixcount of all page in bufferpool       		   
Returns:    returns an array containing Fixcount of all page in bufferpool
         		                                                        
---------------------------------------------------------------------------------------*/

int *getFixCounts (BM_BufferPool *const bm)
{
looper=((bufferPoolStat *)bm->mgmtData)->next;				      //Point looper to the first page     
for(counter=0;counter<bm->numPages;counter++)				     //Iterate through all pages of BufferPool
{
((bufferPoolStat *)bm->mgmtData)->oFixCounts[counter]=looper->fixCounts;   //Assign the current Pages fixcount to current array element 
looper=looper->next;                                                      //Move to NextPage    
}
return ((bufferPoolStat *)bm->mgmtData)->oFixCounts;			//returns the array of fixcounts for the pages of the file in the BufferPool.
}
/*------------------------------getNumReadIO-------------------------------------------
Function:   getNumReadIO                                               
Purpose:    to determine the disk operation count through read operation of a page
Parameters: IN =  bm :Buffer Pool of a file in context	
	    OUT = an array to determine readcount since bufferpool is initialized       		   
Returns:    returns an array containing readcount since bufferpool is initialized  
         		                                                        
---------------------------------------------------------------------------------------*/

int getNumReadIO (BM_BufferPool *const bm)
{
return *(((bufferPoolStat *)bm->mgmtData)->oReadIO);		//returns disk operation count through read operation of a page
}
/*------------------------------getNumWriteIO-----------------------------------------------
Function:   getNumWriteIO                                               
Purpose:    to determine the disk operation count through write operation on a page on disk
Parameters: IN =  bm :Buffer Pool of a file in context	
	    OUT = an array with readcount since bufferpool is initialized       		   
Returns:    returns an array containing count through write operation on a page on disk
            since bufferpool is initialized  
         		                                                        
-------------------------------------------------------------------------------------------*/
int getNumWriteIO (BM_BufferPool *const bm)
{
return *(((bufferPoolStat *)bm->mgmtData)->oWriteIO);		//returns disk operation count through write operation of a page
}
