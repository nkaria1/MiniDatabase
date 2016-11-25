/*---------------------------------------------------------------------------  
    fileName:         storage_mgr.c                                             
    Authors:          Niharika Karia, Shiva Naveen Ravi, Shiv Ram Krishna       
    Purpose:          To implement the functions of a storage manager.    	     
    Language:         C                                                         
    Created On:       August 27,2015                                           
    Last Modified On: September 11, 2015                                       
  -------------------------------------------------------------------------*/


/*Headers Declaration*/

#include <fcntl.h>
#include <sys/types.h>
#include <errno.h>
#define _GNU_SOURCE
#include <stdlib.h>
#include "storage_mgr.h"                     // Custom header for storage manger
#include "dberror.h"                        //Custom header for validation

/*Global Variables Declaration*/
 
int input_f;                             //Global Filehandler for the file in Context 
int metadataSize=sizeof(SM_FileHandle); //Global size of metadata for for the file in Context
char *buffer;                          //Global buffer for Read/Write based data transfer


       			 /*------------------------------initStorageManager------------------------------
         		  Function:  initStorageManager                                               
       			  						                   		                                 	
   		          Purpose:   This function is used to dynamically allocate and initialize the 
       		                 the allocated memory with '\0' with size =Page_size(4096bytes) 
         		              									                              
         		  Parameters: No parameter                                                    
                  IN =        void (no parameters)                            
           		              OUT = void (no parameters)                
         		  Returns:    void (nothing)                         
         		                                                        
         		  ------------------------------------------------------------------------------*/

extern void initStorageManager ()
{	
}

                 /*------------------------------createPageFile---------------------------------
         		    Function:   createPageFile                                                 
       			  						                   	                                   	
          		    Purpose:    This function is used to set the metadata of a newly created   
         		                file and keep them in the first few bytes of the file.The rest   
       		              	    bytes are set with the allocated memory from buffer           
         		    Parameters: IN =  filename :name of the file in context		             
                                OUT = RC_OK    :after successfull completion         		   
         		    Returns:  	RC_OK: completion status of file page creation                
         		                                                                                
         		  -----------------------------------------------------------------------------*/


extern RC createPageFile (char *fileName)
{	int i;
	SM_FileHandle fh;                                     //declaring a file handler instance                                     
	fh.fileName=NULL;			                         //filename of newly created file would be NULL		
	fh.totalNumPages=1;                                 //total no of pages of a newly created file would be 1  
	fh.curPagePos=0;				                   //current position of newly created file would be 0
	fh.mgmtInfo=NULL;				                  //management information of a newly created file would be NULL
	buffer = (char *) malloc(PAGE_SIZE);           //dynamically allocating memory
	for (i=0;i<PAGE_SIZE;i++)                     //for each byte in allocated memory till a page size
        buffer[i]='\0'; 
	input_f=open(fileName,O_RDWR | O_CREAT);	     //open the file in context
	write(input_f,&fh,metadataSize);	            //write metadata into first few bytes of the file
	write(input_f,buffer,PAGE_SIZE);               //rest bytes are set with the buffer as in initStorageManager function
	close(input_f);                               //close the file handler
	free(buffer);
	return RC_OK;                                //return with successfull completion status
}

                          /*------------------------------openPageFile-----------------------------------
         		             Function:   openPageFile                                                   
       			             Purpose:    This function aims to open a file containing pages             
         		             Parameters: IN =filename :name of the file in context		                 
           		                         IN =fHandle  :filehandle of the file in context		         
			               		         OUT=RC_OK    :after successfull opening a page file            
                                         OUT=RC_FILE_NOT_FOUND:after an error occurs in opening file.   
         		             Returns:    RC_OK: completion status of opening a page of file             
         		                                                                                        
         		           -----------------------------------------------------------------------------*/


extern RC openPageFile (char *fileName,SM_FileHandle *fHandle)
{				
	char *path=realpath(fileName,NULL);		                   //fetch the path of the file in context	
	chmod(path,0644);                                         //grant sufficient permission for the current user	
	input_f=open(path,O_RDWR);                               //open the file in fetched path 
	if(input_f==-1)	                                        //if file handle returns -1 it means
	{
	free(path);	
	return RC_FILE_NOT_FOUND;	                         //file does not exists in disk
	}
	else                                               // if file exists
        {
	read(input_f,fHandle,metadataSize);             //read metadata and advance pointer after it. 
	fHandle->fileName=fileName;           	      //set filename to the current file handler
	fHandle->mgmtInfo=malloc(sizeof(int));
	*((int *)fHandle->mgmtInfo)=input_f;	   // set fd to mgmtInfo 
	free(path);
	return RC_OK;                      		 //return with successfull completion status
	}
}

                 /*------------------------------closePageFile-----------------------------------
         		    Function:   closePageFile                                                   
       			  						                   	                                     	
           		    Purpose:    This function aims to close an open page file                   
         		    Parameters: IN =filename :name of the file in context		                 
           		                		 						                                 
	                            OUT=RC_OK  :after successfull completion of closing a file      
                                OUT=RC_FILE_ALREADY_CLOSED: a file is closed already             		         
       		                         							                             
         		    Returns:  	RC_OK: completion status of closing a page of file              
         		                                                                                 
         		  --------------------------------------------------------------------------------*/                      

extern RC closePageFile (SM_FileHandle *fHandle)
{	
	if(*((int *)fHandle->mgmtInfo)==-1)		            //check file handle value for -1,if yes
	{ 	                                               //then 
  	return RC_FILE_ALREADY_CLOSED;                    //file is already closed
	}
        else                                        // else
	{
	lseek(*((int *)fHandle->mgmtInfo),0,SEEK_SET);             //traverse to start of file
	write(*((int *)fHandle->mgmtInfo),fHandle,metadataSize);  //traverse and set into the file's metadata area
	close(*((int *)fHandle->mgmtInfo));                      //close the file handler
	free(fHandle->mgmtInfo);
	return RC_OK;			                                //return completion status of closing a page of file	
        }
}

                 /*------------------------------destroyPageFile-----------------------------------
         		    Function:   destroyPageFile                                                   
       			  						                   	                                       	
            	    Purpose:    This function aims to destroy a closed page file                  
         		    Parameters: IN =filename :name of the file in context		                   
           		                		 						                                   
	          	                OUT=RC_OK:after successfull completion of destroying a file       
                                OUT=RC_FILE_DOES_NOT_EXIST: a file is not in disk to be destroyed  		         
        		                         							                               
         		    Returns:  	RC_OK: completion status of destroying a page file                
         		                                                                                   
         		  ---------------------------------------------------------------------------------*/ 

extern RC destroyPageFile (char *fileName)
{
	return (remove(fileName)!=-1)? RC_OK:RC_FILE_DOES_NOT_EXIST; //delete if file exists in disk,or return status of file not existing in disk.   
}

                 /*------------------------------readBlock-------------------------------------------------
   		            Function:   readBlock                                                                 
       			  						                                   	                               	 
          		    Purpose:    This function aims to read one specified block from file into the memory  
         		    Parameters: IN =pageNum : specified page to read from file                            
           		                IN =fHandle : filehandle of referenced file                               
           	         	        IN =memPage : The Page in memory                			               
			  	  	            OUT=RC_OK   :after successfull read of one block                          
                                OUT=RC_READ_NON_EXISTING_PAGE: page not within scope of file       	    		         	 											 |
        		                         							    	                               
         		    Returns:    RC_OK: completion status of reading a page of file                 	       
         		                                                                                   	    
         		  ----------------------------------------------------------------------------------------*/  

extern RC readBlock (int pageNum, SM_FileHandle *fHandle, SM_PageHandle memPage)
{ 
	if (fHandle->totalNumPages <= pageNum)                 		                           //check if pageNum is within scope of file in context
		{                                              		                              //then
		return RC_READ_NON_EXISTING_PAGE;             	                                 //trying to read a non existing page
		}					     		                                                //else
	else 
		{
		lseek(*((int *)fHandle->mgmtInfo),(pageNum*PAGE_SIZE)+metadataSize,SEEK_SET);//traverse to the byte location where read is required
		read(*((int *)fHandle->mgmtInfo),memPage,PAGE_SIZE);                        //read the file into the memory
		fHandle->curPagePos=pageNum+1;			                                   //byte location reaches next page
		return RC_OK;                                                             //return with completion status of reading a page of file
		}	
		
}

                 /*--------------------------------------getBlockPos--------------------------------
         		    Function:    getBlockPos                                                        
       			  						                                   	                        	
           		    Purpose:     This function aims to get the current position of the block in file
         		    Parameters:  IN =fHandle : filehandle of referenced file  		    	        
         		    Returns:  	 void : return the postition of current block of a file. 	        
         		                                                                                     
         		  -----------------------------------------------------------------------------------*/

extern int getBlockPos (SM_FileHandle *fHandle)
{	
	return fHandle->curPagePos;		//return the file metadata's current position for the current block position
	
}
 
                 /*------------------------------setPagePos-------------------------------------------------
         		    Function:   setPagePos                                                                 
       			  						                                   	                                	
           		    Purpose:    This function aims to set the current position to a specified block in file
         		    Parameters: IN =pageNum : specified page position to be set into file                  
           		                IN =fHandle : filehandle of referenced file  				                
		      	                OUT=RC_OK   : after successfull set of current block position              
                                OUT=RC_PAGE_DOES_NOT_EXIST: page not within scope of file       	         									    	
        		                         							    	                                
         		    Returns:  	RC_OK: completion status of setting of current block position              
         		  -----------------------------------------------------------------------------------------*/


extern RC setPagePos (int pageNum, SM_FileHandle *fHandle)
{
	if (fHandle->totalNumPages < pageNum)               //check if the specified block is within the scope of file
		{      					                       //then 
		return RC_PAGE_DOES_NOT_EXIST;                //return with page not existing status
		}
	else 						                  // else
		{
		fHandle->curPagePos=pageNum;	       //set file metadata's current position to the specified page number
		return RC_OK; 			              //return with completion status of set of current block position
		}
	
}

			     /*--------------------------readFirstBlock-------------------------------------------
         		    Function:   readFirstBlock                                                       
       			  						                                                              	
           		    Purpose:    This function aims to read the first block from file into the memory 
         		    Parameters: IN =fHandle : filehandle of referenced file 				           
           		                IN =memPage : The Page in memory                              	      
           		  		                 			 			     	                              
		                	    OUT=RC_OK   : after successfull read of first block                  
                                OUT=RC_READ_NON_EXISTING_PAGE: page not within scope of file          												     
        		                         							    	                          
         		    Returns:  	RC_OK: completion status of  read of first block               	  
         		                                                                                       
         		  -----------------------------------------------------------------------------------*/

extern RC readFirstBlock (SM_FileHandle *fHandle, SM_PageHandle memPage)
{			
		return readBlock(0,fHandle,memPage); //read the 0th block of file
}

                 /*------------------------------readPreviousBlock-----------------------------------------------
         		    Function:   readPreviousBlock                                                               
       			  						                                                                         	
           		    Purpose:    This function aims to read a block previous to current block position       	 
         		    Parameters: IN =fHandle : filehandle of referenced file 				      	             
           		                IN =memPage : The Page in memory                              	             	 
          		  		                 			 			     	     	                                 
		         	            OUT=RC_OK   :after successfull read of previous block                           
                                OUT=RC_READ_NON_EXISTING_PAGE: page not within scope of file       	       	  		         	  												 |
        		                         							    	                                     
         		    Returns:  	RC_OK: completion status of read of block previous to current block.            
         		                                                                                   	           
         		  ----------------------------------------------------------------------------------------------*/



extern RC readPreviousBlock (SM_FileHandle *fHandle, SM_PageHandle memPage)
{
	if (fHandle->totalNumPages < 2 || fHandle->curPagePos<1)         //check if read is done on valid scope of file
		{                                                           //then
		return RC_READ_NON_EXISTING_PAGE;                          //return with page not existing status 
		}
	else                        					  //else
		{
		lseek(*((int *)fHandle->mgmtInfo),((fHandle->curPagePos-1)*PAGE_SIZE)+metadataSize,SEEK_SET);//traverse to previous block relative to file beginning
		read(*((int *)fHandle->mgmtInfo),memPage,PAGE_SIZE);				                       	//read the block into the memory
		return RC_OK; 							                                                   //return with completion status of read of previous block
		}
}

                 /*------------------------------readCurrentBlock------------------------------
         		    Function:   readCurrentBlock                                              
       			  						                                                      	
           		    Purpose:    This function aims to read current block in file     		  
         		    Parameters: IN =fHandle : filehandle of referenced file 				  
           		                IN =memPage : The Page in memory                              
           		  		                 			 			     	                      
			                    OUT=RC_OK   :after successfull read of current block          
                                OUT=RC_READ_NON_EXISTING_PAGE: page not within scope of file 
        		                         							    	                   
         		    Returns:  	RC_OK: completion status of read of current block             
         		  ----------------------------------------------------------------------------*/

extern RC readCurrentBlock (SM_FileHandle *fHandle, SM_PageHandle memPage)
{
	return readBlock(fHandle->curPagePos,fHandle,memPage); //read block pointed by current block position of file
}

		         /*---------------------------readNextBlock------------------------------------------------
         		    Function:   readNextBlock                                                             
       			  						                                                                   	
           		    Purpose:    This function aims to read next block relative to current position in file
         		    Parameters: IN =fHandle : filehandle of referenced file 				               
           		                IN =memPage : The Page in memory                              	           
           		  		                 			 			     	                                   
		  	            	    OUT=RC_OK   :after successfull read of next block                         
                                OUT=RC_READ_NON_EXISTING_PAGE: page not within scope of file       	    		         	 											    |
        		                         							    	                               
         		    Returns:  	RC_OK: completion status of read of next block                     	   
         		                                                                                   	     
         		  ----------------------------------------------------------------------------------------*/


extern RC readNextBlock (SM_FileHandle *fHandle, SM_PageHandle memPage)
{
	if(fHandle->curPagePos > (fHandle->totalNumPages-2))    //check if read is done on valid scope of file
		{                                                  //then  
		return RC_READ_NON_EXISTING_PAGE;                 //return with page not existing status
		}
	else						                       //else
		{
		return readBlock(fHandle->curPagePos+1,fHandle,memPage); //return the next block from current block's position to be read.
		}
}

		/*------------------------------readLastBlock----------------------------------------------
       	   Function:   readLastBlock                                                                
       	 						                                                                     	
           Purpose:    This function aims to read last block of a file into the memory              
           Parameters: IN =fHandle : filehandle of referenced file 				                 
                       IN =memPage : The Page in memory                              	             
         		                 			 			     	                                     
		 		        OUT=RC_OK   :after successfull read of next block                            
                              	     							     	                                  							    	   		     
           Returns:  	RC_OK: completion status of read of last block                               
                                                                                           	       
  		 -------------------------------------------------------------------------------------------*/			

extern RC readLastBlock (SM_FileHandle *fHandle, SM_PageHandle memPage)
{	
	lseek(*((int *)fHandle->mgmtInfo),PAGE_SIZE,SEEK_END);                     //traverse file where reaad is required
	read(*((int *)fHandle->mgmtInfo),memPage,PAGE_SIZE);                      //start reading the file
	fHandle->curPagePos=fHandle->totalNumPages;                              //set current page position to the total number of pages 
	return RC_OK;
	
}
                 /*------------------------------WriteBlock----------------------------------------------------
         		    Function:   WriteBlock                                                                    
       			  				                                                                               	
           		    Purpose:    One Page data from the memory has to be written into a specified block of file
         		    Parameters: IN =fHandle : filehandle of referenced file 			             	      
           		                IN =memPage : The Page in memory                              	              
       		  	         	    IN =pageNum : The Specified block of file to be written into.       	                        		 		  					       |	     	  			  |						       					       |
			       	            OUT =RC_OK  :after successfull write of specified block                          
                                OUT =RC_WRITE_NON_EXISTING_PAGE: page not within scope of file       	    		        												       | 
        		                OUT =RC_WRITE_FAILED: if write operation fails     			                				   	                     								       |
         		    Returns:  	RC_OK: completion status of write into the specified block of file         		                                                                                   	         
         		  --------------------------------------------------------------------------------------------*/

extern RC writeBlock (int pageNum, SM_FileHandle *fHandle, SM_PageHandle memPage)       
{
	if(pageNum<0||pageNum>=fHandle->totalNumPages)            //if specified pageNum is not within the scope of file 				
        {      					                             //then
         return RC_WRITE_NON_EXISTING_PAGE;                //return with page not existing status
        } 
	else                                                 //else
	{	 
	  lseek(*((int *)fHandle->mgmtInfo),(pageNum*PAGE_SIZE)+metadataSize,SEEK_SET);     //move pointer to the block where write is required
	  if(write(*((int *)fHandle->mgmtInfo),memPage,PAGE_SIZE)==-1)     		           //write a page of data from memory into the block of file,if it fails
          { 							                                              //then
	   return RC_WRITE_FAILED;                                                      //return with write fails status           
	  }
	   else							                                              //else 
	 {
           fHandle->curPagePos=pageNum+1;			                            //increase the file's current page position by 1	    	
	   return RC_OK;
		}
          }  
}

			/*------------------------------writeCurrentBlock----------------------------------------------
      		    Function:   writeCurrentBlock                                                             
   			  				                                                                               	
                Purpose:    One Page data from the memory has to be written into a current block of file  
       		    Parameters: IN =fHandle : filehandle of referenced file 				                   
       		                IN =memPage : The Page in memory                              	               
       		  					       						                                               
	  		                OUT =RC_OK   :after successfull write into current block of file.                 
                            OUT =RC_WRITE_NON_EXISTING_PAGE: page not within scope of file       	        		         	  											       |
      		                OUT =RC_WRITE_FAILED: if write operation fails     			         	     	                     								       |
       		    Returns:  	OUT =RC_OK: completion status of write into the current block of file         
       		  --------------------------------------------------------------------------------------------*/

extern RC writeCurrentBlock (SM_FileHandle *fHandle, SM_PageHandle memPage)
{
	 return writeBlock(fHandle->curPagePos,fHandle,memPage);	//write the block where the file is currently pointed to   
}

                 /*------------------------------appendEmptyBlock-------------------------------------
         		    Function:   appendEmptyBlock                                                     
       			  				                                                                     	
           		    Purpose:    One '\0' initialized block has be appended to a file in context      
                    Parameters: IN =fHandle : filehandle of referenced file 				          
         		  		        OUT =RC_OK   :after successfull append of one block into the file.	               	                          								       |
        		                OUT =RC_WRITE_FAILED: if write operation fails     			       				   	                     								       |
         		    Returns:  	OUT =RC_OK: completion status of one empty block append into the file  
         		  -----------------------------------------------------------------------------------*/

extern RC appendEmptyBlock (SM_FileHandle *fHandle)
{	int i;
	lseek(*((int *)fHandle->mgmtInfo),0,SEEK_END);                                   //traverse to the end of file
	buffer = (char *) malloc(PAGE_SIZE);           //dynamically allocating memory
	for (i=0;i<PAGE_SIZE;i++)                     //for each byte in allocated memory till a page size
        	buffer[i]='\0'; 
	if(write(*((int *)fHandle->mgmtInfo),buffer,PAGE_SIZE)==-1)                     //write into the file, the above 4096 zero initialized bytes,if write fails
	{ free(buffer); 							                                                  //then
          return RC_WRITE_FAILED;                                               //return with write failed error
        } 
        else							                                     //else 
 	  {
           fHandle->totalNumPages++;                                      //after append,increase the total no of blocks of file by 1
	   free(buffer);
	   return RC_OK; 					                                 //return with completion status of one empty block append into the file
	   }
}		


			 /*------------------------------ensureCapacity----------------------------------------
       		    Function:   ensureCapacity                                                        
       		  						                                   	    	                  	
                Purpose:    This function aims to ensure for particular number of pages in a file 
         	    Parameters: IN  =numberOfPages : The minimum positive no of pages that a file      
			  				      should have after this function executes.                       
                            IN  =fHandle : filehandle of referenced file  			              
		  		            OUT =RC_WRITE_FAILED: if write operation fails                       
			  		        OUT =RC_OK   : after successfully ensuring capacity of a file        
               Returns:  	RC_OK: completion status of ensuring capacity of a file              
      		  ------------------------------------------------------------------------------------*/

extern RC ensureCapacity (int numberOfPages, SM_FileHandle *fHandle)
{	
	if(fHandle->totalNumPages>=numberOfPages)                       //if specified no of blocks already exists, then no append needed.
        {
        } 
	else 
	    {						                                // iterate and append a block until the required appends is not comleted
             while(numberOfPages-fHandle->totalNumPages!=0)
                  {
          	  appendEmptyBlock(fHandle);
          	  }         
         }
	return RC_OK;						                    //return with completion status of ensuring capacity of a file
}