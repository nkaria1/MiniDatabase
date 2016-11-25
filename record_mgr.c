/*---------------------------------------------------------------------------
fileName:         record_mgr.c
Authors:          Niharika Karia, Shiva Naveen Ravi, Shiv Ram Krishna
Purpose:          To implement the functions of a Record manager.
Language:         C
Dev OS:           ubuntu 14.04 LTS (Unix Environment)
Reference:	  Course CS525,IIT CHICAGO
Created On:       07 October,2015
Last Modified On: 10 November, 2015 
----------------------------------------------------------------------------*/
/*Headers Declaration*/
#include <stdlib.h>
#include <stdio.h>
#include <string.h>
#include "buffer_mgr.h"
#include "dberror.h"
#include "storage_mgr.h"
#include "record_mgr.h"
#include "tables.h"
#include "expr.h"

typedef struct RM_tableInfo
{	Schema *schema;					   	  // Schema of the table in context
	RID freeSpace;					  	 //Next available free space
	int noOfSlots;					 	//Number of slots in the table file
	int numTuples;					       // Number of records of the table in context.
}RM_tableInfo;

typedef struct RM_scanInfo
{	struct Expr *cal;				//pointer for our expression holder
	RID id;   				       // RID for the qualifying record
}RM_scanInfo;

bool deleteFlag=false;			             //Flag to indicate that a record is deleted.

/*------------------------------initRecordManager------------------------------------------
Function:   initRecordManager                                               
Purpose:    To initialize the Record Manager
Parameters: IN =MgmtData: any information that we want our RecordManager to initialize with        
            OUT = RC_OK    :after successfull completion         		   
Returns:    RC_OK: completion status of Intialization of Recordmanager                        
         		                                                        
------------------------------------------------------------------------------------------*/
extern RC initRecordManager (void *mgmtData)
{	
	return RC_OK;				//Return Success Full completion status
}
/*------------------------------createTable------------------------------------------
Function:   createTable                                               
Purpose:    To Create a table
Parameters: IN =name:   name of the table or file in context
	    IN =schema: schema of the table in context
            OUT = RC_OK    :after successfull completion          		   
Returns:    RC_OK: completion status of creation on table.                        
         		                                                        
------------------------------------------------------------------------------------------*/
extern RC createTable (char *name, Schema *schema)
{	
	createPageFile(name);                                                     // create a page file on disk with table name as file name
		
	BM_BufferPool *bm = MAKE_POOL();                              	        // Allocate space for the buffer pool.
	initBufferPool(bm, name, 3, RS_LRU, NULL);                  	       // initialize the bufferpool

	BM_PageHandle *tableInfoPage = MAKE_PAGE_HANDLE();         	      //create a page for the intial page that holds schema information
	pinPage(bm,tableInfoPage,0);                              	     //pin the 0th page i.e tableinfopage in the  bufferpool
	((RM_tableInfo *)tableInfoPage->data)->schema=schema;    	    //associate the current schema information in the schema page 
	((RM_tableInfo *)tableInfoPage->data)->freeSpace.page=1;	   //initialize  the first freespace's page to be 1  
	((RM_tableInfo *)tableInfoPage->data)->freeSpace.slot=1;	  //initialize the first free slot to be 1.
	((RM_tableInfo *)tableInfoPage->data)->noOfSlots=abs(PAGE_SIZE/(getRecordSize(schema)+sizeof(bool))); //total no of slots in the page
	((RM_tableInfo *)tableInfoPage->data)->numTuples=0;		// initialize the number of records in the file
	markDirty(bm,tableInfoPage);  				       //tableinfopageis modified
	unpinPage(bm,tableInfoPage); 				      //unpin the tableinfopage

	shutdownBufferPool(bm);   			 	    //shut down the buffer pool
	free(bm);                			 	   //free the bufferpool pointer
	free(tableInfoPage);    			 	  //free the 0th page pointer

	return RC_OK;					 	//Return Success Full completion status
}
/*------------------------------openTable------------------------------------------
Function:   openTable                                               
Purpose:    To open a table
Parameters: IN =name:     name of the table or file in context
	    IN =rel:     the relation in context
            OUT = RC_OK: after successfull completion          		   
Returns:    RC_OK: completion status of opening a table.                        
         		                                                        
------------------------------------------------------------------------------------------*/
extern RC openTable (RM_TableData *rel, char *name)
{	
	BM_BufferPool *bm = MAKE_POOL();				 // Allocate space for the buffer pool.
	initBufferPool(bm, name, 3, RS_LRU, NULL);			// initialize the bufferpool
	BM_PageHandle *tableInfoPage = MAKE_PAGE_HANDLE(); 	       //create a page for the intial page that holds schema information
	pinPage(bm,tableInfoPage,0);				      //pin the 0th page i.e tableinfopage in the  bufferpool
	rel->name=name; 					     //assign the name of the relation 
	rel->schema=(((RM_tableInfo *)tableInfoPage->data)->schema);//assign the schema of the relation in context
	rel->mgmtData=bm; 					   // the relation will be pointing to the instance of the bufferpool 
	unpinPage(bm,tableInfoPage);                              //unpin the tableinfopage
	free(tableInfoPage); 					 // free the tableinfopage 
	return RC_OK;						//Return Success Full completion status
}

/*------------------------------closeTable------------------------------------------
Function:   closeTable                                               
Purpose:    To close a table
Parameters: IN =rel: the relation in context
            OUT = RC_OK: after successfull completion          		   
Returns:    RC_OK: completion status of closing a table.                        
         		                                                        
------------------------------------------------------------------------------------------*/

extern RC closeTable (RM_TableData *rel)
{	
	shutdownBufferPool((BM_BufferPool *)rel->mgmtData);  //shut down the buffer pool
	free(rel->mgmtData);                                //free the management pointer pointing to the table's bufferpool
	return RC_OK;					   //Return Success Full completion status
}
/*------------------------------deleteTable------------------------------------------
Function:   deleteTable                                               
Purpose:    To delete a table
Parameters: IN =name:name of the table or file in context
            OUT = RC_OK: after successfull completion          		   
Returns:    RC_OK: completion status of destroying the file of the table.                        
         		                                                        
------------------------------------------------------------------------------------------*/
extern RC deleteTable (char *name)
{
	return destroyPageFile(name);                  //destroy the pagefile for the table
}

/*------------------------------getNumTuples------------------------------------------
Function:   getNumTuples                                               
Purpose:    To get number of the records in the table
Parameters: IN =rel: the relation in context
            OUT = result: number of the records in the table        		   
Returns:    result: number of the records in the table                          
         		                                                        
------------------------------------------------------------------------------------------*/

extern int getNumTuples (RM_TableData *rel)
{
	BM_PageHandle *tableInfoPage = MAKE_PAGE_HANDLE();           // allocate space for a page   
	pinPage((BM_BufferPool *)rel->mgmtData,tableInfoPage,0);    //page 0 or tableinfopage is pinned
  	int result;               				   //initialize variable to hold number of records.
	result=((RM_tableInfo *)tableInfoPage->data)->numTuples;  //fetch the value of no of records from the 0th page.
	unpinPage((BM_BufferPool *)rel->mgmtData,tableInfoPage); //unpin the tableinformation page from the bufferpool
	free(tableInfoPage);                                    //deallocate space for 0ths page
	return result;         				       //return the number of the records in the table.
}
/*------------------------------shutdownRecordManager------------------------------------------
Function:   shutdownRecordManager                                               
Purpose:    To shut down the record manager
Parameters: IN =void: no input
            OUT = RC_OK: after successfull completion          		   
Returns:    RC_OK: completion status of shuting down the recordmanager                        
         		                                                        
------------------------------------------------------------------------------------------*/

extern RC shutdownRecordManager ()
{
	return RC_OK;                                //Return Success Full completion status
}
/*------------------------------createSchema------------------------------------------
Function:   createSchema                                               
Purpose:    To create a schema and set memory for the data of the schema
Parameters: IN =numAttr: The number of attribute in the schema
		attrNames; Name of the attributes in the table 
                dataTypes: datatype of the data that attributes of the table would hold
		typeLength: length of the string if the attribute holds string value
		keySize: NUmber of Keys in the table
		keys: array containing the position of the attributes in the records of the table
            OUT = result: after the schema is created schema's instance is returned          		   
Returns:    result: the value-initialized schema is returned.                         
         		                                                        
------------------------------------------------------------------------------------------*/
extern Schema *createSchema (int numAttr, char **attrNames, DataType *dataTypes, int *typeLength, int keySize, int *keys)
{	
	Schema *result;                                     //holder of the schema type
	int i;                                             //iterator variable
	result=(Schema *)malloc(sizeof(Schema));          //create space for the schema data   
	result->numAttr=numAttr;			 //setting the number of attribute for the schema
	result->keySize=keySize;                        //setting the NUmber of Keys in the table
	result->attrNames=(char **)malloc(sizeof(char *)*numAttr);//allocating space for the name of the attributes in the table 
	result->dataTypes=(DataType *)malloc(sizeof(DataType)*numAttr);//allocating space for the datatype of the data that attributes would hold
	result->typeLength=(int *)malloc(sizeof(int)*numAttr);//allocating space for the string if the attribute holds string value 
	result->keyAttrs=(int *)malloc(sizeof(int)*keySize);//allocating space for NUmber of Keys in the table
	for(i = 0; i < numAttr; i++)                                       //iterate for each atributes
    	{
      		result->attrNames[i] = (char *) malloc(sizeof(char));     //allocate space for each attribute's name
      		strcpy(result->attrNames[i], attrNames[i]);              //set the names of the attributes
    	}
  	memcpy(result->dataTypes, dataTypes, sizeof(DataType)*numAttr);//set the datatypes for the schema
  	memcpy(result->typeLength, typeLength, sizeof(int)*numAttr);  //set the typelength for the string datatypes
    	memcpy(result->keyAttrs, keys, sizeof(int)*keySize);         //set the value of key attributes of the table.
	return result;                 				    //the value-initialized schema is returned.     
}
/*------------------------------getAttr------------------------------------------
Function:   getAttr                                               
Purpose:    To fetch the value of one attribute from a record    
Parameters: IN =attrNum: The position of the attribute in the schema
		record: record in context to be fetched value from 
                schema: schema information of the table
		value: the value holder for the value of the record
	    OUT = RC_OK: the successful completion status.          		   
Returns:    RC_OK: completion status of fetching the value of one attribute from a record                      
         		                                                        
------------------------------------------------------------------------------------------*/
extern RC getAttr (Record *record, Schema *schema, int attrNum, Value **value)
{
	int i,recordLength,remaining;               //iterator, size of record and remaining offset holder                                
	char *position=record->data;               //fetch the record data
	recordLength=getRecordSize(schema);       //get the size of the record through schema information
	remaining=recordLength;                  //initially remaining length of record will be full record size
	Value *holder=malloc(sizeof(Value));    // allocate memory for the value holder
	for(i=0;i<schema->numAttr;i++)         //iterate for each attribute         
	{	if(i==attrNum)                // if atribute number and the iterator are same
		{	switch(schema->dataTypes[i]) 
			{
      			case DT_INT: 				                 //if the data type is integer
			holder->dt=schema->dataTypes[i];                        //holder dt's data type will be the data type of the array dataType
			memcpy(&(holder->v.intV),position,sizeof(int));        // copy memory content of position into the holder for an int size
			break;
     			case DT_FLOAT:                                       //if the data type is float
			holder->dt=schema->dataTypes[i];                    //holder's dt data type will be the data type of the array dataType
			memcpy(&(holder->v.floatV),position,sizeof(float));// copy memory content of position into the holder for a float size
			break;
      			case DT_BOOL:						//if the data type is bool
			holder->dt=schema->dataTypes[i];                       //holder's dt data type will be the data type of the array dataType
			memcpy(&(holder->v.boolV),position,sizeof(bool));     // copy memory content of position into the holder for a bool size
			break;
			case DT_STRING:					         //if the data type is bool
			holder->dt=schema->dataTypes[i];      			//holder's dt data type will be the data type of the array dataType
			holder->v.stringV=(char *)malloc(sizeof(char)*(schema->typeLength[i]+1));//the typelength will give the size for the string
			memcpy(holder->v.stringV,position,sizeof(char)*(schema->typeLength[i]+1));//copy only the string sized byte locations
			break;
			}
			break;
		}	else{
			switch(schema->dataTypes[i])                        
      			{
      			case DT_INT:                                            //in case of integer datatype
			remaining=remaining-sizeof(int);                       //remaining will now have one integer less space from recordsize 
			position=record->data+(recordLength-remaining);       //traverse to the new position after the integer data
			break;
     			case DT_FLOAT:					    //in case of float datatype
			remaining=remaining-sizeof(float);                 //remaining will now have one float less space from recordsize
			position=record->data+(recordLength-remaining);   //traverse to the new position after the float data 
			break;
      			case DT_BOOL:					      //in case of bool datatype
			remaining=remaining-sizeof(bool);                    //remaining will now have one bool less space from recordsize
			position=record->data+(recordLength-remaining);     //traverse to the new position after the bool data
			break;
			case DT_STRING:                                    	      //in case of String type
			remaining=remaining-(sizeof(char)*(schema->typeLength[i]+1));//remaining will have stringlength less space from size of rec
			position=record->data+(recordLength-remaining);             //traverse to the new position after the string length
			break;
      			}
		}	
	}
	*value=holder;                                                            // Update the value with the new value

	return RC_OK;                                                           // return the successful completion status.

}
/*------------------------------getRecordSize------------------------------------------
Function:   getRecordSize                                               
Purpose:    To get the size of one record   
Parameters: IN =schema: schema information of the table
	    OUT = recordLength: Size of one record         		   
Returns:    recordLength: the size of the record                         
         		                                                        
------------------------------------------------------------------------------------------*/
extern int getRecordSize (Schema *schema)
{	int i,recordLength;                                              			    	   //iterator,size of the record 
	recordLength=0;                                      
	for(i=0;i<schema->numAttr;i++)                                  				 //iterate through each of the attribute 
	{	switch(schema->dataTypes[i])                     
      		{
      			case DT_INT:                                                                  // if the attribute's data type is an integer
			recordLength=recordLength+sizeof(int);                                       //add integer size to record length
			break; 
     			case DT_FLOAT:                                                             //if the attribute's data type is a float
			recordLength=recordLength+sizeof(float);				  //add float size to record length
			break;
      			case DT_BOOL:				                                //if the attribute's data type is bool 			   
			recordLength=recordLength+sizeof(bool);				      //add bool size to record length
			break;
			case DT_STRING:                                                     //if the attribute's data type is string
			recordLength=recordLength+(sizeof(char)*(schema->typeLength[i]+1));//add recordsize with its set typelength
			break;
      		}
	}
	
	return recordLength;                                                          //return length of the record
}
/*------------------------------createRecord------------------------------------------
Function:   createRecord                                               
Purpose:    To create one record.
Parameters: IN =schema: schema information of the table
		record: record to be created
	    OUT = RC_OK: recordLength: Size of one record         		   
Returns:    RC_OK: Succeful completion status after creating record                      
         		                                                        
------------------------------------------------------------------------------------------*/
extern RC createRecord (Record **record, Schema *schema)
{
	Record *result=(Record *)malloc(sizeof(Record));                //allocate space for the record's data
	result->data=(char *)malloc(getRecordSize(schema));	       //the data section of the allocated space is allocated
	*record=result;                                               //assign the address of the record to allocated space
	return RC_OK;						     //return with successful completion status
}
/*------------------------------setAttr------------------------------------------
Function:   setAttr                                               
Purpose:    To fetch the value of one attribute into the record    
Parameters: IN =attrNum: The position of the attribute in the schema
		record: record in context to be fetched value from 
                schema: schema information of the table
		value: the value holder for the value of the record
	    OUT = RC_OK: the successful completion status.          		   
Returns:    RC_OK: completion status of fetching the value of one attribute from a record                      
         		                                                        
------------------------------------------------------------------------------------------*/
extern RC setAttr (Record *record, Schema *schema, int attrNum, Value *value)
{	
	int i,recordLength,remaining;//iterator, size of record and remaining offset holder              
	char *position=record->data;//fetch the record data
	recordLength=getRecordSize(schema);//get the size of the record through schema information
	remaining=recordLength;//initially remaining length of record will be full record size
	for(i=0;i<schema->numAttr;i++)//iterate for each attribute  
	{	
		if(i==attrNum)
		{	switch(value->dt)
			{
      			case DT_INT:  				             //if the data type is integer
			memcpy(position,&(value->v.intV),sizeof(int));      // copy memory content into position through the holder for an int size
			break;
     			case DT_FLOAT:					   //if the data type is float
			memcpy(position,&(value->v.floatV),sizeof(float));// copy memory content into position through the holder for float size
			break;
      			case DT_BOOL:					 //if the data type is Bool
			memcpy(position,&(value->v.boolV),sizeof(bool));// copy memory content into position through the holder for Bool size
			break;
			case DT_STRING:				                                  //if the data type is integer
			memcpy(position,value->v.stringV,sizeof(char)*(schema->typeLength[i]+1));// copy memory content into position through the 													//holder for stringlength size
			break;
			}
			break;
		}else{
			switch(schema->dataTypes[i])
      			{
      			case DT_INT:					 //in case of integer datatype
			remaining=remaining-sizeof(int);       		//remaining will now have one integer less space from recordsize 
			position=record->data+(recordLength-remaining);//traverse to the new position after the integer data
			break;
     			case DT_FLOAT:				         //in case of float datatype
			remaining=remaining-sizeof(float);		//remaining will now have one float less space from recordsize 
			position=record->data+(recordLength-remaining);//traverse to the new position after the float data
			break;
      			case DT_BOOL:			                 //in case of Bool datatype
			remaining=remaining-sizeof(bool);		//remaining will now have one Bool less space from recordsize 
			position=record->data+(recordLength-remaining);//traverse to the new position after the Bool data
			break;
			case DT_STRING:						      //in case of String datatype
			remaining=remaining-(sizeof(char)*(schema->typeLength[i]+1));//remaining will now have stringlength less space from 											    //	recordsize 
			position=record->data+(recordLength-remaining);		   //traverse to the new position after the string data
			break;
      			}
		}
	}

	return RC_OK;                   					//Return successful completion status. 
} 
/*------------------------------insertRecord------------------------------------------
Function:   insertRecord                                               
Purpose:    To insert a record into the file
Parameters: IN =rel: the relation in context
            IN =record: the record to be inserted
            OUT = RC_OK: the successful completion status.          		   
Returns:    RC_OK: completion status of inserting a record into the file                        
         		                                                        
------------------------------------------------------------------------------------------*/
extern RC insertRecord (RM_TableData *rel, Record *record)
{	
	BM_PageHandle *tableInfoPage = MAKE_PAGE_HANDLE();       //create a page for the intial page that holds schema information
	pinPage((BM_BufferPool *)rel->mgmtData,tableInfoPage,0);//pin the 0th page i.e tableinfopage/Schemapage in the  bufferpool
	BM_PageHandle *PageToInsert = MAKE_PAGE_HANDLE();      //Allocate space for the page to insert
	char *locator; 				              //pointer for the offset value
	if(((RM_tableInfo *)tableInfoPage->data)->freeSpace.slot>((RM_tableInfo *)tableInfoPage->data)->noOfSlots)// if slot exceeds capacity
	{	(((RM_tableInfo *)tableInfoPage->data)->freeSpace.page)++;     //start with next new page,
		((RM_tableInfo *)tableInfoPage->data)->freeSpace.slot=1;      //start with first slot of the new page.
		pinPage((BM_BufferPool *)rel->mgmtData,PageToInsert,((RM_tableInfo *)tableInfoPage->data)->freeSpace.page);//Pin the new Page
		locator=PageToInsert->data; 										  //locate the offset 
	}else{	
pinPage((BM_BufferPool *)rel->mgmtData,PageToInsert,((RM_tableInfo *)tableInfoPage->data)->freeSpace.page); 		// pin the current page
locator=PageToInsert->data + (getRecordSize(rel->schema)+sizeof(bool))*((((RM_tableInfo *)tableInfoPage->data)->freeSpace.slot)-1);//reduce slot
	}
		record->id.page=(((RM_tableInfo *)tableInfoPage->data)->freeSpace.page);    //point the page that freespace points to		
		record->id.slot=((RM_tableInfo *)tableInfoPage->data)->freeSpace.slot;     // point the page that freespace page's slot points to
		(((RM_tableInfo *)tableInfoPage->data)->freeSpace.slot)++;                //increament the slot	
		memcpy(locator,&deleteFlag,sizeof(bool));                                //update the locator with delete flag status
		locator=locator+sizeof(bool);	                                        //displace the locator from the flag data
		memcpy(locator,record->data,getRecordSize(rel->schema));               //copy the actual record data into the locator.
		markDirty((BM_BufferPool *)rel->mgmtData,PageToInsert);               //the page modified so marked dirty
		unpinPage((BM_BufferPool *)rel->mgmtData,PageToInsert);              //then the page is unpinned.
		(((RM_tableInfo *)tableInfoPage->data)->numTuples)++;               //no of records increamented  
		markDirty((BM_BufferPool *)rel->mgmtData,tableInfoPage);           //then the schema page is also modified so marked dirty
		unpinPage((BM_BufferPool *)rel->mgmtData,tableInfoPage);          //then the schema page is unpinned
		free(PageToInsert);                                              //release the pagetoinsert pointer
		free(tableInfoPage);		                                //release the schema page pointer
		return RC_OK;                                                  //return with success status                   						        									
}
/*------------------------------getRecord------------------------------------------
Function:   getRecord                                               
Purpose:    To get a record by RID from the file
Parameters: IN =rel: the relation in context
            IN =id: id of the record to be inserted
	    IN =record: record to get value from
            OUT = RC_OK: the successful completion status. 
            OUT = RC_RM_NO_TUPLE_EXISTS: After all tuples are traversed when searching         		   
Returns:    RC_OK: completion status of getting a record from the file   
            RC_RM_NO_TUPLE_EXISTS: when  all tuples are traversed                     
         		                                                        
------------------------------------------------------------------------------------------*/
extern RC getRecord (RM_TableData *rel, RID id, Record *record)
{	
	BM_PageHandle *tableInfoPage = MAKE_PAGE_HANDLE();         //create a page for the intial page that holds schema information
	pinPage((BM_BufferPool *)rel->mgmtData,tableInfoPage,0);  //pin the 0th page i.e tableinfopage in the  bufferpool
	BM_PageHandle *PageToRead = MAKE_PAGE_HANDLE();   	 //create a page for the page to read from for the record
	char *locator;                                          //locator for the record data

	pinPage((BM_BufferPool *)rel->mgmtData,PageToRead,id.page);				// Pin the required page to get the record
	locator=PageToRead->data + (getRecordSize(rel->schema)+sizeof(bool))*(id.slot-1);      //decrease the slot
	if(*((bool *)locator)==false)                                                         //if the deleteflag is not set
	{
	locator=locator + sizeof (bool);                                                    //displace the offset further by bool size
	memcpy(record->data,locator,getRecordSize(rel->schema));                           //copy locators data to record data for a record size
	record->id.page=id.page;                                                          //set the page through the record's id
	record->id.slot=id.slot;                                                         //set the slot through the record's id
	unpinPage((BM_BufferPool *)rel->mgmtData,PageToRead);                           //unpin the page used to read record
	unpinPage((BM_BufferPool *)rel->mgmtData,tableInfoPage);                       //unpin the schema page
	free(tableInfoPage);                                                          //release the schema page
	free(PageToRead);							     //release the page used to read record.	
        return RC_OK;                                                               //return successfull completion status
	}else {                                                                    //else
	unpinPage((BM_BufferPool *)rel->mgmtData,PageToRead);                     //directly unpin the page used for record
	unpinPage((BM_BufferPool *)rel->mgmtData,tableInfoPage);                 //directly unpin the schema page.
	free(tableInfoPage);                                                    //release the schema page
	free(PageToRead); 						       //release the page used for record	
	return RC_RM_NO_TUPLE_EXISTS;                                         //when no more records in the page exists.
	}  
}
/*------------------------------updateRecord------------------------------------------
Function:   updateRecord                                               
Purpose:    To update a record into the file
Parameters: IN =rel: the relation in context
            IN =record: record to get value from
            OUT = RC_OK: the successful completion status. 
            OUT = RC_RM_NO_TUPLE_EXISTS: After all tuples are traversed when searching         		   
Returns:    RC_OK: completion status of updating a record into the file   
            RC_RM_NO_TUPLE_EXISTS: when  all tuples are traversed                     
         		                                                        
------------------------------------------------------------------------------------------*/
extern RC updateRecord (RM_TableData *rel, Record *record)
{
	BM_PageHandle *tableInfoPage = MAKE_PAGE_HANDLE();			//create a page for the intial page that holds schema information
	pinPage((BM_BufferPool *)rel->mgmtData,tableInfoPage,0);               //pin the 0th page i.e tableinfopage in the  bufferpool
	BM_PageHandle *PageToWrite = MAKE_PAGE_HANDLE();                      //create a page for the page to update into the record
	char *locator;                                                       //locator for the record data
	pinPage((BM_BufferPool *)rel->mgmtData,PageToWrite,record->id.page);// Pin the required page to get the record
	locator=PageToWrite->data + (getRecordSize(rel->schema)+sizeof(bool))*(record->id.slot-1);//decrease the slot
	if(*((bool *)locator)==false)								 //if the deleteflag is not set
	{ 
	locator=locator+sizeof(bool);								//displace the offset further by bool size
	memcpy(locator,record->data,getRecordSize(rel->schema));		               //copy RECORDS'S data to LOCATOR for a record size
	markDirty((BM_BufferPool *)rel->mgmtData,PageToWrite);				      //the page modified so marked dirty
	unpinPage((BM_BufferPool *)rel->mgmtData,PageToWrite);                               //unpin the page used to UPDATE record
	unpinPage((BM_BufferPool *)rel->mgmtData,tableInfoPage);			    //unpin the schema page
	free(tableInfoPage);								   //release the schema page
	free(PageToWrite);							          //release the page used for update record
	return RC_OK;									 //return successfull completion status
	}else{ 										//else
	unpinPage((BM_BufferPool *)rel->mgmtData,PageToWrite); 			       //unpin the page used for record
	unpinPage((BM_BufferPool *)rel->mgmtData,tableInfoPage);		      //unpin the schema page	                      
	free(tableInfoPage);							     //release the schema page
	free(PageToWrite);                                                          //release the page used for update record
	return RC_RM_NO_TUPLE_EXISTS;						   //when no more records in the page exists.
	}
}
/*------------------------------freeRecord------------------------------------------
Function:   freeRecord                                               
Purpose:    To free a record from the file
Parameters: IN =record: record to get value from
            OUT = RC_OK: the successful completion status. 
                   		   
Returns:    RC_OK: completion status of freeing a record from the file   
                                
         		                                                        
------------------------------------------------------------------------------------------*/
extern RC freeRecord (Record *record)
{
	free(record->data);              //release the data of the record
	record->data=NULL;              
	free(record);                    //release the record
	return RC_OK;		        //return successful completion status.
}
/*------------------------------deleteRecord------------------------------------------
Function:   deleteRecord                                               
Purpose:    To delete a record by RID from the file
Parameters: IN =rel: the relation in context
            IN =id: id of the record to be deleted
	    OUT = RC_OK: the successful completion status. 
            OUT = RC_RM_NO_TUPLE_EXISTS: After all tuples are traversed when searching         		   
Returns:    RC_OK: completion status of deleting a record from the file   
            RC_RM_NO_TUPLE_EXISTS: when  all tuples are traversed                     
         		                                                        
------------------------------------------------------------------------------------------*/
extern RC deleteRecord (RM_TableData *rel, RID id)
{
	BM_PageHandle *tableInfoPage = MAKE_PAGE_HANDLE();   			  //create a page for the intial page that holds schema information
	pinPage((BM_BufferPool *)rel->mgmtData,tableInfoPage,0);		 //pin the 0th page i.e tableinfopage in the  bufferpool
	BM_PageHandle *PageToDelete = MAKE_PAGE_HANDLE();			//create a page for the page containg the record to be deleted
	char *locator;//locator for the record data
	pinPage((BM_BufferPool *)rel->mgmtData,PageToDelete,id.page);			    // Pin the required page to get the record
	locator=PageToDelete->data + (getRecordSize(rel->schema)+sizeof(bool))*(id.slot-1);//decrease the slot
	if(*((bool *)locator)==false)						          //if the deleteflag is not set
	{
	deleteFlag=1;                							//set the deleteflag to true
	memcpy(locator,&deleteFlag,sizeof(bool));                                      // update locatordata with the deleteflag
	deleteFlag=0;                          					      //set delete flag to false
	(((RM_tableInfo *)tableInfoPage->data)->numTuples)++; 			     // increase no of records
	markDirty((BM_BufferPool *)rel->mgmtData,PageToDelete);                     //page modified so marked dirty 
	unpinPage((BM_BufferPool *)rel->mgmtData,PageToDelete);                    // unpin the page containing the record
	markDirty((BM_BufferPool *)rel->mgmtData,tableInfoPage);                  //schema page modified so marked dirty.
	unpinPage((BM_BufferPool *)rel->mgmtData,tableInfoPage);                 //unpin the schema page
	free(tableInfoPage);                                                    //release the schema page
	free(PageToDelete);	      					       //release the page containing the record
	return RC_OK;							      //return successfull completion status
	}else{								     //else
	unpinPage((BM_BufferPool *)rel->mgmtData,PageToDelete);             //unpin the page used for record
	unpinPage((BM_BufferPool *)rel->mgmtData,tableInfoPage);           //unpin the schema page	
	free(tableInfoPage);                                              //release the schema page
	free(PageToDelete);                                              //release the page used for delete record
        return RC_RM_NO_TUPLE_EXISTS;                                   //when no more records in the page exists.                                 
	}	
}
/*------------------------------freeSchema------------------------------------------
Function:   freeSchema                                               
Purpose:    To free a schema and all its associated resources
Parameters: IN =schema: schema of the table
            OUT = RC_OK: the successful completion status. 
                   		   
Returns:    RC_OK: completion status of freeing a schema                        
------------------------------------------------------------------------------------------*/
extern RC freeSchema (Schema *schema)
{	
	int i;
	free(schema->keyAttrs);                          //release space for all key attributes
	free(schema->typeLength);                       //release space for all type length for string attributes
	free(schema->dataTypes);                       //release space for datatypes of attributes.
	for (i=0;i<schema->numAttr;i++)               //iterate for all records
	{	free(schema->attrNames[i]);          //release space for names of attributes
	}
	free(schema->attrNames);                 
	free(schema);                              //release the schema

	return RC_OK;
}
/*------------------------------startScan------------------------------------------
Function:   startScan                                               
Purpose:    To set up the relation, condition and scan condition for the scan function
Parameters: IN =rel: the relation in context
            IN =scan: Instance of scanhandle for the condition
            IN =cond: condition of the expression
	    OUT = RC_OK: the successful completion status. 
Returns:    RC_OK: completion status of deleting a record from the file        
------------------------------------------------------------------------------------------*/
extern RC startScan (RM_TableData *rel, RM_ScanHandle *scan, Expr *cond)
{	
	scan->rel=rel;                                           //assign the relation to the required instance 
	scan->mgmtData=(void *)malloc(sizeof(RM_scanInfo));     //allocate space for management data.   
	((RM_scanInfo *)scan->mgmtData)->cal=cond;	       //assign the condition to the expression 
	((RM_scanInfo *)scan->mgmtData)->id.page=1;           //start search from the page 1
	((RM_scanInfo *)scan->mgmtData)->id.slot=1;          //start search from slot 1
	return RC_OK;                                       //return successful completion status.
}
/*------------------------------------------next------------------------------------------
Function:   next                                               
Purpose:    to search next record based on the condition
Parameters: IN =record: one record of the file 
            IN =scan: Instance of scanhandle for the condition
            OUT = RC_OK: the successful completion status. 
Returns:    RC_OK: completion status of scanning from the file.       
------------------------------------------------------------------------------------------*/
extern RC next (RM_ScanHandle *scan, Record *record)
{	
	BM_PageHandle *tableInfoPage = MAKE_PAGE_HANDLE();//create a page for the intial page that holds schema information
	pinPage((BM_BufferPool *)scan->rel->mgmtData,tableInfoPage,0);//pin the 0th page i.e tableinfopage in the  bufferpool
	Value *result;        //Value pointer for the one attribute value
	bool flag=0;         //flag to check the match
	
	while((((RM_scanInfo *)scan->mgmtData)->id.page<((RM_tableInfo *)tableInfoPage->data)->freeSpace.page) ||
		(((RM_tableInfo *)tableInfoPage->data)->freeSpace.slot>((RM_scanInfo *)scan->mgmtData)->id.slot))// iterate within the page
	{	
		getRecord(scan->rel,((RM_scanInfo *)scan->mgmtData)->id,record); //get the record by the id
		evalExpr (record, scan->rel->schema,((RM_scanInfo *)scan->mgmtData)->cal,&result);//evaluate the expression 
		if(result->v.boolV) // if the flag is set
		{	if(++(((RM_scanInfo *)scan->mgmtData)->id.slot)>((RM_tableInfo *)tableInfoPage->data)->noOfSlots) // if next page needed
			{	((RM_scanInfo *)scan->mgmtData)->id.slot=1; //set for first slot for next page 
				((RM_scanInfo *)scan->mgmtData)->id.page++; //set for the next page by increamenting the page.
			}
			flag=1; //if found then set 
			freeVal(result); //release the result for the record.
			break;		
		}else if(++(((RM_scanInfo *)scan->mgmtData)->id.slot)>((RM_tableInfo *)tableInfoPage->data)->noOfSlots)// if next page iteration
		{	((RM_scanInfo *)scan->mgmtData)->id.slot=1; // set slot to the first slot of next page
			((RM_scanInfo *)scan->mgmtData)->id.page++;// set page to next page by increasing the page.
		}
		freeVal(result);           				 // release the result for the record.
	}
	
	unpinPage((BM_BufferPool *)scan->rel->mgmtData,tableInfoPage); //unpin the schema page. 
	free(tableInfoPage);                                          //release the schema

	if(flag)                                                  // if flag is set then a match is found
	{	
		return RC_OK;                                    // return successfull completion status.
	}else{
		return RC_RM_NO_MORE_TUPLES;                    // return when no more records are left 
	}	
}
/*------------------------------------------closeScan------------------------------------------
Function:   closeScan                                               
Purpose:    to Close the scan and its resources.
Parameters: 
            IN =scan: Instance of scanhandle 
            OUT = RC_OK: the successful completion status. 
Returns:    RC_OK: completion status of closing the scan        
------------------------------------------------------------------------------------------*/
extern RC closeScan (RM_ScanHandle *scan)
{	
	free(scan->mgmtData);              //Free the expression holder for the scan Condition
	free(scan);		          //Free the scanhandle Instance.

	return RC_OK;                   // Return Successfull completion  Status
}
