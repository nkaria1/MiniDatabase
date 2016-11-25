/*---------------------------------------------------------------------------  
fileName:         buffer_mgr.c                                             
Authors:          Niharika Karia, Shiva Naveen Ravi, Shiv Ram Krishna       
Purpose:          To implement the functions of a Buffer manager.    	     
Language:         C                                                         
Created On:       08 September,2015                                           
Last Modified On: 06 October, 2015                                       
---------------------------------------------------------------------------*/
/*Headers Declaration*/
#include <stdlib.h>
#include <stdio.h>
#include <string.h>

#include "btree_mgr.h"
#include "buffer_mgr.h"
#include "storage_mgr.h"
#include "dberror.h"
#include "tables.h"

typedef struct BTreeStack {
  int pageNum;				//stores the page number of the node
  int index;				//stores the index of a key within the node.
} BTreeStack;

typedef struct BTreeScanInfo {
  int lastPageNum;			//stores the page number of the last node
  int keyIndex;				//stores the index of the key inside the node
} BTreeScanInfo;
// gives information about the Btree
typedef struct BTreeInfo {	//tree metadata	
  DataType keyType;		//data type of teh index attribute on which index is created
  int keySize;			//the number of keys that can be stored in a node
  int BTreeRoot;		//the page of root
  int LastPageInUse;		//last page that was used
  int noOfEntries;		//count of number of keys
  int noOfNodes;		//count of number of nodes
  BTreeStack stack[3];		
} BTreeInfo;

typedef struct BTreeNode {
  bool leaf;
  int noOfKeys;
  int pageNum[2];
  int nextleaf;
  RID recordKeys[1];
  Value keys[1];
} BTreeNode;

/*------------------------------initIndexManager--------------------------------
Function:   initIndexManager                                               
Purpose:    To initialise index manager
Parameters: IN =  mgmtData :pointer to management data	
	    OUT = RC_OK    :after successfull completion         		   
Returns:    RC_OK: completion status of Intialization of BufferPool                        
         		                                                        
------------------------------------------------------------------------------*/	
extern RC initIndexManager (void *mgmtData)
{
	return RC_OK;
}

/*------------------------------shutdownIndexManager--------------------------------
Function:   shutdownIndexManager                                               
Purpose:    To destroy the Btree and free all associated resources
Parameters: OUT = RC_OK    :after successfull completion         		   
Returns:    RC_OK: completion status of Intialization of BufferPool                        
         		                                                        
------------------------------------------------------------------------------*/	
extern RC shutdownIndexManager ()
{
	return RC_OK;
}

/*------------------------------allocateNode--------------------------------
Function:   allocateNode                                               
Purpose:    To allocate a node in the b+Tree
Parameters: OUT = BM_PageHandle * :after successfull allocating a node         		   
Returns:    BM_PageHandle *: ointer to the page where the node is allocated                      
         		                                                        
------------------------------------------------------------------------------*/	
extern BM_PageHandle* allocateNode (BM_BufferPool *bm,BTreeInfo *bt)
{
	BM_PageHandle *h = MAKE_PAGE_HANDLE();		//allocate the page handle
	bt->LastPageInUse++;				//increment the last used page 
	pinPage(bm,h,bt->LastPageInUse);		//increment the number of nodes
	bt->noOfNodes++;
	BTreeNode *n=(BTreeNode *)h->data;		
	n->leaf=true;					//set the node as leaf
	n->noOfKeys=0;
	n->nextleaf=0;					// initialise the next leaf null
	return h;
}

/*------------------------------createBtree--------------------------------
Function:   createBtree                                               
Purpose:    To create a new Btree
Parameters: IN =  idxId :the index id	
	    IN = keyType : data type of the index attribute 	
            IN = n: number of keys that can be stored in a node        
            OUT = RC_OK    :after successfull completion         		   
Returns:    RC_OK: completion status of creation of Btree                       
         		                                                        
------------------------------------------------------------------------------*/
extern RC createBtree (char *idxId, DataType keyType, int n)
{
	BM_BufferPool *bm = MAKE_POOL();		// make buffer pool
	createPageFile(idxId);				// create file

	initBufferPool(bm, idxId, 7, RS_LRU, NULL);	//initialise buffer pool
	
	BM_PageHandle *h = MAKE_PAGE_HANDLE();
	pinPage(bm, h, 0);
	
	BTreeInfo *bt=(BTreeInfo*)malloc(sizeof(BTreeInfo));
	bt->keyType=keyType;				//parameterise the iniial values
	bt->keySize=n;
	bt->LastPageInUse=0;
	bt->noOfEntries=0;
	bt->noOfNodes=0;

	BM_PageHandle *p = allocateNode(bm,bt);

	bt->BTreeRoot=1;
	memcpy(h->data,bt,sizeof(BTreeInfo));
	
	markDirty(bm,h);
	unpinPage(bm,h);

	markDirty(bm,p);
	unpinPage(bm,p);
	
	shutdownBufferPool(bm);
	free(h);
	free(p);
	free(bt);
	free(bm);

	return RC_OK;
}

/*------------------------------openBtree--------------------------------
Function:   openBtree                                               
Purpose:    To open a new Btree
Parameters: IN = idxId :the index id	
	    IN = BTreeHandle: pointer to pointer of tree handle 	
            OUT = RC_OK    :after successfull completion         		   
Returns:    RC_OK: completion status of after open of Btree                       
         		                                                        
------------------------------------------------------------------------------*/
extern RC openBtree (BTreeHandle **tree, char *idxId)
{
	BTreeHandle *result= (BTreeHandle *)malloc(sizeof(BTreeHandle));

	BM_BufferPool *bm = MAKE_POOL();
	initBufferPool(bm, idxId, 10, RS_LRU, NULL);

	BM_PageHandle *h = MAKE_PAGE_HANDLE();
	pinPage(bm, h, 0);

	result->keyType=((BTreeInfo *)h->data)->keyType;
	result->idxId = idxId;
	result->mgmtData = bm;
	*tree=result;

	unpinPage(bm,h);
	free(h);

	return RC_OK;		
}
/*------------------------------closeBtree--------------------------------
Function:   closeBtree                                               
Purpose:    To close the Btree
Parameters: IN = BTreeHandle: pointer to tree handle 	
            OUT = RC_OK    :after successfull completion         		   
Returns:    RC_OK: completion status of after close of Btree                       
         		                                                        
------------------------------------------------------------------------------*/
extern RC closeBtree (BTreeHandle *tree)
{
	shutdownBufferPool((BM_BufferPool *)tree->mgmtData);
	free((BM_BufferPool *)tree->mgmtData);
	free(tree);

	return RC_OK;
}
/*------------------------------deleteBtree--------------------------------
Function:   deleteBtree                                               
Purpose:    To destroy the Btree
Parameters: IN = idxId :the index id	
            OUT = RC_OK    :after successfull completion         		   
Returns:    RC_OK: completion status of after delete of Btree                       
         		                                                        
------------------------------------------------------------------------------*/
extern RC deleteBtree (char *idxId)
{
	destroyPageFile(idxId);
	
	return RC_OK;
}
/*------------------------------findKey--------------------------------
Function:   findKey                                               
Purpose:    To find a key in the Btree
Parameters: IN = BTreeHandle: pointer to tree handle 
	    IN = Key: pointer to Value of the key that needs to be searched
	    IN = result: The pointer to store RID of the key if it is found. 	
            OUT = RC_OK    :after successfull completion         		   
Returns:    RC_OK: completion status of seach in Btree                       
         		                                                        
------------------------------------------------------------------------------*/
extern RC findKey (BTreeHandle *tree, Value *key, RID *result)
{	
	BM_PageHandle *h = MAKE_PAGE_HANDLE();
	pinPage((BM_BufferPool *)tree->mgmtData,h,0);
	BTreeInfo *b=(BTreeInfo *)h->data;

	BM_PageHandle *r = MAKE_PAGE_HANDLE();
	pinPage((BM_BufferPool *)tree->mgmtData,r,b->BTreeRoot);
	int temp,tempPageNum;
	while(!(((BTreeNode *)r->data)->leaf))
	{
		temp=((BTreeNode *)r->data)->noOfKeys;
		while(temp>=1 && ((BTreeNode *)r->data)->keys[temp-1].v.intV>key->v.intV)
		{	temp=temp-1;
		}
		tempPageNum=((BTreeNode *)r->data)->pageNum[temp];
		unpinPage((BM_BufferPool *)tree->mgmtData,r);
		pinPage((BM_BufferPool *)tree->mgmtData,r,tempPageNum);
	}
	temp=((BTreeNode *)r->data)->noOfKeys;
	while(temp>=1 && ((BTreeNode *)r->data)->keys[temp-1].v.intV>key->v.intV)
	{	temp=temp-1;
	}
	if(((BTreeNode *)r->data)->keys[temp-1].v.intV==key->v.intV)
	{	*result=((BTreeNode *)r->data)->recordKeys[temp-1];
		unpinPage((BM_BufferPool *)tree->mgmtData,h);
		unpinPage((BM_BufferPool *)tree->mgmtData,r);
	
		free(h);
		free(r);
		return RC_OK;
	}else{
		unpinPage((BM_BufferPool *)tree->mgmtData,h);
		unpinPage((BM_BufferPool *)tree->mgmtData,r);

		free(h);
		free(r);
		return RC_IM_KEY_NOT_FOUND;
	}
}

/*------------------------------getNumEntries--------------------------------
Function:   getNumEntries                                               
Purpose:    To find the count of keys in the Btree
Parameters: IN = BTreeHandle: pointer to tree handle 
	    IN = result: The pointer to store the count of key. 	
            OUT = RC_OK    :after successfull completion         		   
Returns:    RC_OK: completion count of all keys in Btree                       
         		                                                        
------------------------------------------------------------------------------*/
extern RC getNumEntries (BTreeHandle *tree, int *result)
{	
	BM_PageHandle *h = MAKE_PAGE_HANDLE();
	pinPage((BM_BufferPool *)tree->mgmtData,h,0);
	BTreeInfo *b=(BTreeInfo *)h->data;
	*result=b->noOfEntries;

	unpinPage((BM_BufferPool *)tree->mgmtData,h);
	free(h);

	return RC_OK;
}


/*------------------------------openTreeScan--------------------------------
Function:   openTreeScan                                               
Purpose:    To scan keys in the Btree in sequential order
Parameters: IN = tree: pointer to tree handle
	    IN = handle: pointer to scan handle to track the scan.
            OUT = RC_OK    :after successfull completion         		   
Returns:    RC_OK: completion status                      
         		                                                        
------------------------------------------------------------------------------*/
extern RC openTreeScan (BTreeHandle *tree, BT_ScanHandle **handle)
{
	BT_ScanHandle *result=(BT_ScanHandle *)malloc(sizeof(BT_ScanHandle));
	BTreeScanInfo *si = (BTreeScanInfo *)malloc(sizeof(BTreeScanInfo));

	BM_PageHandle *h = MAKE_PAGE_HANDLE();
	pinPage((BM_BufferPool *)tree->mgmtData,h,0);
	BTreeInfo *b=(BTreeInfo *)h->data;

	BM_PageHandle *r = MAKE_PAGE_HANDLE();
	pinPage((BM_BufferPool *)tree->mgmtData,r,b->BTreeRoot);

	int temp,tempPageNum;
	while(!(((BTreeNode *)r->data)->leaf))
	{
		tempPageNum=((BTreeNode *)r->data)->pageNum[0];
		unpinPage((BM_BufferPool *)tree->mgmtData,r);
		pinPage((BM_BufferPool *)tree->mgmtData,r,tempPageNum);
	}
	si->lastPageNum=r->pageNum;
	si->keyIndex=-1;
	result->tree=tree;
	result->mgmtData=si;
	*handle=result;
	unpinPage((BM_BufferPool *)tree->mgmtData,r);
	unpinPage((BM_BufferPool *)tree->mgmtData,h);

	free(r);
	free(h);

	return RC_OK;
}
/*------------------------------nextEntry--------------------------------
Function:   nextEntry                                               
Purpose:    To scan keys in the Btree in sequential order
Parameters: IN = Handle: pointer to tree handle
	    IN = result: The pointer to store the RID of next key. 	
            OUT = RC_OK    :after successfull completion         		   
Returns:    RC_OK: completion status                       
         		                                                        
------------------------------------------------------------------------------*/
extern RC nextEntry (BT_ScanHandle *handle, RID *result)
{
	if(((BTreeScanInfo *)handle->mgmtData)->lastPageNum==0)
	{
		return RC_IM_NO_MORE_ENTRIES;
	}else
	{
		BM_PageHandle *r = MAKE_PAGE_HANDLE();
		pinPage((BM_BufferPool *)((handle->tree)->mgmtData),r,((BTreeScanInfo *)handle->mgmtData)->lastPageNum);
		if(++((BTreeScanInfo *)handle->mgmtData)->keyIndex<((BTreeNode *)r->data)->noOfKeys)
		{	result->page=((BTreeNode *)r->data)->recordKeys[((BTreeScanInfo *)handle->mgmtData)->keyIndex].page;
			result->slot=((BTreeNode *)r->data)->recordKeys[((BTreeScanInfo *)handle->mgmtData)->keyIndex].slot;
			unpinPage((BM_BufferPool *)((handle->tree)->mgmtData),r);
			free(r);
			return RC_OK;
		}else
		{
			((BTreeScanInfo *)handle->mgmtData)->keyIndex=-1;
			((BTreeScanInfo *)handle->mgmtData)->lastPageNum=((BTreeNode *)r->data)->nextleaf;
			unpinPage((BM_BufferPool *)((handle->tree)->mgmtData),r);
			free(r);
			return nextEntry(handle,result);
		}
	}
}
/*------------------------------closeTreeScan--------------------------------
Function:   closeTreeScan                                               
Purpose:    To close the scan handle and free associated resources
Parameters: IN = Handle: pointer to scan handle
            OUT = RC_OK    :after successfull completion         		   
Returns:    RC_OK: completion  status of close.                       
         		                                                        
------------------------------------------------------------------------------*/
extern RC closeTreeScan (BT_ScanHandle *handle)
{
	free(handle->mgmtData);
	free(handle);

	return RC_OK;
}

/*------------------------------insertLeafKey--------------------------------
Function:   insertLeafKey                                               
Purpose:    insert a new leaf key
Parameters: IN = node: pointer to tree node
	    IN = key: the value that needs to be inserted
	    IN = rid: the RID of the leaf where the insreion should be done
            OUT = RC_OK    :after successfull completion         		   
Returns:    RC_OK: completion  status of close.                       
         		                                                        
------------------------------------------------------------------------------*/
extern RC insertLeafKey(BTreeNode *node, Value *key, RID rid)
{
	int i=node->noOfKeys;
	while(i>=1 && key->v.intV<(node->keys[i-1]).v.intV)
	{	
		(node->keys[i]).v.intV=(node->keys[i-1]).v.intV;
		(node->keys[i]).dt=(node->keys[i-1]).dt;
		node->recordKeys[i].page=node->recordKeys[i-1].page;
		node->recordKeys[i].slot=node->recordKeys[i-1].slot;
		i=i-1;
	}
	(node->keys[i]).v.intV=key->v.intV;
	(node->keys[i]).dt=key->dt;
	node->recordKeys[i].page=rid.page;
	node->recordKeys[i].slot=rid.slot;
	(node->noOfKeys)++;

	return RC_OK;
}
/*------------------------------updateKey--------------------------------
Function:   updateKey                                               
Purpose:    To updated the key values in the node structure in case of underflow
Parameters: IN = n: pointer to Btree node
	    IN = key: Value of the key 	
	    IN = rid: RID of the node  	
	    IN = okey: Value of the other key 	
	    IN = orid: RID of the other node  	
            OUT = RC_OK    :after successfull completion         		   
Returns:    RC_OK: completion status                       
         		                                                        
------------------------------------------------------------------------------*/
extern RC updateKey(BTreeNode* n, Value *key, RID *rid, Value *oKey, RID *orid)
{
	int i=0,j=n->noOfKeys-1;
	memcpy(oKey,key,sizeof(Value));
	memcpy(orid,rid,sizeof(RID));
	while(i<n->noOfKeys && n->keys[i].v.intV<key->v.intV)
	{	i++;
	}
	if(i!=n->noOfKeys)
	{
		oKey->v.intV=n->keys[n->noOfKeys-1].v.intV;
		orid->page=n->recordKeys[n->noOfKeys-1].page;
		orid->slot=n->recordKeys[n->noOfKeys-1].slot;
		while(j>i)
		{	n->keys[j].v.intV=n->keys[j-1].v.intV;
			n->recordKeys[j].page=n->recordKeys[j-1].page;
			n->recordKeys[j].slot=n->recordKeys[j-1].slot;
			j--;
		}
		n->keys[i].v.intV=oKey->v.intV;
		n->recordKeys[i].page=orid->page;
		n->recordKeys[i].slot=orid->slot;
	}

	return RC_OK;
}
/*------------------------------rearrangeIN--------------------------------
Function:   rearrangeIN                                               
Purpose:    To rearrange the key values in the node(parent) structure in case of underflow
Parameters: IN = internalN: pointer to Btree node
	    IN = key: Value of the key 	
	    IN = pageNum: Page of the node  	
	    IN = okey: Value of the other key 	
            OUT = RC_OK    :after successfull completion         		   
Returns:    RC_OK: completion status                       
         		                                                        
------------------------------------------------------------------------------*/
extern int rearrangeIN(BTreeNode *internalN, int index, Value *key, int pageNum,Value *oKey)
{	
	int i=internalN->noOfKeys,exitPageNum;
	memcpy(oKey,&(internalN->keys[i-1]),sizeof(Value));
	exitPageNum=internalN->pageNum[i];
	if(index<internalN->noOfKeys)
	{	
				
		while(i==index)
		{	
			internalN->keys[i].v.intV=internalN->keys[i-1].v.intV;
			internalN->pageNum[i+1]=internalN->pageNum[i-1];
			i--;
		}
		internalN->keys[index].v.intV=key->v.intV;
		internalN->keys[index].dt=key->dt;
		internalN->pageNum[index+1]=pageNum;
		internalN->noOfKeys++;
	}else
	{
		(internalN->noOfKeys)++;
		internalN->keys[index].v.intV=key->v.intV;
		internalN->keys[index].dt=key->dt;
		internalN->pageNum[index+1]=pageNum;	
	}
	
	return exitPageNum;
}
/*------------------------------splitNode--------------------------------
Function:   splitNode                                               
Purpose:    To rearrange the key values in the node(parent) structure in case of overflow
Parameters: IN = child: pointer to Btree node of child that has over flow
	    IN = bm: buffer pool pointer	
	    IN = bt: Pointer to the btree info
	    IN = counter: the counte to determine the depth of node  	
	    IN = key: Value of the key
	    IN = rid: RID of the node
            OUT = RC_OK    :after successfull completion         		   
Returns:    RC_OK: completion status                       
         		                                                        
------------------------------------------------------------------------------*/
extern RC splitNode(BTreeNode* child, BM_BufferPool* bm, BTreeInfo* bt, int counter, Value *key, RID rid, int existPageNum)
{	
	RID *orid=(RID *)malloc(sizeof(RID));
	Value *oKey=(Value *)malloc(sizeof(Value));

	if(counter==0)
	{	
			BM_PageHandle *r=allocateNode(bm,bt);
			((BTreeNode*)r->data)->leaf=false;
			BM_PageHandle *rl=allocateNode(bm,bt);
			((BTreeNode*)r->data)->pageNum[0]=bt->BTreeRoot;
			((BTreeNode*)r->data)->pageNum[1]=rl->pageNum;
			((BTreeNode*)rl->data)->noOfKeys=1;
			((BTreeNode*)r->data)->noOfKeys=1;
			if(child->leaf)
			{	updateKey(child,key,&rid,oKey,orid);
				(((BTreeNode*)rl->data)->keys[0]).v.intV=oKey->v.intV;
				(((BTreeNode*)rl->data)->keys[0]).dt=oKey->dt;
				(((BTreeNode*)r->data)->keys[0]).v.intV=oKey->v.intV;
				(((BTreeNode*)r->data)->keys[0]).dt=oKey->dt;
				((BTreeNode*)rl->data)->recordKeys[0].page=orid->page;
				((BTreeNode*)rl->data)->recordKeys[0].slot=orid->slot;
				child->nextleaf=rl->pageNum;
			}else
			{	
				(((BTreeNode*)rl->data)->keys[0]).v.intV=key->v.intV;
				(((BTreeNode*)rl->data)->keys[0]).dt=key->dt;
				((BTreeNode*)rl->data)->leaf=false;	
				((BTreeNode*)rl->data)->pageNum[0]=child->pageNum[bt->keySize];
				((BTreeNode*)rl->data)->pageNum[1]=existPageNum;
				(((BTreeNode*)r->data)->keys[0]).v.intV=(child->keys[bt->keySize-1]).v.intV;
				(((BTreeNode*)r->data)->keys[0]).dt=(child->keys[bt->keySize-1]).dt;
				child->keys[bt->keySize-1].v.intV=0;
				child->pageNum[bt->keySize]=0;
				child->noOfKeys--;
			}
			bt->BTreeRoot=r->pageNum;
			markDirty(bm,rl);
			unpinPage(bm,rl);
			markDirty(bm,r);
			unpinPage(bm,r);
			
			free(orid);
			free(oKey);
			free(rl);
			free(r);
	}else 
	{		
			BM_PageHandle *r = MAKE_PAGE_HANDLE();
			pinPage(bm,r,bt->stack[counter-1].pageNum);
			BM_PageHandle *rl=allocateNode(bm,bt);
			((BTreeNode*)rl->data)->noOfKeys=1;
			if(child->leaf)
			{	updateKey(child,key,&rid,oKey,orid);
				(((BTreeNode*)rl->data)->keys[0]).v.intV=oKey->v.intV;
				(((BTreeNode*)rl->data)->keys[0]).dt=oKey->dt;
				((BTreeNode*)rl->data)->recordKeys[0].page=orid->page;
				((BTreeNode*)rl->data)->recordKeys[0].slot=orid->slot;
				child->nextleaf=rl->pageNum;
				if(bt->stack[counter-1].index+1<bt->keySize)
				{	((BTreeNode*)rl->data)->nextleaf=((BTreeNode*)r->data)->pageNum[bt->stack[counter-1].index+1];
				}
					
			}else
			{	
				(((BTreeNode*)rl->data)->keys[0]).v.intV=key->v.intV;
				(((BTreeNode*)rl->data)->keys[0]).dt=key->dt;
				((BTreeNode*)rl->data)->leaf=false;	
				((BTreeNode*)rl->data)->pageNum[0]=child->pageNum[bt->keySize];
			}
			
			markDirty(bm,rl);
			unpinPage(bm,rl);
			free(orid);
			free(rl);
			
			if(((BTreeNode *)r->data)->noOfKeys<bt->keySize && child->leaf)
			{	existPageNum=rearrangeIN(((BTreeNode *)r->data),bt->stack[counter-1].index,key,bt->LastPageInUse,oKey);
			}else if(((BTreeNode *)r->data)->noOfKeys<bt->keySize && !(child->leaf))
			{	rearrangeIN(((BTreeNode *)r->data),bt->stack[counter-1].index,&(child->keys[bt->keySize-1]),bt->LastPageInUse,oKey);
				child->noOfKeys--;
				
			}else
			{	
				existPageNum=rearrangeIN((BTreeNode *)r->data,bt->stack[counter-1].index,key,0,oKey);
				counter--;
				splitNode((BTreeNode *)r->data,bm,bt,counter,oKey,rid,existPageNum);
			}
			free(oKey);
			markDirty(bm,r);
			unpinPage(bm,r);
			free(r);
	}
	return RC_OK;	
	
}
/*------------------------------insertKey--------------------------------
Function:   insertKey                                               
Purpose:    To in the key values in the tree structure 
Parameters: IN = tree: pointer to Btree handle
	    IN = key: Value of the key
	    IN = rid: RID of the node
            OUT = RC_OK    :after successfull completion         		   
Returns:    RC_OK: completion status                       
         		                                                        
------------------------------------------------------------------------------*/
extern RC insertKey (BTreeHandle *tree, Value *key, RID rid)
{
	
	BM_PageHandle *h = MAKE_PAGE_HANDLE();
	pinPage((BM_BufferPool *)tree->mgmtData,h,0);
	BTreeInfo *b=(BTreeInfo *)h->data;

	BM_PageHandle *r = MAKE_PAGE_HANDLE();
	pinPage((BM_BufferPool *)tree->mgmtData,r,b->BTreeRoot);
	
	int temp,tempPageNum,counter=0;

	while(!(((BTreeNode *)r->data)->leaf))
	{
		temp=((BTreeNode *)r->data)->noOfKeys;
		while(temp>=1 && ((BTreeNode *)r->data)->keys[temp-1].v.intV>key->v.intV)
		{	temp=temp-1;
		}
		b->stack[counter].pageNum=r->pageNum;
		b->stack[counter].index=temp;
		tempPageNum=((BTreeNode *)r->data)->pageNum[temp];
		unpinPage((BM_BufferPool *)tree->mgmtData,r);
		pinPage((BM_BufferPool *)tree->mgmtData,r,tempPageNum);
		counter++;
	}
	if(((BTreeNode *)r->data)->noOfKeys<b->keySize)
	{
		insertLeafKey((BTreeNode *)r->data,key,rid);
	}else
	{
		splitNode((BTreeNode *)r->data,(BM_BufferPool *)tree->mgmtData,b,counter,key,rid,0);	
	}
	b->noOfEntries++;
	
	markDirty((BM_BufferPool *)tree->mgmtData,h);
	unpinPage((BM_BufferPool *)tree->mgmtData,h);

	free(h);

	markDirty((BM_BufferPool *)tree->mgmtData,r);
	unpinPage((BM_BufferPool *)tree->mgmtData,r);
	free(r);

	return RC_OK;
}

extern RC getNumNodes (BTreeHandle *tree, int *result)
{
	BM_PageHandle *h = MAKE_PAGE_HANDLE();
	pinPage((BM_BufferPool *)tree->mgmtData,h,0);
	BTreeInfo *b=(BTreeInfo *)h->data;
	*result=b->noOfNodes;
	unpinPage((BM_BufferPool *)tree->mgmtData,h);
	free(h);

	return RC_OK;
}	
/*------------------------------reArrangeKeys--------------------------------
Function:   reArrangeKeys                                               
Purpose:    To rearrange the key values in the node(parent) structure in case of overflow
Parameters: IN = child: pointer to Btree node of child that has over flow
	    IN = bm: buffer pool pointer	
	    IN = bt: Pointer to the btree info
	    IN = counter: the counte to determine the depth of node  	
	    OUT = RC_OK    :after successfull completion         		   
Returns:    RC_OK: completion status                       
         		                                                        
------------------------------------------------------------------------------*/
extern RC reArrangeKeys(BTreeNode *child,BTreeInfo *bt,BM_BufferPool *bm,int counter)
{
	int temp=0;
	BM_PageHandle *ll=MAKE_PAGE_HANDLE();
	BM_PageHandle *r=MAKE_PAGE_HANDLE();
	pinPage(bm,r,bt->stack[counter-1].pageNum);

	if(bt->stack[counter-1].index-1>=0)
	{	
		pinPage(bm,ll,((BTreeNode *)r->data)->pageNum[bt->stack[counter-1].index-1]);
	}else
	{	ll->pageNum=-1;
	}
	if(ll->pageNum>0)
	{	if(((BTreeNode *)ll->data)->noOfKeys<bt->keySize)
		{	BM_PageHandle *rl=MAKE_PAGE_HANDLE();
			if(child->nextleaf)
			{	pinPage(bm,rl,child->nextleaf);
				if(((BTreeNode *)rl->data)->noOfKeys<bt->keySize)
				{
				}else{
				child->keys[0].v.intV=((BTreeNode *)rl->data)->keys[0].v.intV;
				child->recordKeys[0].page=((BTreeNode *)rl->data)->recordKeys[0].page;
				child->recordKeys[0].slot=((BTreeNode *)rl->data)->recordKeys[0].slot;
				child->noOfKeys++;
				while(temp<((BTreeNode *)rl->data)->noOfKeys-1)
				{	((BTreeNode *)rl->data)->keys[temp].v.intV=((BTreeNode *)rl->data)->keys[temp+1].v.intV;
					((BTreeNode *)rl->data)->recordKeys[temp].page=((BTreeNode *)rl->data)->recordKeys[temp+1].page;
					((BTreeNode *)rl->data)->recordKeys[temp].slot=((BTreeNode *)rl->data)->recordKeys[temp+1].slot;
					temp=temp+1;
				}
				((BTreeNode *)rl->data)->noOfKeys--;
				((BTreeNode *)r->data)->keys[bt->stack[counter-1].index-1].v.intV=child->keys[0].v.intV;
				((BTreeNode *)r->data)->keys[bt->stack[counter-1].index].v.intV=((BTreeNode *)rl->data)->keys[0].v.intV;
								
				markDirty(bm,rl);
				unpinPage(bm,rl);
				free(rl);
				}
			}else{
				markDirty(bm,rl);
				unpinPage(bm,rl);
				free(rl);
			}
						
		}else
		{	
			child->keys[0].v.intV=((BTreeNode *)ll->data)->keys[bt->keySize-1].v.intV;
			child->recordKeys[0].page=((BTreeNode *)ll->data)->recordKeys[bt->keySize-1].page;
			child->recordKeys[0].slot=((BTreeNode *)ll->data)->recordKeys[bt->keySize-1].slot;
			child->noOfKeys++;
			((BTreeNode *)ll->data)->noOfKeys--;
			((BTreeNode *)r->data)->keys[bt->stack[counter-1].index-1].v.intV=child->keys[0].v.intV;
		}
	}

			markDirty(bm,r);			
			unpinPage(bm,r);
			unpinPage(bm,ll);
			free(r);
			free(ll);
	return RC_OK;
}
/*------------------------------deleteKey--------------------------------
Function:   deleteKey                                               
Purpose:    To rearrange the key values in the node structure in case of overflow
Parameters: IN = tree: pointer to Btree handle
	    IN = key: pointer to the value od the key
	    OUT = RC_OK    :after successfull completion         		   
Returns:    RC_OK: completion status                       
         		                                                        
------------------------------------------------------------------------------*/
extern RC deleteKey (BTreeHandle *tree, Value *key)
{	
	BM_PageHandle *h = MAKE_PAGE_HANDLE();
	pinPage((BM_BufferPool *)tree->mgmtData,h,0);
	BTreeInfo *b=(BTreeInfo *)h->data;

	BM_PageHandle *r = MAKE_PAGE_HANDLE();
	pinPage((BM_BufferPool *)tree->mgmtData,r,b->BTreeRoot);
	
	int temp,tempPageNum,counter=0;
	while(!(((BTreeNode *)r->data)->leaf))
	{
		temp=((BTreeNode *)r->data)->noOfKeys;
		while(temp>=1 && ((BTreeNode *)r->data)->keys[temp-1].v.intV>key->v.intV)
		{	temp=temp-1;
		}
		b->stack[counter].pageNum=r->pageNum;
		b->stack[counter].index=temp;
		tempPageNum=((BTreeNode *)r->data)->pageNum[temp];
		unpinPage((BM_BufferPool *)tree->mgmtData,r);
		pinPage((BM_BufferPool *)tree->mgmtData,r,tempPageNum);
		counter++;
	}
	
	temp=0;
	while(temp<((BTreeNode *)r->data)->noOfKeys && ((BTreeNode *)r->data)->keys[temp].v.intV!=key->v.intV)
	{	temp=temp+1;
	}

	if(temp<((BTreeNode *)r->data)->noOfKeys)
	{	
		while(temp<((BTreeNode *)r->data)->noOfKeys-1)
		{	
			((BTreeNode *)r->data)->keys[temp].v.intV=((BTreeNode *)r->data)->keys[temp+1].v.intV;	
			((BTreeNode *)r->data)->recordKeys[temp].page=((BTreeNode *)r->data)->recordKeys[temp+1].page;
			((BTreeNode *)r->data)->recordKeys[temp].slot=((BTreeNode *)r->data)->recordKeys[temp+1].slot;
			temp++;
		}
		((BTreeNode *)r->data)->noOfKeys--;
	}else
	{	
		markDirty((BM_BufferPool *)tree->mgmtData,h);
		unpinPage((BM_BufferPool *)tree->mgmtData,h);

		markDirty((BM_BufferPool *)tree->mgmtData,r);
		unpinPage((BM_BufferPool *)tree->mgmtData,r);

		free(h);
		free(r);		
		return RC_IM_KEY_NOT_FOUND;
	}

	if(((BTreeNode *)r->data)->noOfKeys<1)
	{	
		reArrangeKeys((BTreeNode *)r->data,b,(BM_BufferPool *)tree->mgmtData,counter);
	}

	b->noOfEntries--;

	markDirty((BM_BufferPool *)tree->mgmtData,h);
	unpinPage((BM_BufferPool *)tree->mgmtData,h);

	markDirty((BM_BufferPool *)tree->mgmtData,r);
	unpinPage((BM_BufferPool *)tree->mgmtData,r);

	free(h);
	free(r);

	return RC_OK;
}
