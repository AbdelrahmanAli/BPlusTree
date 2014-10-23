package btree;

import global.AttrType;
import global.GlobalConst;
import global.PageId;
import global.RID;
import global.SystemDefs;
import heap.HFPage;
import heap.InvalidSlotNumberException;

import java.io.IOException;

import bufmgr.BufMgrException;
import bufmgr.BufferPoolExceededException;
import bufmgr.HashEntryNotFoundException;
import bufmgr.HashOperationException;
import bufmgr.InvalidFrameNumberException;
import bufmgr.PageNotReadException;
import bufmgr.PagePinnedException;
import bufmgr.PageUnpinnedException;
import bufmgr.ReplacerException;
import diskmgr.DiskMgrException;
import diskmgr.DuplicateEntryException;
import diskmgr.FileEntryNotFoundException;
import diskmgr.FileIOException;
import diskmgr.FileNameTooLongException;
import diskmgr.InvalidPageNumberException;
import diskmgr.InvalidRunSizeException;
import diskmgr.OutOfSpaceException;
import diskmgr.Page;

public class BTreeFile extends IndexFile implements GlobalConst {

	// the id of the header page
	private PageId headerPageID;

	// The header page is used to hold information about the tree as a whole,
	// such as the page id of the
	// root page, the type of the search key, the length of the key field(s),
	// etc.
	private BTreeHeaderPage headerPage;

	private String fileName;

	/**
	 * BTreeFile class an index file with given filename should already exist;
	 * this opens it.
	 * 
	 * @param filename
	 *            - the B+ tree file name. Input parameter.
	 */
	public BTreeFile(String filename) {
		try {
			fileName = filename;
			Page page = new Page();
			// get the header page id
			headerPageID = SystemDefs.JavabaseDB.get_file_entry(filename);
			if(headerPageID!=null)
			{
				// pin the page and save it in headerPage
				SystemDefs.JavabaseBM.pinPage(headerPageID, page, false);
				headerPage = new BTreeHeaderPage(page);
				// set headerPage instances (RootPageID,TypeOfKey,LengthOfKey)
				headerPage.loadData();
				// set page type
				headerPage.setType(NodeType.BTHEAD);
			}
		} catch (FileIOException | InvalidPageNumberException
				| DiskMgrException | IOException | ReplacerException
				| HashOperationException | PageUnpinnedException
				| InvalidFrameNumberException | PageNotReadException
				| BufferPoolExceededException | PagePinnedException
				| BufMgrException e) {
			e.printStackTrace();
		}
	}

	/**
	 * if index file exists, open it; else create it.
	 * 
	 * @param filename
	 *            - Input parameter.
	 * @param keytype
	 *            - the type of key. Input parameter.
	 * @param keysize
	 *            - the maximum size of a key. Input parameter.
	 * @param delete_fashion
	 *            - full delete or naive delete. You can pass zero in this
	 *            parameter.
	 */
	public BTreeFile(String filename, int keytype, int keysize,int delete_fashion) {
		try {
			fileName = filename;
			Page page = new Page();
			// get the header page id
			headerPageID = SystemDefs.JavabaseDB.get_file_entry(filename);
			if (headerPageID != null) // file exist
			{
				// pin the page and save it in headerPage
				SystemDefs.JavabaseBM.pinPage(headerPageID, page, false);
				headerPage = new BTreeHeaderPage(page);
				// set headerPage instances (RootPageID,TypeOfKey,LengthOfKey)
				headerPage.loadData();
				headerPage.setType(NodeType.BTHEAD);
			} 
			else // file doesn't exist then create it
			{
				// create a new page and pin it
				headerPageID = SystemDefs.JavabaseBM.newPage(page, 1);
				// create the file and save the header page
				SystemDefs.JavabaseDB.add_file_entry(filename, headerPageID);
				headerPage = new BTreeHeaderPage();
				headerPage.init(headerPageID, page); // initialize next , prev
														// and free space

				// add attributes to the header page
				// (RootPageID,TypeOfKey,LengthOfKey,delete_fashion)
				// create root page (Leaf Page)
				BTLeafPage tempPage = new BTLeafPage(keytype);
				PageId rootPageID = SystemDefs.JavabaseBM.newPage(tempPage, 1);
				tempPage.init(rootPageID,tempPage);
				tempPage.setType(NodeType.LEAF);
				headerPage.insertAll(rootPageID, keytype, keysize);

				headerPage.setType(NodeType.BTHEAD);
				SystemDefs.JavabaseBM.unpinPage(rootPageID, true);
			}
		} catch (FileIOException | InvalidPageNumberException
				| DiskMgrException | IOException | ReplacerException
				| HashOperationException | PageUnpinnedException
				| InvalidFrameNumberException | PageNotReadException
				| BufferPoolExceededException | PagePinnedException
				| BufMgrException | HashEntryNotFoundException
				| FileNameTooLongException | InvalidRunSizeException
				| DuplicateEntryException | OutOfSpaceException
				| ConstructPageException e) {
			e.printStackTrace();
		}
	}

	/**
	 * Close the B+ tree file. Unpin header page.
	 */
	public void close() {
		try {
			if (headerPage != null) {
				SystemDefs.JavabaseBM.unpinPage(headerPageID, true);
				headerPage = null;
			}
		} catch (ReplacerException | PageUnpinnedException
				| HashEntryNotFoundException | InvalidFrameNumberException e) {
			e.printStackTrace();
		}
	}

	/**
	 * Destroy entire B+ tree file.
	 */
	public void destroyFile() {

		try 
		{
			if (headerPage != null) 
			{
				close();
				SystemDefs.JavabaseDB.delete_file_entry(fileName);
			}
		} catch (FileEntryNotFoundException | FileIOException
				| InvalidPageNumberException | DiskMgrException | IOException e) {
			e.printStackTrace();
		}
	}

	/**
	 * insert record with the given key and rid
	 * 
	 * @param key
	 *            - the key of the record. Input parameter.
	 * @param rid
	 *            - the rid of the record. Input parameter.
	 */
	@Override
	public void insert(KeyClass key, RID rid) {
		try {
			if (headerPage != null) 
			{
    			// check if the length of the key value is less than the max allowed
    			// key length value
    			if (BT.getKeyLength(key) > headerPage.getMaxKeyFieldLength())
    				throw new KeyTooLongException(null, "");
    
    			// attrString = 0 , attrInteger = 1
    			// check if the type of the key value is the same as the B+ Tree
    			if (key instanceof IntegerKey) {
    				if (headerPage.get_keyType() != AttrType.attrInteger)
    					throw new KeyNotMatchException(null, "");
    			} else if (key instanceof StringKey) {
    				if (headerPage.get_keyType() != AttrType.attrString)
    					throw new KeyNotMatchException(null, "");
    			} else
    				throw new KeyNotMatchException(null, "");
    
    			// search to find the required leaf page
    			// load the root page
    			HFPage page = new HFPage();
    			SystemDefs.JavabaseBM.pinPage(headerPage.get_rootId(), page, false);
    			BTSortedPage rootPage = new BTSortedPage(page,headerPage.get_keyType());

    			//insert
    			KeyDataEntry entry = new KeyDataEntry(key,rid);
    			splittedInserstion(rootPage,entry);
    			//rootPage.dumpPage();
			}
		} 
		catch (KeyTooLongException | IOException | KeyNotMatchException
				| ReplacerException | HashOperationException
				| PageUnpinnedException | InvalidFrameNumberException
				| PageNotReadException | BufferPoolExceededException
				| PagePinnedException | BufMgrException e) {
			e.printStackTrace();
		}
	}
	private KeyDataEntry splittedInserstion(BTSortedPage currentPage , KeyDataEntry currentEntry)
	{
		try 
		{
			KeyDataEntry returnedEntry;
			// if the page is an index page
			if(currentPage.getType() == NodeType.INDEX)
			{
				// set the page as index page object
				BTIndexPage parentPage = new BTIndexPage(currentPage,headerPage.get_keyType());
				// find child page id ************************************
				PageId childPageId = parentPage.getPageNoByKey(currentEntry.key);
				//load child page and pin it
				HFPage childLoader = new HFPage();
				SystemDefs.JavabaseBM.pinPage(childPageId, childLoader, false);
				BTSortedPage childPage = new BTSortedPage(childLoader, headerPage.get_keyType());
				
				// recursive call
				returnedEntry = splittedInserstion(childPage, currentEntry);
				
				// no insertion is required if (returnedEntry == null) 	
				
				// try to insert in the parent page, if successful then return null else enter the else if 
			    if(returnedEntry!=null && parentPage.insertKey(returnedEntry.key,((IndexData) (returnedEntry.data)).getData())==null)
				{
					// split page
					BTIndexPage splittedPage = new BTIndexPage(splitIndex(parentPage),headerPage.get_keyType());

					//find where the push up entry should be inserted
					// left , right , up
					boolean[] location = findLocation(parentPage,splittedPage,returnedEntry);
					// insert returnedEntry and handle pointers
					if(location[2])
					{
					    splittedPage.setPrevPage(((IndexData) returnedEntry.data).getData());
					    returnedEntry = new KeyDataEntry(returnedEntry.key,splittedPage.getCurPage());
					}
					else if (location[1])
					{
					    RID tempRecord = new RID();
					    KeyDataEntry pushUpEntry = splittedPage.getFirst(tempRecord);
					    splittedPage.deleteSortedRecord(tempRecord);
					    splittedPage.insertKey(returnedEntry.key,((IndexData) returnedEntry.data).getData());
                        splittedPage.setPrevPage(((IndexData) pushUpEntry.data).getData());//4
                        returnedEntry = new KeyDataEntry(pushUpEntry.key,splittedPage.getCurPage());//3
					}
					else if (location [0])
					{
					    RID tempRecord = new RID ();
                	    KeyDataEntry pushUpEntry = parentPage.getFirst(tempRecord);
                	    // get right most data entry in the parent page (left)
                        for (int i = 0; i < parentPage.numberOfRecords()-1 ; i++) 
                        {  pushUpEntry = parentPage.getNext(tempRecord);  }
                        
                        parentPage.deleteSortedRecord(tempRecord);
					    parentPage.insertKey(returnedEntry.key,((IndexData) returnedEntry.data).getData());
                        splittedPage.setPrevPage(((IndexData) pushUpEntry.data).getData());//5
                        returnedEntry = new KeyDataEntry(pushUpEntry.key,splittedPage.getCurPage());//4
					}

					//if the page is a root page
					if(parentPage.getCurPage().pid == headerPage.get_rootId().pid)
					{
						// set the new root and pin it
						BTIndexPage rootPage = new BTIndexPage(headerPage.get_keyType());
						rootPage.init(rootPage.getCurPage(),rootPage);
						rootPage.setType(NodeType.INDEX);
						// insert push up
						rootPage.insertKey(returnedEntry.key ,((IndexData) returnedEntry.data).getData());
						// handle pointers
						rootPage.setPrevPage(parentPage.getCurPage());
						// set root in header page
						headerPage.setRootPageId(rootPage.getCurPage());
						// set return = null
						returnedEntry = null;
						// unpin root page 
						SystemDefs.JavabaseBM.unpinPage(rootPage.getCurPage(),true);
					}
					SystemDefs.JavabaseBM.unpinPage(splittedPage.getCurPage(),true);
				}
				else returnedEntry = null;
				SystemDefs.JavabaseBM.unpinPage(parentPage.getCurPage(),true);
				return returnedEntry;
			}
			// if the page is a leaf page
			else if(currentPage.getType() == NodeType.LEAF)
			{
				KeyDataEntry copyUpEntry = null ;
				// load the page as leaf page object
				BTLeafPage leafPage = new BTLeafPage(currentPage,headerPage.get_keyType());
				//System.out.println(">>> "+currentEntry.key +" "+currentEntry.data);
				// try to insert , enter the if when a split is required
				if(leafPage.insertRecord(currentEntry.key,((LeafData) currentEntry.data).getData())==null)
				{
				    // split page
				    BTLeafPage splittedPage =  new BTLeafPage(splitLeaf(leafPage),headerPage.get_keyType());
				    
				    // handle page pointers
				    splittedPage.setPrevPage(leafPage.getCurPage());
				    PageId nextId = leafPage.getNextPage();
				    if(nextId.pid!=-1)
				    {
				        HFPage loadNextPage = new HFPage();
				        SystemDefs.JavabaseBM.pinPage(nextId, loadNextPage, false);
				        BTLeafPage nextPage = new BTLeafPage(loadNextPage, loadNextPage.getType());
				        nextPage.setPrevPage(splittedPage.getCurPage());
				        SystemDefs.JavabaseBM.unpinPage(nextPage.getCurPage(),true);
				    }
				    splittedPage.setNextPage(nextId);
				    leafPage.setNextPage(splittedPage.getCurPage());
				    // detect insertion location ****************************
				    boolean[] location = findLocation(leafPage,splittedPage,currentEntry);
				    
				    // insert right
				    if(location[1])
				    {
					   splittedPage.insertRecord(currentEntry.key,((LeafData) currentEntry.data).getData());
				    }
				    // insert left
				    else if (location[0])
				    {
				       leafPage.insertRecord(currentEntry.key,((LeafData) currentEntry.data).getData());
				    }
				    RID tempRecord = new RID();
				    copyUpEntry = new KeyDataEntry(splittedPage.getFirst(tempRecord).key
					                                            ,splittedPage.getCurPage());
					
					// if  its the root                                
					if(leafPage.getCurPage().pid == headerPage.get_rootId().pid)
					{
						// set the new root and pin it
						BTIndexPage rootPage =  new BTIndexPage(headerPage.get_keyType());
						rootPage.init(rootPage.getCurPage(),rootPage);
						rootPage.setType(NodeType.INDEX);
						// insert copy up
						rootPage.insertKey(copyUpEntry.key ,((IndexData) copyUpEntry.data).getData());
						// handle pointers
						rootPage.setPrevPage(leafPage.getCurPage());
						// set root in header page
						headerPage.setRootPageId(rootPage.getCurPage());
						// set return = null
						copyUpEntry = null;
						// unpin root page 
						SystemDefs.JavabaseBM.unpinPage(rootPage.getCurPage(),true);
					}
                   SystemDefs.JavabaseBM.unpinPage(splittedPage.getCurPage(),true);
				}
				SystemDefs.JavabaseBM.unpinPage(leafPage.getCurPage(),true);
				return copyUpEntry;
			}

		}
		catch (IOException | ReplacerException | PageUnpinnedException | InvalidFrameNumberException | HashOperationException | PageNotReadException | BufferPoolExceededException | PagePinnedException | BufMgrException | DeleteRecException | HashEntryNotFoundException | ConstructPageException e) 	{	e.printStackTrace();	}
		return currentEntry;
	}
	
	
	private BTIndexPage splitIndex(BTIndexPage parentPage)
	{
        BTIndexPage splittedPage = null;
		try 
		{
            	// split page
				splittedPage= new BTIndexPage(headerPage.get_keyType()); // make a new page and pin it
				splittedPage.init(splittedPage.getCurPage(),splittedPage);
				splittedPage.setType(NodeType.INDEX);
				
				// take records from the parent page until it's halve full
				RID iteratorRecord = parentPage.firstRecord();
                KeyDataEntry currentEntry = parentPage.getFirst(iteratorRecord);
				//get the left most record of the splitted page 
				//(if 4 then start from the 3rd , if 3 then from 2st)
				for (int i = 0; i < parentPage.numberOfRecords()/2 ; i++) 
				{	 currentEntry = parentPage.getNext(iteratorRecord);	}
				
				// insert those records in the splited page
				for (int i = parentPage.numberOfRecords()/2; i < parentPage.numberOfRecords(); i++) 
				{
					splittedPage.insertKey(currentEntry.key , ((IndexData) currentEntry.data).getData());
				    currentEntry = parentPage.getNext(iteratorRecord);
				}
				
				//delete entries from 1st page
			    int boundry = parentPage.numberOfRecords();
				for (int i =boundry/2; i < boundry; i++) 
				{
				    iteratorRecord = getLastRecord(parentPage);
					parentPage.deleteSortedRecord(iteratorRecord);
				}
		} catch (IOException | ConstructPageException | DeleteRecException  e) {
			e.printStackTrace();
		}
		return splittedPage;
	}
	
	private BTLeafPage splitLeaf(BTLeafPage parentPage)
	{
        BTLeafPage splittedPage = null;
		try 
		{
            	// split page
				splittedPage= new BTLeafPage(headerPage.get_keyType()); // make a new page and pin it
				splittedPage.init(splittedPage.getCurPage(),splittedPage);
				splittedPage.setType(NodeType.LEAF);
				
				// take records from the parent page until it's halve full
				RID iteratorRecord = parentPage.firstRecord();
                KeyDataEntry currentEntry = parentPage.getFirst(iteratorRecord);
                
				//get the left most record of the splitted page 
				//(if 4 then start from the 3rd , if 3 then from 2st)
				for (int i = 0; i < parentPage.numberOfRecords()/2  ; i++) 
				{	 currentEntry = parentPage.getNext(iteratorRecord);	}
				
				// insert those records in the splited page
				for (int i = parentPage.numberOfRecords()/2; i < parentPage.numberOfRecords(); i++) 
				{
					splittedPage.insertRecord(currentEntry.key , ((LeafData) currentEntry.data).getData());
				    currentEntry = parentPage.getNext(iteratorRecord);
				}
				
				//delete entries from 1st page
				int boundry = parentPage.numberOfRecords();
				for (int i =boundry/2; i < boundry; i++) 
				{
				    currentEntry = getLastEntry(parentPage);
					parentPage.delEntry(currentEntry);
				}
					
		} catch (IOException | ConstructPageException  e) {
			e.printStackTrace();
		}
		return splittedPage;
	}

	private KeyDataEntry getLastEntry(BTLeafPage page)
	{
	    RID iteratorRecord = new RID();
	    KeyDataEntry currentEntry = page.getFirst(iteratorRecord);
	    KeyDataEntry nextEntry = page.getNext(iteratorRecord);
	    while(nextEntry!= null)
	    {
	        currentEntry = page.getCurrent(iteratorRecord);
	        nextEntry = page.getNext(iteratorRecord);
	    }
	    return currentEntry;
	}
	
	private RID getLastRecord(BTIndexPage page)
	{
	    RID currentRecord = null;
		try {
			currentRecord = page.firstRecord();

		    RID nextRecord = page.nextRecord(currentRecord);
		    while(nextRecord!= null)
		    {
		        currentRecord = page.nextRecord(currentRecord);
		        nextRecord = page.nextRecord(nextRecord);
		    }
		    return currentRecord;
		    
		} catch (IOException e) {
			// TODO Auto-generated catch block
			e.printStackTrace();
		}
	    return currentRecord;
	}

	
	
	private boolean[] findLocation(BTSortedPage parentPage,BTSortedPage splittedPage , KeyDataEntry entry)
	{
	    boolean[] location = new boolean[3];
		try
		{
			RID leftRecord = parentPage.firstRecord();
			RID rightRecord = splittedPage.firstRecord();
	        
		    if(parentPage.getType() == NodeType.INDEX)
		    {
		    	BTIndexPage indexPage = new BTIndexPage(parentPage,headerPage.get_keyType());
				indexPage.setType(NodeType.INDEX);

		    	BTIndexPage splittedIndexPage = new BTIndexPage(splittedPage,headerPage.get_keyType());
				splittedIndexPage.setType(NodeType.INDEX);

		    	KeyDataEntry leftData = indexPage.getFirst(leftRecord);
			    // get right most data entry in the parent page (left)
		        for (int i = 0; i < parentPage.numberOfRecords()-1 ; i++) 
		        {   leftData = indexPage.getNext(leftRecord);  }
			    KeyDataEntry rightData = splittedIndexPage.getFirst(rightRecord);
		        
		        // insert to the left page
	            if (BT.keyCompare(entry.key, leftData.key) < 0  && BT.keyCompare(entry.key, rightData.key) < 0 )
	            {
	                location[0] = true;
	                return location;
	            }
		        // insert to the right page
	            else if(BT.keyCompare(entry.key, leftData.key) > 0 && BT.keyCompare(entry.key, rightData.key) > 0  )
	            {
	                location[1] = true;
	                return location;
	            }
	            // insert to the upper page
	            else 
	            {
	                location[2] = true;
	                return location;
	            }
		    }
		    else if(parentPage.getType() == NodeType.LEAF)
		    {
		    	BTLeafPage leafPage = new BTLeafPage(parentPage,headerPage.get_keyType());
				leafPage.setType(NodeType.LEAF);

		    	BTLeafPage splittedleafPage = new BTLeafPage(splittedPage,headerPage.get_keyType());
				splittedleafPage.setType(NodeType.LEAF);

		    	KeyDataEntry leftData = leafPage.getFirst(leftRecord);
			    // get right most data entry in the parent page (left)
		        for (int i = 0; i <  parentPage.numberOfRecords()-1 ; i++) 
		        {   leftData = leafPage.getNext(leftRecord);  }
			    KeyDataEntry rightData = splittedleafPage.getFirst(rightRecord);
		    	
				// insert to the left page if less than right
				if (BT.keyCompare(entry.key, rightData.key) < 0) 
				{
				    location[0] = true;
				    return location ;
				}
		        // insert to the right page if greater than left or equal to right
				else if (BT.keyCompare(entry.key, leftData.key) > 0 || BT.keyCompare(entry.key, rightData.key) == 0) 
				{
				   	location[1]= true;
				   	return location;
				}
		    }
		} catch (IOException | KeyNotMatchException  e) {
			e.printStackTrace();
		}
	        return location;
	}


    // unpin the root
	private BTLeafPage search(KeyClass key, RID rid,BTIndexPage rootPage) 
	{
		BTSortedPage currentPage = null;
		try 
		{
		    PageId currentPageId = rootPage.getPageNoByKey(key);
		    //unpin root
		    
				SystemDefs.JavabaseBM.unpinPage(rootPage.getCurPage(),false);
			
		    // load retrieved page by id
	        currentPage = new BTSortedPage(currentPageId,headerPage.get_keyType()); // pin it
	        
	        while(currentPage.getType() == NodeType.INDEX)
	        {
	            BTIndexPage currentIndexPage = new BTIndexPage(currentPage,headerPage.get_keyType());
	            currentPageId = currentIndexPage.getPageNoByKey(key);
	            SystemDefs.JavabaseBM.unpinPage(currentPage.getCurPage(),false);
	            currentPage = new BTSortedPage(currentPageId,headerPage.get_keyType()); // pin it
	        }
		} 
		catch (ReplacerException | PageUnpinnedException| HashEntryNotFoundException | InvalidFrameNumberException| IOException | ConstructPageException e) 
		{	e.printStackTrace();	}
        return (new BTLeafPage(currentPage , headerPage.get_keyType())  );
	}

	/**
	 * delete leaf entry given its pair. `rid' is IN the data entry; it is not
	 * the id of the data entry)
	 * 
	 * @param key	- the key in pair . Input Parameter.
	 * @param rid	- the rid in pair . Input Parameter.
	 * @return true if deleted. false if no such record.
	 */
	@Override
	public boolean Delete(KeyClass key, RID rid) 
	{
		
		// search the page and delete the record
        boolean deleted = false;
		 try 
		 {
			if (headerPage != null) 
			{
		     HFPage page = new HFPage();
	    	 SystemDefs.JavabaseBM.pinPage(headerPage.get_rootId(), page, false);
	         BTSortedPage rootPage = new BTSortedPage(page,headerPage.get_keyType());
			 if(rootPage.getType() == NodeType.INDEX)
			 {
    			// search for the key in the tree and get it's page
				 BTIndexPage theRootPage = new BTIndexPage(rootPage,headerPage.get_keyType());
				 // returned page is pinned from search method
    			 BTLeafPage requiredPage = new BTLeafPage(search(key, rid,theRootPage),headerPage.get_keyType());
    			// initial value (first record in the page ) 
    			RID currentRecord = requiredPage.firstRecord();
    			KeyDataEntry currentEntry = requiredPage.getFirst(currentRecord);
    			
    			for (int i = 0; i < requiredPage.getSlotCnt(); i++) {
    				
    				// check if the currentEntry is the desired one 
    				if( currentEntry != null && BT.keyCompare( key , currentEntry.key) == 0)	
    				{	
    						deleted =	requiredPage.delEntry(currentEntry);
                            break;
    				}
    				else if(currentEntry != null)
    				{
    					// advance record 
    					currentEntry = requiredPage.getNext(currentRecord);
    				}
    				else
    				{
    				    deleted = false ;
    				    break;
    				}
    			}
    			
    			SystemDefs.JavabaseBM.unpinPage(requiredPage.getCurPage(),true);
			 }
			 else 	
			 {
			    	deleted = rootPage.deleteSortedRecord(rid);
			        SystemDefs.JavabaseBM.unpinPage( rootPage.getCurPage(),true);	
			 }
		 }
			
		} catch (IOException | KeyNotMatchException | DeleteRecException | ReplacerException | HashOperationException | PageUnpinnedException | InvalidFrameNumberException | PageNotReadException | BufferPoolExceededException | PagePinnedException | BufMgrException | HashEntryNotFoundException  e) {
			e.printStackTrace();
		}
    	return deleted;
	}

	/**
	 * create a scan with given keys Cases: 
	 * (1) lo_key = null,hi_key = null scan the whole index 
	 * (2) lo_key = null,hi_key!= null range scan from min to the hi_key 
	 * (3) lo_key!= null, hi_key = null range scan from the lo_key to max 
	 * (4) lo_key!= null, hi_key!= null, lo_key = hi_key exact match ( might not unique) 
	 * (5) lo_key!= null, hi_key!= null, lo_key < hi_key range scan from lo_key to hi_key
	 * @param lo_key - the key where we begin scanning. Input parameter.
	 * @param hi_key - the key where we stop scanning. Input parameter.
	 * @return
	 */
	public BTFileScan new_scan(KeyClass lo_key, KeyClass hi_key) 
	{
		BTFileScan scanner = null;
		if (headerPage != null) 
    	{
    		scanner = new BTFileScan();
    		try 
    		{
    			scanner.btree = this;
    			scanner.highKey = hi_key;
    			scanner.lowKey = lo_key;
    			scanner.keyType = headerPage.get_keyType();
    			scanner.maxKeysize = headerPage.getMaxKeyFieldLength();
    			
    			HFPage page = new HFPage();
    			SystemDefs.JavabaseBM.pinPage(headerPage.get_rootId(), page, false);
    			BTSortedPage currentPage = new BTSortedPage(page, headerPage.get_keyType());
    			PageId currentPageId = new PageId();
    			while(currentPage.getType()==NodeType.INDEX)
    			{
	                BTIndexPage currentIndexPage = new BTIndexPage(currentPage,headerPage.get_keyType());
	                currentPageId = currentIndexPage.getLeftLink();
	                SystemDefs.JavabaseBM.unpinPage(currentPage.getCurPage(),false);
	                currentPage = new BTSortedPage(currentPageId,headerPage.get_keyType()); //
    			}
    			scanner.currentLeaf = new BTLeafPage(currentPage,headerPage.get_keyType());
    			scanner.currentRecord = scanner.currentLeaf.firstRecord();
    			
    		} 
    		catch (ReplacerException | HashOperationException
    				| PageUnpinnedException | InvalidFrameNumberException
    				| PageNotReadException | BufferPoolExceededException
    				| PagePinnedException | BufMgrException | IOException | HashEntryNotFoundException 
    				| ConstructPageException e) 
    		{	e.printStackTrace();	}
    	}
		return scanner;
	}

	public BTreeHeaderPage getHeaderPage() 
	{
		return headerPage;
	}

}