package btree;

import global.PageId;
import global.RID;

import java.io.IOException;
import diskmgr.Page;



public class BTIndexPage extends BTSortedPage
{
	
	/**
	 * pin the page with pageno, and get the corresponding BTIndexPage,
	 * also it sets the type of node to be NodeType.INDEX.
	 * @param pageno - Input parameter. To specify which page number the BTIndexPage will correspond to.
	 * @param keyType - either AttrType.attrInteger or AttrType.attrString. Input parameter.
	 * @throws ConstructPageException
	 */
	 public BTIndexPage(PageId pageno,	int keyType) throws ConstructPageException
	 {
		 super(pageno,keyType);
	 }
	 
	 /**
	  * associate the BTIndexPage instance with the Page instance, also it sets the type of node to be NodeType.INDEX.
	  * it doesn't pin a page
	  * @param page - input parameter. To specify which page the BTIndexPage will correspond to.
	  * @param keyType - either AttrType.attrInteger or AttrType.attrString. Input parameter.
	  */
	 public BTIndexPage(Page page,	int keyType)
	 {
		 super(page,keyType);
	 }
	 
	 public BTIndexPage(int keyType) throws ConstructPageException
	 {
		 super(keyType);
	 }
	 
	 /**
	  * It inserts a value into the index page,
	  * @param key - the key value in . Input parameter.
	  * @param pageNo - the pageNo in . Input parameter.
	  * @return It returns the rid where the record is inserted; null if no space left.
	  */
	 public RID insertKey(KeyClass key,	PageId pageNo)
	 {
	     KeyDataEntry entry = new KeyDataEntry(key,pageNo);
		try 
		{
			return super.insertRecord(entry);
		}
		catch (InsertRecException e) 
		{
			e.printStackTrace();
		}
		return null;
	 }

	 public PageId getPageNoByKey(KeyClass key)
	 {
	    RID currentRecord,nextRecord;
		KeyDataEntry currentEntry,nextEntry;
		try 
		{
			currentRecord = new RID();
			nextRecord = new RID();
		    currentEntry = getFirst(currentRecord);
		    nextEntry = getFirst(currentRecord);
		    boolean firstEntry = true;
		    for(int i = 0 ; i < getSlotCnt()+1 ; i++)
		    {
		        // get the id of the left most page
			    if (firstEntry && BT.keyCompare(key, currentEntry.key) < 0) 
			    {
					return getLeftLink();
				}
				// advance next entry
			    else if (firstEntry)
			    {
			    	  nextEntry = getNext(nextRecord);
			    	  firstEntry=false;
			    }
			    //get the id of the page
			    else if (nextEntry == null || BT.keyCompare(key, nextEntry.key) < 0) 
			    {
					return  ((IndexData) currentEntry.data).getData();
				}
				// advance entries
				else
				{
					nextEntry = getNext(nextRecord);
					currentEntry = getNext(currentRecord);
				}
			    
			}
		
		} catch (IOException | KeyNotMatchException e) {
			e.printStackTrace();
		}
		return null;
	 }
	 
	 /**
	  * Iterators. One of the two functions: 
	  * getFirst and getNext which provide an iterator interface to the records on a BTIndexPage.
	  * @param rid - It will be modified and the first rid in the index page will be passed out by itself.
	  *  			 Input and Output parameter.
	  * @return return the first KeyDataEntry in the index page. null if NO MORE RECORD
	  */
	 public KeyDataEntry getFirst(RID rid)
	 {
	     try {
	    	 if(firstRecord()!=null)
			 {
	    		 rid.copyRid(firstRecord());
	    		 KeyDataEntry entry =  BT.getEntryFromBytes(getpage(),getSlotOffset(rid.slotNo),
		                                        getSlotLength(rid.slotNo), keyType, NodeType.INDEX);
	    		 return entry;
			 }
		}
		catch (IOException | KeyNotMatchException | NodeNotMatchException | ConvertException e) 
		{   e.printStackTrace();    }
		 return null;
	 }
	 
	 /**
	  * Iterators. One of the two functions: 
	  * getFirst and getNext which provide an iterator interface to the records on a BTIndexPage.
	  * @param rid - It will be modified and next rid will be passed out by itself. Input and Output parameter.
	  * @return - return the next KeyDataEntry in the index page. null if no more record
	  */
	 public KeyDataEntry getNext(RID rid)
	 {
		try 
		{
			if(nextRecord(rid)!=null)
			{
				rid.copyRid(nextRecord(rid));
				KeyDataEntry entry =  BT.getEntryFromBytes(getpage(),getSlotOffset(rid.slotNo),
		                                        getSlotLength(rid.slotNo), keyType, NodeType.INDEX);
				return entry;
			}
		}
		catch (IOException | KeyNotMatchException | NodeNotMatchException | ConvertException e) 
		{   e.printStackTrace();    }
		 return null;
	 }
	 
	 /**
	  * Left Link You will recall that the index pages have a left-most pointer that is followed 
	  * whenever the search key value is less than the least key value in the index node.
	  * The previous page pointer is used to implement the left link.
	  * @return - It returns the left most link.
	  */
	 public PageId getLeftLink()
	 {
		 try 
		 {	return getPrevPage();   }
		catch (IOException e) { e.printStackTrace();    }
		return null;
	 }
	 
	 /**
	  * You will recall that the index pages have a left-most pointer that is followed
	  * whenever the search key value is less than the least key value in the index node.
	  * The previous page pointer is used to implement the left link.
	  * The function sets the left link.
	  * @param left - the PageId of the left link you wish to set. Input parameter.
	  */
	 public void setLeftLink(PageId left)
	 {
		try 
		{	setPrevPage(left);	}
		catch (IOException e) 
		{ 	e.printStackTrace();	}
	 }


}
