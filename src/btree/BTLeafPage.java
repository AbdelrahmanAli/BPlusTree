package btree;

import global.PageId;
import global.RID;

import java.io.IOException;

import diskmgr.Page;

public class BTLeafPage extends BTSortedPage 
{

	/**
	 * pin the page with pageno, and get the corresponding BTLeafPage, also it sets the type to be NodeType.LEAF.
	 * @param pageno - Input parameter. To specify which page number the BTLeafPage will correspond to.
	 * @param keyType - either AttrType.attrInteger or AttrType.attrString. Input parameter.
	 * @throws ConstructPageException
	 */
	public BTLeafPage(PageId pageno,int keyType) throws ConstructPageException
	{
		super(pageno,keyType);
	}
	
	/**
	 * associate the BTLeafPage instance with the Page instance, also it sets the type to be NodeType.LEAF.
	 * @param page - input parameter. To specify which page the BTLeafPage will correspond to.
	 * @param keyType - either AttrType.attrInteger or AttrType.attrString. Input parameter.
	 */
	public BTLeafPage(Page page,int keyType)
	{
		super(page,keyType);
	}
	
	/**
	 * new a page, associate the BTLeafPage instance with the Page instance, also it sets the type to be NodeType.LEAF.
	 * @param keyType - either AttrType.attrInteger or AttrType.attrString. Input parameter.
	 * @throws ConstructPageException
	 */
	public BTLeafPage(int keyType) throws ConstructPageException
	{
		super(keyType);
	}
	
	/**
	 * insertRecord. READ THIS DESCRIPTION CAREFULLY.
	 * THERE ARE TWO RIDs WHICH MEAN TWO DIFFERENT THINGS. 
	 * Inserts a key, rid value into the leaf node. 
	 * This is accomplished by a call to SortedPage::insertRecord() Parameters:
	 * @param key - the key value of the data record. Input parameter.
	 * @param dataRid - the rid of the data record. 
	 * 					This is stored on the leaf page along with the corresponding key value. Input parameter.
	 * @return - the rid of the inserted leaf record data entry, i.e., the pair.
	 */
	public RID insertRecord(KeyClass key,RID dataRid)
	{ 
	    KeyDataEntry entry = new KeyDataEntry(key,dataRid);
		try 
		{
			return insertRecord(entry);
		}
		catch (InsertRecException e) 
		{
			e.printStackTrace();
		}
		return null;
	}
	
	/**
	 * Iterators. One of the two functions: getFirst and getNext which 
	 * provide an iterator interface to the records on a BTLeafPage.
	 * @param rid - It will be modified and the first rid in the leaf page will be passed out by itself.
	 * 				Input and Output parameter.
	 * @return - return the first KeyDataEntry in the leaf page. null if no more record
	 */
	public KeyDataEntry getFirst(RID rid)
	{ 
	    try 
	    {
	    	if(firstRecord()!=null)
	    	{
	    		rid.copyRid(firstRecord());
	    		KeyDataEntry entry =  BT.getEntryFromBytes(getpage(),getSlotOffset(rid.slotNo),
		                                        getSlotLength(rid.slotNo),keyType, NodeType.LEAF);
	    		return entry;
	    	}
		}
		catch (IOException | KeyNotMatchException | NodeNotMatchException | ConvertException e) 
		{   e.printStackTrace();    }
		 return null;
	}
	
	/**
	 * Iterators. One of the two functions: getFirst and getNext which 
	 * provide an iterator interface to the records on a BTLeafPage.
	 * @param rid - It will be modified and the next rid will be passed out by itself. Input and Output parameter.
	 * @return - return the next KeyDataEntry in the leaf page. null if no more record.
	 */
	 public KeyDataEntry getNext(RID rid)
	 {
		try 
		{
			if(nextRecord(rid)!=null)
			{
				rid.copyRid(nextRecord(rid));
				KeyDataEntry entry =  BT.getEntryFromBytes(getpage(),getSlotOffset(rid.slotNo),
		                                        getSlotLength(rid.slotNo), keyType, NodeType.LEAF);
		     	return entry;
			}
		}
		catch (IOException | KeyNotMatchException | NodeNotMatchException | ConvertException e) 
		{   e.printStackTrace();    }
		 return null;
	 }
	 
	 /**
	  * getCurrent returns the current record in the iteration; 
	  * it is like getNext except it does not advance the iterator.
	  * @param rid - the current rid. Input and Output parameter. But Output=Input.
	  * @return - return the current KeyDataEntry
	  */
	 public KeyDataEntry getCurrent(RID rid)
	 {
		try 
		{
			if(rid!=null)
			{
				KeyDataEntry entry =  BT.getEntryFromBytes(getpage(),getSlotOffset(rid.slotNo),
		                                        getSlotLength(rid.slotNo), keyType, NodeType.LEAF);
		     return entry;
			}
		}
		catch (IOException | KeyNotMatchException | NodeNotMatchException | ConvertException e) 
		{   e.printStackTrace();    }
		 return null;
	 }
	 
	 /**
	  * delete a data entry in the leaf page.
	  * @param dEntry - the entry will be deleted in the leaf page. Input parameter.
	  * @return - true if deleted; false if no dEntry in the page
	  */
	 public boolean delEntry(KeyDataEntry dEntry)
	 {
	     
		RID currentRecord;
	    KeyDataEntry currentEntry;
		try 
		{   
			if(firstRecord()!=null)
			{
				currentRecord = firstRecord();
			    currentEntry = getFirst(currentRecord);
			    for(int i = 0 ; i < getSlotCnt() ; i++)
			    {
				    if (BT.keyCompare(currentEntry.key, dEntry.key) == 0) break;
					else    currentEntry = getNext(currentRecord);
				   
				    if(currentEntry==null) return false;
				}
				if(currentRecord==null) return false;
				return deleteSortedRecord(currentRecord);
			}
		}
		catch ( DeleteRecException | IOException | KeyNotMatchException e) 
		{
			e.printStackTrace();
		}
		 return false;
	 }
	

}
