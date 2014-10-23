package btree;

import global.GlobalConst;
import global.PageId;
import global.RID;
import global.SystemDefs;
import heap.HFPage;

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

/**
 * BTFileScan implements a search/iterate interface to B+ tree index files
 * (class BTreeFile). It derives from abstract base class IndexFileScan.
 * 
 * @author MaTrix
 * 
 */
public class BTFileScan extends IndexFileScan implements GlobalConst 
{

	BTreeFile btree;	// the called tree
	BTLeafPage currentLeaf; // (initialy) the most left leaf page (initial pin is done in the BTreeFile method)
	RID currentRecord; // (initialy) first record of the left most leaf page
	KeyClass lowKey; // lower bound
	KeyClass highKey; // upper bound
	int keyType; // type of the key
	int maxKeysize; // max length for the key
	
	private boolean getNextCalled = false; // initial call for getNext
	private boolean deletedCalled = false; // if the delete function is called
	private KeyDataEntry currentEntry;

	/**
	 * Iterate once (during a scan).
	 * (1) lowKey = null,highKey = null scan the whole index 
	 * (2) lowKey = null,highKey!= null range scan from min to the highKey 
	 * (3) lowKey!= null, highKey = null range scan from the lowKey to max 
	 * (4) lowKey!= null, highKey!= null, lowKey = highKey exact match ( might not unique) 
	 * (5) lowKey!= null, highKey!= null, lowKey < highKey range scan from lowKey to highKey
	 * @return null if done otherwise next KeyDataEntry
	 */
	@Override
	public KeyDataEntry get_next() 
	{
		try 
		{
			if (currentLeaf != null && btree.getHeaderPage()!= null ) 
			{
				// first call
				if (!getNextCalled) 
				{
					// start from left most record
					if( (lowKey==null && highKey==null) || (lowKey==null && highKey!=null) )
					{
						currentEntry =  currentLeaf.getFirst(currentRecord);
					}
					//start from lowKey record
					else
					{
						// initial value
						currentEntry =  currentLeaf.getFirst(currentRecord);
						while(true)
						{
							//found
							if(BT.keyCompare(lowKey, currentEntry.key) == 0)	break;
							advanceEntry();
						}
					}
					
					// 1st record not found or no records in the page 
					if(currentEntry == null) currentLeaf = null;
					// done 1st call
					getNextCalled = true;
				}
				// calls after the first one ( !deletedCalled because if it is called then current record = next)
				else if(!deletedCalled)
				{
					// from lowKey to the end
					if( (lowKey==null && highKey==null) || (lowKey!= null && highKey == null))	
					{
						advanceEntry();
					}
					// from lowKey to highKey
					else if( (lowKey==null && highKey!=null) || 
							(lowKey!= null && highKey != null && BT.keyCompare(lowKey, highKey)<0) )
					{
						if(BT.keyCompare(highKey, currentEntry.key) >= 0)	advanceEntry();
						// upper bound reached
						else return null;
					}
					// Exactly match (how is this? is there duplicates or what?)
					else								//found
							return null ;
				}
				else deletedCalled = false;
			}
			else throw new IteratorException(new Exception(),"BTFileScan.IteratorException");
		} 
		catch (KeyNotMatchException e) 	
		{	e.printStackTrace();	}
		catch (IteratorException e)
		{
			currentEntry=null;
			System.out.println(">>> IteratorException");	
		}
		
		return currentEntry;
	}
	
	private void advanceEntry()
	{
		try 
		{
			// advance
			currentEntry = currentLeaf.getNext(currentRecord);
	
			// end of current page
			if(currentEntry == null)
			{
				//get id of the next page
				PageId nextPageId = currentLeaf.getNextPage();
				// unpin current page
				SystemDefs.JavabaseBM.unpinPage(currentLeaf.getCurPage(), true);
				// if there is no next page , end of search
				if(nextPageId.pid==-1)
				{
					currentEntry = null;
					currentLeaf = null;
					return;
				}
				//if there is
				HFPage nextPage = new HFPage();
				SystemDefs.JavabaseBM.pinPage(nextPageId, nextPage, false);
				currentLeaf = new BTLeafPage(nextPage, NodeType.LEAF);
				currentEntry =  currentLeaf.getFirst(currentRecord);
			}
		} 
		catch (ReplacerException | PageUnpinnedException
				| HashEntryNotFoundException | InvalidFrameNumberException
				| IOException | HashOperationException | PageNotReadException |
				BufferPoolExceededException | PagePinnedException | BufMgrException e) 
		{	e.printStackTrace();	}
	}

	/**
	 * Delete currently-being-scanned(i.e., just scanned) data entry.
	 */
	@Override
	public void delete_current() 
	{
		if(currentLeaf!=null)
		{
			if(currentEntry==null || !getNextCalled) return;
			
			KeyDataEntry temp = currentLeaf.getCurrent(currentRecord);
			advanceEntry();
			btree.Delete(temp.key, ((LeafData) temp.data).getData());
			deletedCalled = true;
		}
	}

	/**
	 * max size of the key
	 * 
	 * @return the maxumum size of the key in BTFile
	 */
	@Override
	public int keysize() 
	{
		return maxKeysize;
	}

	/**
	 * destructor. unpin some pages if they are not unpinned already. and do
	 * some clearing work.
	 */
	public void DestroyBTreeFileScan() 
	{
		try 
		{
			// if no call done to the getEntry method OR if we stopped at a highKey entry
			if(!getNextCalled || currentEntry!= null)
			{
				SystemDefs.JavabaseBM.unpinPage(currentLeaf.getCurPage(), true);
			}
			
			currentLeaf = null;
		}
		catch (ReplacerException | PageUnpinnedException
				| HashEntryNotFoundException | InvalidFrameNumberException
				| IOException e) 
		{	e.printStackTrace();	}
	}

}
