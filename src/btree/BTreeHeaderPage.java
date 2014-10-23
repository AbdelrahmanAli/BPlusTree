package btree;

import global.Convert;
import global.PageId;
import global.RID;
import heap.HFPage;
import heap.InvalidSlotNumberException;
import heap.Tuple;

import java.io.IOException;

import diskmgr.Page;

public class BTreeHeaderPage extends HFPage 
{
	private int maxKeyFieldLength,keyType,rootPageId;
	// slot 1 , prev , next

	public BTreeHeaderPage(Page page) {
		super(page);
	}
	
	public BTreeHeaderPage() {
		super();
	}

	public PageId get_rootId() {
		try 
		{
			return getNextPage();
		}
		catch (IOException e) 
		{	e.printStackTrace();	}
		return null;
	}

	public short get_keyType() {
		try 
		{
			return (short) getPrevPage().pid;
		}
		catch (IOException e) 
		{	e.printStackTrace();	}
		return -1;
	}

	public int getMaxKeyFieldLength() {
		try 
		{
			return getSlotLength(1);
		}
		catch (IOException e) {	e.printStackTrace();	}
		return -1;
	}
	
	
	public void setRootPageId(PageId rootPageId) {
		this.rootPageId = rootPageId.pid;
		try 
		{
			setNextPage(rootPageId);
		}
		catch (IOException e) {	e.printStackTrace();	}
	}
	
	public void setKeyType(int keyType) {
		this.keyType = keyType;
		try 
		{
			setPrevPage(new PageId(keyType));
		}
		catch (IOException e) {	e.printStackTrace();	}
	}

	public void setMaxKeyFieldLength(int maxKeyFieldLength) 
	{
		this.maxKeyFieldLength = maxKeyFieldLength;
		try 
		{
			setSlot(1, maxKeyFieldLength, 0);
		}
		catch (IOException e) 
		{	e.printStackTrace();	}
		
	}

	// set instances from the page (RootPageID,TypeOfKey,LengthOfKey)
	public void loadData() {
		try 
		{
			rootPageId = getNextPage().pid;
			keyType =  getPrevPage().pid;
			maxKeyFieldLength = getSlotLength(1);
		}
		catch (IOException e) {	e.printStackTrace();	}
	}

	public void insertAll(PageId rootPageID, int keytype, int keysize) 
	{
		setRootPageId(rootPageID);
	    setKeyType(keytype);
	    setMaxKeyFieldLength(keysize);
	}
}
