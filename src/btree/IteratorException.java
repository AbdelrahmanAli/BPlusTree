package btree;

import chainexception.ChainException;

public class IteratorException extends ChainException 
{
	
	public IteratorException()
	  {
	  }
	
	  public IteratorException(String paramString)
	  {
	    super(null, paramString); 
	  } 
	  public IteratorException(Exception paramException, String paramString) 
	  {
		  super(paramException, paramString); 
	  }


}
