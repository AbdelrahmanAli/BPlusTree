package btree;
import chainexception.ChainException;
public class KeyTooLongException extends ChainException {

	public KeyTooLongException()
	  {
	  }
	
	  public KeyTooLongException(String paramString)
	  {
	    super(null, paramString); 
	  } 
	  public KeyTooLongException(Exception paramException, String paramString) 
	  {
		  super(paramException, paramString); 
	  }


}
