package au.com.ezy2c.dataimport.repository;

public class LocationRepositoryException extends Exception {
	private static final long serialVersionUID = -96872928052237641L;

	public LocationRepositoryException(String msg, Exception ex) {
		super(msg,ex);
	}
}
