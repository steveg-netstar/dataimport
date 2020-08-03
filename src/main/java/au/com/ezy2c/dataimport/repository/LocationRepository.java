package au.com.ezy2c.dataimport.repository;

import au.com.ezy2c.common.DBConnectException;
import au.com.ezy2c.dataimport.AreaType;

public interface LocationRepository {
	/**
	 * 
	 */
	void storeLocations(long fleetId, AreaType areaType) throws DBConnectException, LocationRepositoryException;
}
