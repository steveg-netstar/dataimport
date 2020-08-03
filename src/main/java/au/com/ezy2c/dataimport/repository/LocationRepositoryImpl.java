package au.com.ezy2c.dataimport.repository;

import java.math.BigDecimal;
import java.sql.Connection;
import java.sql.PreparedStatement;
import java.sql.ResultSet;
import java.sql.SQLException;
import java.util.List;
import java.util.logging.Level;
import java.util.logging.Logger;

import org.springframework.beans.factory.annotation.Value;
import org.springframework.cloud.context.config.annotation.RefreshScope;
import org.springframework.stereotype.Component;

import au.com.ezy2c.common.DBConnectException;
import au.com.ezy2c.common.DBConnector;
import au.com.ezy2c.dataimport.AreaType;

@Component
@RefreshScope
public class LocationRepositoryImpl implements LocationRepository {
	static Logger logger = Logger.getLogger(LocationRepositoryImpl.class.getSimpleName());
	
	protected String mysqlurlstart;
	protected String connectionAttributes;
	protected String locationHost;
	protected int locationPort;
	protected String locationUsername;
	protected String locationPassword;
	protected String locationSchema;
	protected String url;
	
	public LocationRepositoryImpl( 
			@Value("${mysqlurlstart}") String mysqlurlstart,
			@Value("${connectionAttributes}") String connectionAttributes,
			@Value("${locationDatabase.host}") String locationHost,
			@Value("${locationDatabase.port}") int locationPort,
			@Value("${locationDatabase.username}") String locationUsername,
		    @Value("${locationDatabase.password}") String locationPassword,
			@Value("${locationDatabase.schema}") String locationSchema) {
		this.mysqlurlstart = mysqlurlstart;
		this.connectionAttributes = connectionAttributes;
		this.locationHost = locationHost;
		this.locationPort = locationPort;
		this.locationUsername = locationUsername;
		this.locationPassword = locationPassword;
		this.locationSchema = locationSchema;
    	String locationDbName = locationHost+":"+locationPort+"/"+locationSchema;
    	String locationDbURLParams = "?user="+locationUsername+"&password="+locationPassword+"&"+connectionAttributes;
		url = mysqlurlstart+locationDbName+locationDbURLParams;
	}
	/**
	 * Returns false if it was unable to store it.
	 * Returns true if successfully stored OR the location already existed
	 */
	@Override
	public void storeLocations(long fleetId, AreaType areaType) throws DBConnectException, LocationRepositoryException {
		Connection connection = null;
		try {
			connection = DBConnector.getConnection(url);
			
			delete(connection, fleetId, areaType);
			
			PreparedStatement ps = null;
			ResultSet rs = null;
			String sql = "SELECT id, LocationName, SpeedLimit, IsSafe, SequenceNo, latitude, longitude "+
						 "FROM LocationsImport "+
						 "WHERE AreaType = ? "+
						 "ORDER BY LocationName, SequenceNo ";
			try {
				ps = connection.prepareStatement(sql);
				int i = 1;
				ps.setString(i,areaType.name);
				rs = ps.executeQuery();
				Location currentLocation = null;
				
				while (rs.next()) {
					i = 1;
					long id = rs.getLong(i++);
					String locationName = rs.getString(i++);
					BigDecimal speedLimit = rs.getBigDecimal(i++);
					boolean isSafe = rs.getInt(i++)==1;
					int sequenceNo = rs.getInt(i++);
					BigDecimal latitude = rs.getBigDecimal(i++);
					BigDecimal longitude = rs.getBigDecimal(i++);
					if (currentLocation == null || !currentLocation.isSameAs(locationName)) { // Location has changed
						if (currentLocation != null) {
							save(connection, fleetId, areaType, currentLocation);
						}
						currentLocation = new Location(id, areaType, locationName, sequenceNo, speedLimit, isSafe, latitude, longitude);
						logger.log(Level.INFO,"Started new location "+locationName);
					} else {	// This is another point in the existing currentLocation. Make sure the sequenceNo makes sense, otherwise reject it
						if (currentLocation.isNextInSequence(sequenceNo)) {
							currentLocation.add(sequenceNo, latitude,longitude);
							logger.log(Level.INFO,"Added to location "+locationName+" sequenceNo "+sequenceNo+" number of points "+currentLocation.getNumberOfPoints());
						} else { 
							currentLocation.markAsBad("Incorrect sequence number "+sequenceNo+" - it should have been 1 more than "+currentLocation.getSequenceNo());
							logger.log(Level.INFO,"Incorrect sequenceNo "+sequenceNo+" for location "+locationName+" it should have been "+currentLocation.getSequenceNo()+"+1");
						}
					}
				}
				if (currentLocation != null) {
					save(connection, fleetId, areaType, currentLocation);
				}
				
			} catch (SQLException ex) {
				String msg = "Unable to select the locations to import for AreaType "+areaType+" using sql "+sql+" : SQLException "+ex.getMessage();
				logger.log(Level.SEVERE,msg,ex);
				throw new LocationRepositoryException(msg,ex);
			} finally {		
				if (rs != null) {
					try {
						rs.close();
					} catch (Throwable th) {
					}
					rs = null;
				}
				if (ps != null) {
					try {
						ps.close();
					} catch (Throwable th) {
					}
					ps = null;
				}
			}
		} finally {
			if (connection != null) {
				try {
					connection.close();
				} catch (Throwable th) {
				}
				connection = null;
			}
		}
	}
	private void delete(Connection connection, long fleetId, AreaType areaType) throws LocationRepositoryException{
		PreparedStatement ps = null;
		// delete rows that are no longer imported
		String sql = "DELETE FROM fleet_locations "
				+ " WHERE fleet = ? "
				+ " AND vigilPlusImported = 'Y' "
				+ " AND vigilPlusLocation = ? "
				+ " AND vigilPlusSpeedArea = ? "
				+ " AND vigilPlusKeepIn = ? "
				+ " AND vigilPlusNoGo = ? "
				+ " AND vigilPlusAreaCollection = ? ";
		try {
				// Remove any imported ones that are no longer of any type
				logger.log(Level.INFO,"Deleting locations of type "+areaType);
				ps = connection.prepareStatement(sql);
				int i = 1;
				ps.setLong(i++,fleetId);					
				switch(areaType) {
				case AreaCollection:
					ps.setString(i++,"N");
					ps.setString(i++,"N");
					ps.setString(i++,"N");
					ps.setString(i++,"N");
					ps.setString(i++,"Y");
					break;
				case KeepIn:
					ps.setString(i++,"N");
					ps.setString(i++,"N");
					ps.setString(i++,"Y");
					ps.setString(i++,"N");
					ps.setString(i++,"N");
					break;
				case Location:
					ps.setString(i++,"Y");
					ps.setString(i++,"N");
					ps.setString(i++,"N");
					ps.setString(i++,"N");
					ps.setString(i++,"N");
					break;
				case NoGo:
					ps.setString(i++,"N");
					ps.setString(i++,"N");
					ps.setString(i++,"N");
					ps.setString(i++,"Y");
					ps.setString(i++,"N");
					break;
				case SpeedArea:
					ps.setString(i++,"N");
					ps.setString(i++,"Y");
					ps.setString(i++,"N");
					ps.setString(i++,"N");
					ps.setString(i++,"N");
					break;
				}
				ps.executeUpdate();
				ps.close();
				// Update any imported ones that are multiple types to say they are no longer of the given areaType
				sql = "UPDATE fleet_locations ";
				switch (areaType) {
				case AreaCollection:
					sql += " SET vigilPlusAreaCollection = 'N' ";
					break;
				case KeepIn:
					sql += " SET vigilPlusKeepIn = 'N' ";
					break;
				case Location:
					sql += " SET vigilPlusLocation = 'N' ";
					break;
				case NoGo:
					sql += " SET vigilPlusNoGo = 'N' ";
					break;
				case SpeedArea:
					sql += " SET vigilPlusSpeedArea = 'N' ";
					break;
				}
				sql += " WHERE fleet = ? AND vigilPlusImported = 'Y'";
				ps = connection.prepareStatement(sql);
				i = 1;
				ps.setLong(i++,fleetId);
				
		} catch (SQLException ex) {
			String msg = "Unable to delete the fleet_locations for areaType "+areaType+" using sql "+sql+" : SQLException "+ex.getMessage();
			logger.log(Level.SEVERE,msg,ex);
			throw new LocationRepositoryException(msg,ex);
		} finally {		
			if (ps != null) {
				try {
					ps.close();
				} catch (Throwable th) {
				}
				ps = null;
			}
		}
	}
	private void save(Connection connection, long fleetId, AreaType areaType, Location location) throws LocationRepositoryException{
		PreparedStatement ps = null;
		ResultSet rs = null;
		String sql = null;
		
		try {
			if (location.isBadRecord()) {
				updateBadRecord(connection, location);
			} else {
				// See if one already exists for the given location but with a different area type
				sql = "SELECT loc_id FROM fleet_locations "
				    + " WHERE location_name = ? AND fleet = ? AND vigilPlusImported = 'Y'";
				ps = connection.prepareStatement(sql);
				int i = 1;
				ps.setString(i++,location.getLocationName());
				ps.setLong(i++,fleetId);
				rs = ps.executeQuery();
				if (rs.next()) {
					logger.log(Level.INFO,"Updating location "+location);
					long loc_id = rs.getLong(1);
					rs.close();
					rs = null;
					ps.close();
					ps = null;
					sql = "UPDATE fleet_locations SET ";
					switch (areaType) {
					case AreaCollection:
						sql += "vigilPlusAreaCollection = 'Y'";
						break;
					case KeepIn:
						sql += "vigilPlusKeepIn = 'Y'";
						break;
					case Location:
						sql += "vigilPlusLocation = 'Y'";
						break;
					case NoGo:
						sql += "vigilPlusNoGo = 'Y'";
						break;
					case SpeedArea:
						sql += "vigilPlusSpeedArea = 'Y'";
						break;
					}
					sql += " WHERE loc_id = ? ";
					ps = connection.prepareStatement(sql);
					ps.setLong(1,loc_id);
					ps.execute();
					return;
				}
				rs.close();
				rs = null;
				ps.close();
				ps = null;
				logger.log(Level.INFO,"Inserting location "+location);
				sql = "INSERT IGNORE INTO fleet_locations(location_name, latitude, longitude, topleft_lat, topleft_long,"
						+ " bottomright_lat, bottomright_long, complexpoints, fleet, service_loc_flag, corridor_flag, "
						+ " speed_zone_override, speed_zone_flag, locationcolor, vigilPlusImported, "
						+ " vigilPlusLocation, vigilPlusSpeedArea, vigilPlusKeepIn, vigilPlusNoGo, vigilPlusAreaCollection) "
						+ " VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, 'Y', ?, ?, ?, ?, ?)";
				ps = connection.prepareStatement(sql);
				List<LatLong> cornersAndCentre = location.findCornersAndCentre();
				LatLong topLeft = cornersAndCentre.get(Location.TOP_LEFT);
				LatLong bottomRight = cornersAndCentre.get(Location.BOTTOM_RIGHT);
				LatLong centre = cornersAndCentre.get(Location.CENTRE);
				i = 1;
				ps.setString(i++,location.getLocationName());					
				ps.setBigDecimal(i++,centre.latitude);
				ps.setBigDecimal(i++,centre.longitude);
				ps.setBigDecimal(i++,topLeft.latitude);
				ps.setBigDecimal(i++,topLeft.longitude);
				ps.setBigDecimal(i++,bottomRight.latitude);
				ps.setBigDecimal(i++,bottomRight.longitude);
				ps.setString(i++, location.getComplexPoints());
				ps.setLong(i++,fleetId);
				ps.setString(i++,location.isSafe()?"y":"n");
				ps.setString(i++,location.isCorridor()?"y":"n");
				ps.setBigDecimal(i++,location.getSpeedLimit());
				ps.setString(i++,location.isSpeedZoneOverride()?"Y":"N");
				ps.setString(i++,"#008CFF");
				switch(areaType) {
				case AreaCollection:
					ps.setString(i++,"N");
					ps.setString(i++,"N");
					ps.setString(i++,"N");
					ps.setString(i++,"N");
					ps.setString(i++,"Y");
					break;
				case KeepIn:
					ps.setString(i++,"N");
					ps.setString(i++,"N");
					ps.setString(i++,"Y");
					ps.setString(i++,"N");
					ps.setString(i++,"N");
					break;
				case Location:
					ps.setString(i++,"Y");
					ps.setString(i++,"N");
					ps.setString(i++,"N");
					ps.setString(i++,"N");
					ps.setString(i++,"N");
					break;
				case NoGo:
					ps.setString(i++,"N");
					ps.setString(i++,"N");
					ps.setString(i++,"N");
					ps.setString(i++,"Y");
					ps.setString(i++,"N");
					break;
				case SpeedArea:
					ps.setString(i++,"N");
					ps.setString(i++,"Y");
					ps.setString(i++,"N");
					ps.setString(i++,"N");
					ps.setString(i++,"N");
					break;
				}
				ps.executeUpdate();
			}
		} catch (SQLException ex) {
			String msg = "Unable to insert the fleet_location for location "+location+" using sql "+sql+" : SQLException "+ex.getMessage();
			logger.log(Level.SEVERE,msg,ex);
			throw new LocationRepositoryException(msg,ex);
		} finally {		
			if (rs != null) {
				try {
					rs.close();
				} catch (Throwable th) {
				}
				rs = null;
			}
			if (ps != null) {
				try {
					ps.close();
				} catch (Throwable th) {
				}
				ps = null;
			}
		}
	}
	private void updateBadRecord(Connection connection, Location location) throws LocationRepositoryException{
		PreparedStatement ps = null;
		String sql = "UPDATE LocationsImport set badRecord = 'Y', badRecordReason = ? "
				+ " WHERE  id = ?";
		try {
			logger.log(Level.INFO,"Setting locationName "+location.getLocationName()+" to be a bad record");
			ps = connection.prepareStatement(sql);
			int i = 1;
			ps.setString(i++,location.getBadRecordReason());
			ps.setLong(i++,location.getId());
			ps.executeUpdate();
		} catch (SQLException ex) {
			String msg = "Unable to update the LocationsImport record with the badRecordReason for id "+location.getId()+" badRecordReason "+location.getBadRecordReason()+" using sql "+sql+" : SQLException "+ex.getMessage();
			logger.log(Level.SEVERE,msg,ex);
			throw new LocationRepositoryException(msg,ex);
		} finally {		
			if (ps != null) {
				try {
					ps.close();
				} catch (Throwable th) {
				}
				ps = null;
			}
		}
	}
}
