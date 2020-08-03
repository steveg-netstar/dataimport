package au.com.ezy2c.dataimport.repository;

import java.math.BigDecimal;
import java.util.ArrayList;
import java.util.List;

import au.com.ezy2c.dataimport.AreaType;

class LatLong {
	BigDecimal latitude;
	BigDecimal longitude;
	
	public LatLong(BigDecimal latitude, BigDecimal longitude) {
		this.latitude = latitude;
		this.longitude = longitude;
	}
	public String toString() {
		StringBuilder sb = new StringBuilder();
		sb.append("(");
		sb.append(latitude);
		sb.append(",");
		sb.append(longitude);
		sb.append(")");
		return sb.toString();
	}

}

public class Location {
	long id;
	AreaType areaType;
	String locationName;
	BigDecimal speedLimit;
	boolean isSafe;
	int sequenceNo;
	List<LatLong> latLongs;
	boolean badRecord;
	String badRecordReason;

	public Location(long id, AreaType areaType, String locationName, int sequenceNo, BigDecimal speedLimit, boolean isSafe, BigDecimal latitude, BigDecimal longitude) {
		this.id = id;
		this.areaType = areaType;
		this.locationName = locationName;
		this.speedLimit = speedLimit;
		this.isSafe = isSafe;
		this.latLongs = new ArrayList<>();
		add(sequenceNo,latitude,longitude);
		this.badRecord = false;
		this.badRecordReason = null;
	}
	public boolean isSameAs(String otherLocationName) {
		return this.locationName.equals(otherLocationName);
	}
	public void add(int sequenceNo, BigDecimal latitude, BigDecimal longitude) {
		this.sequenceNo = sequenceNo;
		latLongs.add(new LatLong(latitude,longitude));
	}
	public boolean isNextInSequence(int sequenceNo) {
		return sequenceNo == this.sequenceNo+1;
	}
	public void markAsBad(String reason) {
		this.badRecord = true;
		this.badRecordReason = reason;
	}
	public int getSequenceNo() {
		return sequenceNo;
	}
	public String getLocationName() {
		return locationName;
	}
	public BigDecimal getSpeedLimit() {
		return speedLimit;
	}
	public boolean isSafe() {
		return isSafe;
	}
	public boolean isCorridor() {
		return AreaType.KeepIn.equals(areaType);
	}
	public boolean isSpeedZoneOverride() {
		return false; // Use Location Speeding instead of AreaType.SpeedArea.equals(areaType);
	}
	public boolean isBadRecord() {
		return badRecord;
	}
	public String getBadRecordReason() {
		return badRecordReason;
	}
	// The positions in the latLong list returned by findCornersAndCentre
	public static int TOP_LEFT = 0;
	public static int BOTTOM_RIGHT = 1;
	public static int CENTRE = 2;
	private static BigDecimal TWO = new BigDecimal("2.0000000");
	/**
	 * Returns topLeft, bottomRight, centre 
	 * The latLongs must not be null!
	 * @return
	 */
	public List<LatLong> findCornersAndCentre() {
		LatLong topLeft = new LatLong(null,null);
		LatLong bottomRight = new LatLong(null,null);
		for (LatLong latLong : latLongs) {
			if (topLeft.latitude == null || topLeft.latitude.doubleValue() < latLong.latitude.doubleValue()) 
				topLeft.latitude = latLong.latitude;
			if (bottomRight.latitude == null || bottomRight.latitude.doubleValue() > latLong.latitude.doubleValue()) 
				bottomRight.latitude = latLong.latitude;
			if (topLeft.longitude == null || topLeft.longitude.doubleValue() > latLong.longitude.doubleValue()) 
				topLeft.longitude = latLong.longitude;
			if (bottomRight.longitude == null || bottomRight.longitude.doubleValue() < latLong.longitude.doubleValue()) 
				bottomRight.longitude = latLong.longitude;
		}
		List<LatLong> cornersAndCentre = new ArrayList<>();
		cornersAndCentre.add(topLeft);
		cornersAndCentre.add(bottomRight);
		LatLong centre = new LatLong(bottomRight.latitude.add(topLeft.latitude).divide(TWO),bottomRight.longitude.add(topLeft.longitude).divide(TWO));
		cornersAndCentre.add(centre);
		return cornersAndCentre;
	}
	public long getId() {
		return id;
	}
	public String getComplexPoints() {
		StringBuilder sb = new StringBuilder();
		if (latLongs != null) {
			boolean commaRequired = false;
			for (LatLong latLong : latLongs) {
				if (commaRequired) 
					sb.append(";");
				else
					commaRequired = true;
				sb.append(latLong.latitude);
				sb.append(",");
				sb.append(latLong.longitude);
			}
		}
		return sb.toString();
	}
	public String toString() {
		StringBuilder sb = new StringBuilder();
		sb.append("[Location: id=");
		sb.append(id);
		sb.append(" locationName=");
		sb.append(locationName);
		sb.append(" speedLimit=");
		sb.append(speedLimit);
		sb.append(" isSafe=");
		sb.append(isSafe);
		sb.append(" sequenceNo=");
		sb.append(sequenceNo);
		sb.append(" badRecord=");
		sb.append(badRecord);
		sb.append(" badRecordReason=");
		sb.append(badRecordReason);
		sb.append(" latLongs={");
		if (latLongs != null) {
			boolean commaRequired = false;
			for (LatLong latLong : latLongs) {
				if (commaRequired) 
					sb.append(",");
				else
					commaRequired = true;
				sb.append(latLong);
			}
		}
		sb.append("} ]");
		return sb.toString();
	}
	public int getNumberOfPoints() {
		if (latLongs == null)
			return 0;
		return latLongs.size();
	}
}
