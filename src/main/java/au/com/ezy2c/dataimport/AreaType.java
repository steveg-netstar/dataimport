package au.com.ezy2c.dataimport;

public enum AreaType {
	AreaCollection("AreaCollection"),
	KeepIn("Keep-In"),
	Location("Location"),
	NoGo("NoGo"),
	SpeedArea("speedArea");
	
	public String name;
	private AreaType(String name) {
		this.name = name;
	}
}
