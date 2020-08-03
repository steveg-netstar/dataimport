package au.com.ezy2c.dataimport;
import java.util.logging.Level;
import java.util.logging.Logger;

import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.beans.factory.annotation.Value;
import org.springframework.boot.CommandLineRunner;
import org.springframework.stereotype.Component;

import au.com.ezy2c.common.logging.RollingLogger;
import au.com.ezy2c.dataimport.repository.LocationRepository;

@Component
public class DataImportCommandLineRunner implements CommandLineRunner {
	static final Logger logger = Logger.getLogger(DataImportCommandLineRunner.class.getSimpleName());
	@Autowired
	LocationRepository locationRepository;
	
	@Value("${logFileNumber}")
	int logFileNumber;
	@Value("${logFileSize}")
	int logFileSize;
	
	
	@Override
	public void run(String... args) throws Exception {
		String logFile = "logs/"+DataImportApplication.class.getSimpleName()+".log";
		RollingLogger.init(logFile, logFileSize, logFileNumber);
		logger.log(Level.INFO,"run: Started");
		logger.log(Level.INFO,"args.length "+args.length);
		if (args.length < 2) {
			String msg = "Not enough arguments provided. Please provide fleetId and areaType where areaType is one of {";
			boolean commaRequired = false;
			for (AreaType areaType : AreaType.values()) {
				if (commaRequired) 
					msg += ",";
				else {
					commaRequired = true;
				}
				msg+=areaType;
			}
			logger.severe(msg+"}");
		} else {
			logger.log(Level.INFO,"Using fleetId {0} areaType {1}",new Object[] {args[0], args[1]});
			long fleetId = Long.parseLong(args[0]);
			locationRepository.storeLocations(fleetId, AreaType.valueOf(args[1]));
		}
		logger.log(Level.INFO,"run: Finished");
	}
}
