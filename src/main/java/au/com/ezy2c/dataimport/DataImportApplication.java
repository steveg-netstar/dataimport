package au.com.ezy2c.dataimport;

import org.springframework.boot.SpringApplication;
import org.springframework.boot.autoconfigure.SpringBootApplication;
import org.springframework.cloud.context.config.annotation.RefreshScope;

@SpringBootApplication
@RefreshScope
public class DataImportApplication {

	public static void main(String[] args) {
		SpringApplication.run(DataImportApplication.class, args);
	}

}
