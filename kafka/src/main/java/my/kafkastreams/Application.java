package my.kafkastreams;

import org.springframework.boot.SpringApplication;
import org.springframework.boot.autoconfigure.SpringBootApplication;

/**
 * Created by vsergeev on 06.06.2016.
 */
@SpringBootApplication
public class Application {
    public static void main(String[] args) {
           new SpringApplication(Application.class).run(args);
    }

}
