package org.example.migration;

import com.hazelcast.core.HazelcastInstance;
import com.hazelcast.map.IMap;
import org.springframework.boot.SpringApplication;
import org.springframework.boot.autoconfigure.SpringBootApplication;
import org.springframework.context.ConfigurableApplicationContext;

/** A Spring Boot application whose member comes from Spring Boot's auto-configuration. */
@SpringBootApplication
public class App {

    public static void main(String[] args) {
        try (ConfigurableApplicationContext context = SpringApplication.run(App.class, args)) {
            HazelcastInstance hazelcastInstance = context.getBean(HazelcastInstance.class);
            check("migration-test-boot".equals(hazelcastInstance.getConfig().getClusterName()), "cluster name");

            IMap<String, String> greetings = hazelcastInstance.getMap("greetings");
            greetings.put("en", "hello");
            check("hello".equals(greetings.get("en")), "map put/get");

            System.out.println("OK spring-boot");
        }
    }

    private static void check(boolean condition, String what) {
        if (!condition) {
            throw new AssertionError(what);
        }
    }
}
