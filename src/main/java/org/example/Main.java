package org.example;

import org.springframework.boot.SpringApplication;
import org.springframework.boot.autoconfigure.SpringBootApplication;
import org.springframework.context.annotation.Bean;
import org.springframework.messaging.Message;
import org.springframework.messaging.support.MessageBuilder;

import java.io.IOException;
import java.nio.file.Files;
import java.nio.file.Paths;
import java.util.function.Supplier;

//TIP To <b>Run</b> code, press <shortcut actionId="Run"/> or
// click the <icon src="AllIcons.Actions.Execute"/> icon in the gutter.
@SpringBootApplication

public class Main {
    public static void main(String[] args) {
        var context = SpringApplication.run(Main.class, args);
        System.out.println("Application started successfully");
       // Supplier<Message<String>> supplier = (Supplier<Message<String>>) context.getBean("fileToKafka");
        //System.out.println(supplier.get());


    }
    @Bean
    public Supplier<Message<String>> fileToKafkaSupplier() {
        return () -> {
            String filePath = "src/main/resources/input.txt"; // Adjust file path accordingly
            try {
                // Read file content as a single string
                String content = Files.readString(Paths.get(filePath));
                System.out.println("Producing message: " + content); // Add this log

                // Create a message to send to Kafka
                return MessageBuilder.withPayload(content)
                        .setHeader("file-origin", filePath) // Optional header
                        .build();
            } catch (IOException e) {
                throw new RuntimeException("Failed to read file", e);
            }
        };
    }

}