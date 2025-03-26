package org.example;
import org.springframework.context.annotation.Bean;
import org.springframework.stereotype.Component;

import java.nio.file.Files;
import java.nio.file.Path;
import java.util.stream.Stream;
import java.util.stream.Collectors;
import java.util.Iterator;
import java.util.List;
import java.util.function.Supplier;

@Component
public class FileToKafkaLineStreamer {

    private final Iterator<String> fileIterator;

    // Constructor/Initialization to read file content into an iterator
    public FileToKafkaLineStreamer() throws Exception {
        Path path = Path.of("/Users/vivek/workingdir/test1.txt");
        List<String> lines = Files.lines(path)
                .collect(Collectors.toList());
        this.fileIterator = lines.iterator();
    }

    // Define a Supplier that streams file lines into Kafka
    @Bean
    public Supplier<String> fileToKafka() {
        return () -> fileIterator.hasNext() ? fileIterator.next() : null; // Send one line of the file at a time
    }
}