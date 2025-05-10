package org.example;
import com.mongodb.MongoClientSettings;
import com.mongodb.client.*;
import com.mongodb.client.MongoCollection;
import com.mongodb.client.MongoDatabase;
import org.apache.kafka.clients.consumer.ConsumerConfig;
import org.apache.kafka.clients.consumer.ConsumerRecord;
import org.apache.kafka.clients.consumer.ConsumerRecords;
import org.apache.kafka.clients.consumer.KafkaConsumer;
import org.apache.kafka.clients.producer.KafkaProducer;
import org.apache.kafka.clients.producer.ProducerConfig;
import org.apache.kafka.common.serialization.StringDeserializer;
import org.bson.Document;
import org.apache.kafka.clients.producer.ProducerRecord;
import org.apache.kafka.common.serialization.StringSerializer;
import org.jetbrains.annotations.NotNull;


import java.time.Duration;
import java.time.Instant;
import java.util.*;
import java.util.concurrent.atomic.AtomicInteger;

class Trade {
    private String execId;
    private String clientId;
    private String tradeDate;
    private String security;
    private int quantity;
    private double price;

    // Constructor
    public Trade(String execId, String clientId, String tradeDate, String security, int quantity, double price) {
        this.execId = execId;
        this.clientId = clientId;
        this.tradeDate = tradeDate;
        this.security = security;
        this.quantity = quantity;
        this.price = price;
    }

    // Getters and Setters (optional, Gson can directly access fields if public or private and without setters)
    public String getExecId() {
        return execId;
    }

    public void setExecId(String execId) {
        this.execId = execId;
    }

    public String getClientId() {
        return clientId;
    }

    public void setClientId(String clientId) {
        this.clientId = clientId;
    }

    public String getTradeDate() {
        return tradeDate;
    }

    public void setTradeDate(String tradeDate) {
        this.tradeDate = tradeDate;
    }

    public String getSecurity() {
        return security;
    }

    public void setSecurity(String security) {
        this.security = security;
    }

    public int getQuantity() {
        return quantity;
    }

    public void setQuantity(int quantity) {
        this.quantity = quantity;
    }

    public double getPrice() {
        return price;
    }

    public void setPrice(double price) {
        this.price = price;
    }

    public Document toDocument() {
        return new Document("execId", execId)
                .append("clientId", clientId)
                .append("tradeDate", tradeDate)
                .append("security", security)
                .append("quantity", quantity)
                .append("price", price);
    }
    public String toJson() {
        return String.format("{\"execId\":\"%s\",\"clientId\":\"%s\",\"tradeDate\":\"%s\",\"security\":\"%s\",\"quantity\":%d,\"price\":%.2f}",
                execId, clientId, tradeDate, security, quantity, price);
    }


}

public class TradeToJsonWithGson {
    public static void main(String[] args) {
        // Output file for JSON
        String mongoUri = "mongodb://mongoadmin:secret@localhost:27017"; // Replace with your MongoDB connection string if necessary
        String databaseName = "tradeDB"; // MongoDB database name
        String collectionName = "trades"; // MongoDB collection name

        MongoClient mongoClient = null;
        MongoClientSettings settings = MongoClientSettings.builder().applyConnectionString(
                new com.mongodb.ConnectionString(mongoUri)).build();
        String kafkaBootstrapServers = "localhost:9092";
        String kafkaTopic = "trades_topic";
        KafkaProducer<String, String> kafkaProducer = createKafkaProducer(kafkaBootstrapServers);
        String kafkaGroupId = "trade-consumer-group";

        try {
            // Connect to MongoDB
            mongoClient = MongoClients
                    .create(settings);

            MongoDatabase database = mongoClient.getDatabase(databaseName);
            MongoCollection<Document> collection = database.getCollection(collectionName);

            // Total trades to generate and insert
            final int totalTrades = 10_000_000;
            final int batchSize = 10_000;
           KafkaConsumer<String, String> kafkaConsumer = createKafkaConsumer(kafkaBootstrapServers, kafkaGroupId);


            Random random = new Random();
            writeToMongoFromKafka(database, collectionName, kafkaConsumer, kafkaTopic, batchSize, mongoClient);
           writeToKafkaInBatches(totalTrades, batchSize, random, kafkaTopic, kafkaProducer);
          writeToMongoInBatches(totalTrades, batchSize, random, collection);
            MongoCollection<Document> collectionRead = database.getCollection("trades");
           readFromMongoWithFilter(collectionRead);


            // Access the database and collection

            aggregate(database, mongoClient);


        } catch (Exception e) {
            e.printStackTrace();
        } finally {
            if (mongoClient != null) {
                mongoClient.close();
            }
        }

    }

    private static void aggregate(MongoDatabase database, MongoClient mongoClient) {
        var startTime=Instant.now();
        System.out.println("Starting to read aggregated trades from MongoDB..." + startTime);
        MongoCollection<Document> trades = database.getCollection("trades");

        //// Define the aggregation pipeline
        AggregateIterable<Document> result = trades.aggregate(Arrays.asList(

                new Document("$match", new Document("tradeDate", "2023-11-29")),

                new Document("$group", new Document("_id", new Document("security", "$security")
                        .append("tradeDate", "$tradeDate"))
                        .append("totalQuantity", new Document("$sum", "$quantity"))
                        .append("averagePrice", new Document("$avg", "$price"))
                )
        ));

        // Iterate and print the results
    int resultCount=0;
        for (Document doc : result) {

            resultCount++;
            System.out.println(doc.toJson());
        }
        var endTime=Instant.now();
        System.out.println("Total aggregated trades in MongoDB: "+resultCount);
        System.out.println("Total time taken for total" + java.time.Duration.between(startTime, endTime).getSeconds() + " seconds");
        // Close the connection
        mongoClient.close();
    }

    private static void writeToMongoFromKafka(MongoDatabase database, String collectionName, KafkaConsumer<String, String> kafkaConsumer, String kafkaTopic, int batchSize, MongoClient mongoClient) {
        try {
            // MongoDB Connection
            MongoCollection<Document> collectionWrite = database.getCollection(collectionName);

            // Subscribe to Kafka topic
            kafkaConsumer.subscribe(Collections.singletonList(kafkaTopic));

            var startTime=Instant.now();
            System.out.println("Starting to consume trades from Kafka to mongodb..." + startTime);


            // Batch size for processing

            AtomicInteger count = new AtomicInteger(0);

            List<Document> mongoBatch = new ArrayList<>();

            // Poll Kafka topic and process messages
            while (true) {
                // Poll Kafka for messages
                ConsumerRecords<String, String> records = kafkaConsumer.poll(Duration.ofMillis(100));

                for (ConsumerRecord<String, String> record : records) {
                    // Deserialize the record value (JSON) into a MongoDB Document
                    Document document = Document.parse(record.value());

                    // Add to MongoDB batch list
                    mongoBatch.add(document);
                    count.incrementAndGet();

                    // If batch size reaches the limit, write to MongoDB
                    if (mongoBatch.size() >= batchSize) {
                        collectionWrite.insertMany(mongoBatch);
                        System.out.println("Inserted " + mongoBatch.size() + " documents into MongoDB.");
                        mongoBatch.clear(); // Clear the list for the next batch
                    }
                }

                // Commit Kafka offsets after processing (at-least-once semantics)
                kafkaConsumer.commitSync();

                // If batch is partially filled and there are no more records, write the remaining to MongoDB
                if (!mongoBatch.isEmpty() && records.isEmpty()) {
                    collectionWrite.insertMany(mongoBatch);
                    System.out.println("Inserted remaining " + mongoBatch.size() + " documents into MongoDB.");
                    mongoBatch.clear();
                    var endTime1=Instant.now();
                    System.out.println("Total time taken: " + java.time.Duration.between(startTime, endTime1).getSeconds() + " seconds");
                }
            }

        } catch (Exception e) {
            e.printStackTrace();
        } finally {
            // Close resources
            if (mongoClient != null) {
                mongoClient.close();
            }
            if (kafkaConsumer != null) {
                kafkaConsumer.close();
            }
        }
    }

    private static void readFromMongoWithFilter(MongoCollection<Document> collectionRead) {
        Document filter = new Document("security", "AAPL")
                .append("quantity", new Document("$gt", 500));

        // Execute the query
        FindIterable<Document> results = collectionRead.find(filter);
        System.out.println("Total MSFT docs" + collectionRead.countDocuments(filter));


        var startTime1=Instant.now();
        System.out.println("Starting to read trades from MongoDB..." + startTime1);

        // Iterate over the matching documents
        for (Document doc : results) {
            //System.out.println(doc.toJson());
        }
        var endTime1=Instant.now();
        System.out.println("Total time taken: " + java.time.Duration.between(startTime1, endTime1).getSeconds() + " seconds");
    }

    @NotNull
    private static Instant writeToKafkaInBatches(int totalTrades, int batchSize, Random random, String kafkaTopic, KafkaProducer<String, String> kafkaProducer) {
        var startTime=Instant.now();
        System.out.println("Starting to write trades into Kafka..." + startTime);

        // Generate and insert trades in batches
        for (int i = 0; i < totalTrades / batchSize; i++) {
            List<Document> batch = new ArrayList<>();

            for (int j = 0; j < batchSize; j++) {
                int tradeId = i * batchSize + j + 1;
                Trade trade = generateRandomTrade(random, tradeId);
                String securityKey = trade.getSecurity();
                String tradeJson = trade.toJson();
                ProducerRecord<String, String> kafkaRecord = new ProducerRecord<>(kafkaTopic, securityKey, tradeJson);
                kafkaProducer.send(kafkaRecord);

//                    batch.add(trade.toDocument());
            }

            // Insert the batch into MongoDB
         //  // collection.insertMany(batch);
            kafkaProducer.flush();

            System.out.println("Inserted batch " + (i + 1) + " of " + (totalTrades / batchSize));
        }
        var endTime=Instant.now();
        System.out.println("Total time taken: " + java.time.Duration.between(startTime, endTime).getSeconds() + " seconds");

        System.out.println("All trades have been inserted into Kafka successfully!" );
        return startTime;
    }

    private static Instant writeToMongoInBatches(int totalTrades, int batchSize, Random random, MongoCollection<Document> collection) {
        var startTime=Instant.now();
        System.out.println("Starting to write trades into MongoDb..." + startTime);

        // Generate and insert trades in batches
        for (int i = 0; i < totalTrades / batchSize; i++) {
            List<Document> batch = new ArrayList<>();

            for (int j = 0; j < batchSize; j++) {
                int tradeId = i * batchSize + j + 1;
                Trade trade = generateRandomTrade(random, tradeId);
                 batch.add(trade.toDocument());
            }

            // Insert the batch into MongoDB
               collection.insertMany(batch);


            System.out.println("Inserted batch " + (i + 1) + " of " + (totalTrades / batchSize));
        }
        var endTime=Instant.now();
        System.out.println("Total time taken: " + java.time.Duration.between(startTime, endTime).getSeconds() + " seconds");

        System.out.println("All trades have been inserted into Mongo successfully!" );
        return startTime;
    }

    private static Trade generateRandomTrade(Random random, int index) {
        String execId = "EX" + index; // Unique execId
        String clientId = "CL" + (random.nextInt(1000) + 1); // Random clientId
        String tradeDate = "2023-11-" + (random.nextInt(30) + 1); // Random day in November
        String[] securities = {"AAPL", "MSFT", "GOOGL", "AMZN", "TSLA"}; // Sample securities
        String security = securities[random.nextInt(securities.length)];
        int quantity = random.nextInt(1000) + 1; // Random quantity (1 - 1000)
        double price = 50 + (5000 - 50) * random.nextDouble(); // Random price (between $50 and $5000)

        return new Trade(execId, clientId, tradeDate, security, quantity, price);
    }

    private static KafkaProducer<String, String> createKafkaProducer(String bootstrapServers) {
        Properties props = new Properties();
        props.put(ProducerConfig.BOOTSTRAP_SERVERS_CONFIG, bootstrapServers);
        props.put(ProducerConfig.KEY_SERIALIZER_CLASS_CONFIG, StringSerializer.class.getName());
        props.put(ProducerConfig.VALUE_SERIALIZER_CLASS_CONFIG, StringSerializer.class.getName());
        props.put(ProducerConfig.BATCH_SIZE_CONFIG, 16384); // 16 KB batch size
        props.put(ProducerConfig.LINGER_MS_CONFIG, 10);    // Wait up to 10ms to batch messages
        props.put(ProducerConfig.ACKS_CONFIG, "all");      // Ensure data is fully committed

        // Optional: Compression
        props.put(ProducerConfig.COMPRESSION_TYPE_CONFIG, "snappy"); // Use compression to improve throughput

        return new KafkaProducer<>(props);
    }

    private static KafkaConsumer<String, String> createKafkaConsumer(String bootstrapServers, String groupId) {
        Properties props = new Properties();
        props.put(ConsumerConfig.BOOTSTRAP_SERVERS_CONFIG, bootstrapServers);
        props.put(ConsumerConfig.GROUP_ID_CONFIG, groupId);
        props.put(ConsumerConfig.KEY_DESERIALIZER_CLASS_CONFIG, StringDeserializer.class.getName());
        props.put(ConsumerConfig.VALUE_DESERIALIZER_CLASS_CONFIG, StringDeserializer.class.getName());
        props.put(ConsumerConfig.ENABLE_AUTO_COMMIT_CONFIG, "false"); // Disable auto commit to control offsets
        props.put(ConsumerConfig.AUTO_OFFSET_RESET_CONFIG, "earliest"); // Start consuming from the earliest message
        return new KafkaConsumer<>(props);
    }

}