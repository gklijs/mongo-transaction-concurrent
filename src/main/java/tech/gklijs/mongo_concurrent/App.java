package tech.gklijs.mongo_concurrent;

import com.mongodb.client.MongoClient;
import com.mongodb.client.MongoClients;
import com.mongodb.client.MongoDatabase;
import org.axonframework.common.transaction.TransactionManager;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.springframework.data.mongodb.MongoDatabaseUtils;
import org.springframework.data.mongodb.core.SimpleMongoClientDatabaseFactory;

import java.util.concurrent.ExecutorService;
import java.util.concurrent.Executors;
import java.util.concurrent.TimeUnit;
import java.util.stream.IntStream;

public class App {

    private static final Logger LOGGER = LoggerFactory.getLogger(App.class);
    public static final String CONNECT_URI = "mongodb://localhost:27017";
    public static final int TOTAL_EVENTS = 12;
    public static final int TOTAL_THREADS = 4;
    public static final ExecutorService executorService = Executors.newFixedThreadPool(TOTAL_THREADS);

    public static void main(String[] args) {
        LOGGER.info("About to create {} events, and going to process them from {} threads",
                    TOTAL_EVENTS,
                    TOTAL_THREADS);
        try (MongoClient mongoClient = MongoClients.create(CONNECT_URI)) {
            SimpleMongoClientDatabaseFactory factory =
                    new SimpleMongoClientDatabaseFactory(mongoClient, "test");
            MongoDatabase database = MongoDatabaseUtils.getDatabase(factory);
            database.getCollection("events").drop();
            database.getCollection("projection").drop();
            database.getCollection("tokens").drop();
            TransactionManager transactionManager = TransactionManagerSupplier.get(factory);
            EventsCreator.createEvents(database, TOTAL_EVENTS);
            Processor.setLastEventProcessedTracker(database);
            database.getCollection("tokens");
            IntStream.range(0, TOTAL_THREADS)
                     .forEach(i -> executorService.submit(
                             Processor.processAll(factory, transactionManager, i, TOTAL_THREADS)
                     ));
            executorService.shutdown();
            boolean result = executorService.awaitTermination(30L, TimeUnit.MINUTES);
            if (result) {
                LOGGER.info("Threads finished within time");
            } else {
                LOGGER.info("Threads did not finished within time, maybe increase the time");
            }
            long projectionCount = database.getCollection("projection").countDocuments();
            if (projectionCount == TOTAL_EVENTS + 1) {
                LOGGER.info("All events are now part of the projection");
            } else {
                LOGGER.warn("Not all events are part of the projection, {} are missing",
                            TOTAL_EVENTS + 1 - projectionCount);
            }
        } catch (Exception e) {
            LOGGER.warn("unexpected exception in main thread", e);
        }
    }
}
