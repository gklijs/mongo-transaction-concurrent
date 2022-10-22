package tech.gklijs.mongo_concurrent;

import com.mongodb.client.MongoClient;
import com.mongodb.client.MongoClients;
import com.mongodb.client.MongoDatabase;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

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
            MongoDatabase database = mongoClient.getDatabase("test");
            EventsCreator.createEvents(database, TOTAL_EVENTS);
            Processor.setLastEventProcessedTracker(database);
            database.getCollection("tokens");
            IntStream.range(0, TOTAL_THREADS)
                     .forEach(i -> executorService.submit(
                             Processor.processAll(database, mongoClient.startSession(), i, TOTAL_THREADS)
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
                LOGGER.warn("Not al events are part of the projection, {} are missing",
                            projectionCount + 1 - TOTAL_EVENTS);
            }
        } catch (Exception e) {
            LOGGER.warn("unexpected exception in main thread", e);
        }
    }
}
