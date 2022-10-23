package tech.gklijs.mongo_concurrent;

import com.mongodb.client.MongoCollection;
import com.mongodb.client.MongoDatabase;
import org.axonframework.common.transaction.Transaction;
import org.axonframework.common.transaction.TransactionManager;
import org.bson.Document;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.springframework.data.mongodb.MongoDatabaseUtils;
import org.springframework.data.mongodb.core.SimpleMongoClientDatabaseFactory;

import java.time.Instant;
import java.util.Random;

import static com.mongodb.client.model.Filters.eq;
import static com.mongodb.client.model.Updates.set;
import static java.util.Objects.isNull;

public class Processor {

    private static final Logger LOGGER = LoggerFactory.getLogger(Processor.class);
    private static final Random RANDOM = new Random();

    private Processor() {
        //utility class
    }

    static void setLastEventProcessedTracker(MongoDatabase database) {
        Document tracker = new Document();
        tracker.put("name", "tracker");
        database.getCollection("projection").insertOne(tracker);
    }

    /**
     * process one event will return false if there was no event processed
     *
     * @param factory      Spring Mongo factory
     * @param threadNumber sequence number of the thread (0-based)
     * @param totalThreads total number of threads used for processing
     * @return if an event was processed, when returning false it's likely at the end of the stream
     */
    static boolean processOne(SimpleMongoClientDatabaseFactory factory, int threadNumber, int totalThreads) {
        MongoDatabase database = MongoDatabaseUtils.getDatabase(factory);
        MongoCollection<Document> eventsCol = database.getCollection("events");
        MongoCollection<Document> tokensCol = database.getCollection("tokens");
        MongoCollection<Document> projectionCol = database.getCollection("projection");
        Document token = tokensCol.find(eq("tn", threadNumber)).first();
        int nextEvent = isNull(token) ? threadNumber : token.getInteger("en") + totalThreads;
        Document event = eventsCol.find(eq("token", nextEvent)).first();
        if (isNull(event)) {
            LOGGER.info("No event found with token: {}", nextEvent);
            return false;
        } else {
            LOGGER.info("Retrieved event: {}", event);
        }
        if (isNull(token)) {
            Document newToken = new Document();
            newToken.put("tn", threadNumber);
            newToken.put("en", nextEvent);
            tokensCol.insertOne(newToken);
        } else {
            tokensCol.updateOne(eq("tn", threadNumber), set("en", nextEvent));
        }
        if (RANDOM.nextBoolean()) {
            throw new RuntimeException("not this time");
        }
        event.put("processedAt", Instant.now().toEpochMilli());
        projectionCol.insertOne(event);
        projectionCol.updateOne(eq("name", "tracker"), set("lastProcessed", nextEvent));
        LOGGER.info("Successfully processed event with result {}", event);
        return true;
    }

    static Runnable processAll(SimpleMongoClientDatabaseFactory factory, TransactionManager transactionManager,
                               int threadNumber,
                               int totalThreads) {
        return () -> {
            boolean done = false;
            while (!done) {
                LOGGER.info("going to start new transaction");
                Transaction transaction = transactionManager.startTransaction();
                try {
                    LOGGER.info("about to process one event");
                    done = transactionManager.fetchInTransaction(
                            () -> !processOne(factory, threadNumber, totalThreads)
                    );
                    LOGGER.info("about to commit transaction");
                    transaction.commit();
                } catch (Exception e) {
                    LOGGER.warn("exception processing one", e);
                    transaction.rollback();
                }
            }
        };
    }
}
