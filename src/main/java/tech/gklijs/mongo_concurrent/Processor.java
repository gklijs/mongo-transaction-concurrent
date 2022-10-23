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
import java.util.concurrent.atomic.AtomicInteger;
import java.util.concurrent.atomic.AtomicReference;

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
     * @param factory     Spring Mongo database factory
     * @param threadNumber sequence number of the thread (0-based)
     * @param totalThreads total number of threads used for processing
     * @return if an event was processed, when returning false it's likely at the end of the stream
     */
    static boolean processOne(SimpleMongoClientDatabaseFactory factory, TransactionManager transactionManager, int threadNumber,
                              int totalThreads) {
        MongoDatabase database = MongoDatabaseUtils.getDatabase(factory);
        MongoCollection<Document> eventsCol = database.getCollection("events");
        MongoCollection<Document> tokensCol = database.getCollection("tokens");
        MongoCollection<Document> projectionCol = database.getCollection("projection");
        AtomicReference<Document> token = new AtomicReference<>();
        AtomicInteger nextEvent = new AtomicInteger();
        AtomicReference<Document> event = new AtomicReference<>();
        transactionManager.executeInTransaction(
                () -> {
                    token.set(tokensCol.find(eq("tn", threadNumber)).first());
                    nextEvent.set(isNull(token.get()) ? threadNumber : token.get().getInteger("en") + totalThreads);
                    event.set(eventsCol.find(eq("token", nextEvent)).first());
                }
        );
        if (isNull(event.get())) {
            LOGGER.info("No event found with token: {}", nextEvent.get());
            return false;
        } else {
            LOGGER.info("Retrieved event: {}", event.get());
        }
        if (isNull(token.get())) {
            Document newToken = new Document();
            newToken.put("tn", threadNumber);
            newToken.put("en", nextEvent);
            transactionManager.executeInTransaction(() -> tokensCol.insertOne(newToken));
        } else {
            transactionManager.executeInTransaction(
                    () -> tokensCol.updateOne(eq("tn", threadNumber), set("en", nextEvent))
            );
        }
        event.get().put("processedAt", Instant.now().toEpochMilli());
        if (RANDOM.nextBoolean()) {
            throw new RuntimeException("not this time");
        }
        transactionManager.executeInTransaction(
                () -> {
                    projectionCol.insertOne(event.get());
                    projectionCol.updateOne(eq("name", "tracker"), set("lastProcessed", nextEvent));
                }
        );
        LOGGER.info("Successfully processed event with result {}", event);
        return true;
    }

    static Runnable processAll(SimpleMongoClientDatabaseFactory factory, TransactionManager transactionManager, int threadNumber,
                               int totalThreads) {
        return () -> {
            boolean done = false;
            while (!done) {
                LOGGER.info("going to start new transaction");
                Transaction transaction = transactionManager.startTransaction();
                try {
                    LOGGER.info("about to process one event");
                    done = !processOne(factory, transactionManager, threadNumber, totalThreads);
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
