package tech.gklijs.mongo_concurrent;

import com.mongodb.TransactionOptions;
import com.mongodb.WriteConcern;
import com.mongodb.client.ClientSession;
import com.mongodb.client.MongoCollection;
import com.mongodb.client.MongoDatabase;
import org.bson.Document;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

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
     * @param database     db used
     * @param threadNumber sequence number of the thread (0-based)
     * @param totalThreads total number of threads used for processing
     * @return if an event was processed, when returning false it's likely at the end of the stream
     */
    static boolean processOne(MongoDatabase database, ClientSession session, int threadNumber, int totalThreads) {
        MongoCollection<Document> eventsCol = database.getCollection("events");
        MongoCollection<Document> tokensCol = database.getCollection("tokens");
        MongoCollection<Document> projectionCol = database.getCollection("projection");
        Document token = tokensCol.find(session, eq("tn", threadNumber)).first();
        int nextEvent = isNull(token) ? threadNumber : token.getInteger("en") + totalThreads;
        Document event = eventsCol.find(session, eq("token", nextEvent)).first();
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
            tokensCol.insertOne(session, newToken);
        } else {
            tokensCol.updateOne(session, eq("tn", threadNumber), set("en", nextEvent));
        }
        if (RANDOM.nextBoolean()) {
            throw new RuntimeException("not this time");
        }
        event.put("processedAt", Instant.now().toEpochMilli());
        projectionCol.insertOne(session, event);
        projectionCol.updateOne(session, eq("name", "tracker"), set("lastProcessed", nextEvent));
        LOGGER.info("Successfully processed event with result {}", event);
        return true;
    }

    static Runnable processAll(MongoDatabase database, ClientSession session, int threadNumber, int totalThreads) {
        return () -> {
            boolean done = false;
            while (!done) {
                LOGGER.info("going to start new transaction");
                session.startTransaction(TransactionOptions.builder().writeConcern(WriteConcern.MAJORITY).build());
                try {
                    LOGGER.info("about to process one event");
                    done = !processOne(database, session, threadNumber, totalThreads);
                    LOGGER.info("about to commit transaction");
                    session.commitTransaction();
                } catch (Exception e) {
                    LOGGER.warn("exception processing one", e);
                    session.abortTransaction();
                }
            }
            session.close();
        };
    }
}
