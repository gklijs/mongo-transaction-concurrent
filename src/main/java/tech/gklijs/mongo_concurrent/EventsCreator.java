package tech.gklijs.mongo_concurrent;

import com.mongodb.client.MongoCollection;
import com.mongodb.client.MongoDatabase;
import org.bson.Document;

import java.time.Instant;
import java.util.UUID;
import java.util.stream.IntStream;

class EventsCreator {

    private EventsCreator() {
        //utility class
    }

    static void createEvents(MongoDatabase database, int numberToCreate) {
        MongoCollection<Document> eventsCol = database.getCollection("events");
        IntStream.range(0, numberToCreate).forEach(
                i -> {
                    Document event = new Document();
                    event.put("token", i);
                    event.put("createdAt", Instant.now().toEpochMilli());
                    event.put("data", UUID.randomUUID().toString());
                    eventsCol.insertOne(event);
                }
        );
    }
}
