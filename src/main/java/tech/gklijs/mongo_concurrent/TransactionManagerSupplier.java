package tech.gklijs.mongo_concurrent;

import com.mongodb.TransactionOptions;
import com.mongodb.WriteConcern;
import org.springframework.data.mongodb.MongoTransactionManager;
import org.springframework.data.mongodb.core.SimpleMongoClientDatabaseFactory;
import org.springframework.transaction.PlatformTransactionManager;

public class TransactionManagerSupplier {

    private TransactionManagerSupplier() {
        //utility class
    }

    static PlatformTransactionManager get(SimpleMongoClientDatabaseFactory factory) {
        return new MongoTransactionManager(
                factory,
                TransactionOptions.builder().writeConcern(WriteConcern.MAJORITY).build()
        );
    }
}
