package tech.gklijs.mongo_concurrent;

import com.mongodb.TransactionOptions;
import com.mongodb.WriteConcern;
import org.axonframework.common.transaction.TransactionManager;
import org.axonframework.spring.messaging.unitofwork.SpringTransactionManager;
import org.springframework.data.mongodb.MongoTransactionManager;
import org.springframework.data.mongodb.core.SimpleMongoClientDatabaseFactory;

public class TransactionManagerSupplier {

    private TransactionManagerSupplier() {
        //utility class
    }

    static TransactionManager get(SimpleMongoClientDatabaseFactory factory) {
        MongoTransactionManager mongoTransactionManager = new MongoTransactionManager(
                factory,
                TransactionOptions.builder().writeConcern(WriteConcern.MAJORITY).build()
        );
        return new SpringTransactionManager(mongoTransactionManager);
    }
}
