package org.axonframework.spring.messaging.unitofwork;

import org.axonframework.common.transaction.Transaction;
import org.axonframework.common.transaction.TransactionManager;
import org.springframework.transaction.PlatformTransactionManager;
import org.springframework.transaction.TransactionDefinition;
import org.springframework.transaction.TransactionStatus;
import org.springframework.transaction.support.DefaultTransactionDefinition;

public class SpringTransactionManager implements TransactionManager {

    private final PlatformTransactionManager transactionManager;
    private final TransactionDefinition transactionDefinition;

    public SpringTransactionManager(PlatformTransactionManager transactionManager,
                                    TransactionDefinition transactionDefinition) {
        this.transactionManager = transactionManager;
        this.transactionDefinition = transactionDefinition;
    }

    public SpringTransactionManager(PlatformTransactionManager transactionManager) {
        this(transactionManager, new DefaultTransactionDefinition());
    }

    public Transaction startTransaction() {
        final TransactionStatus status = this.transactionManager.getTransaction(this.transactionDefinition);
        return new Transaction() {
            public void commit() {
                SpringTransactionManager.this.commitTransaction(status);
            }

            public void rollback() {
                SpringTransactionManager.this.rollbackTransaction(status);
            }
        };
    }

    protected void commitTransaction(TransactionStatus status) {
        if (status.isNewTransaction() && !status.isCompleted()) {
            this.transactionManager.commit(status);
        }
    }

    protected void rollbackTransaction(TransactionStatus status) {
        if (status.isNewTransaction() && !status.isCompleted()) {
            this.transactionManager.rollback(status);
        }
    }
}
