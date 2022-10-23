package org.axonframework.common.transaction;

import java.util.function.Supplier;

public interface TransactionManager {

    Transaction startTransaction();

    default void executeInTransaction(Runnable task) {
        Transaction transaction = this.startTransaction();

        try {
            task.run();
            transaction.commit();
        } catch (Throwable var4) {
            transaction.rollback();
            throw var4;
        }
    }

    default <T> T fetchInTransaction(Supplier<T> supplier) {
        Transaction transaction = this.startTransaction();

        try {
            T result = supplier.get();
            transaction.commit();
            return result;
        } catch (Throwable var4) {
            transaction.rollback();
            throw var4;
        }
    }
}
