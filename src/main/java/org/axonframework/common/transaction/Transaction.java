package org.axonframework.common.transaction;

public interface Transaction {

    void commit();

    void rollback();
}
