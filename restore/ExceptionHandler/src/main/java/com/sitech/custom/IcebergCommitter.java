package com.sitech.custom;

import org.apache.flink.api.common.typeutils.TypeSerializer;
import org.apache.flink.streaming.api.functions.sink.TwoPhaseCommitSinkFunction;

import org.apache.iceberg.Transaction;
import org.apache.iceberg.io.WriteResult;

public class IcebergCommitter extends TwoPhaseCommitSinkFunction<WriteResult, Transaction, Void> {
    public IcebergCommitter(TypeSerializer<Transaction> transactionSerializer, TypeSerializer<Void> contextSerializer) {
        super(transactionSerializer, contextSerializer);
    }

    @Override
    protected void invoke(Transaction transaction, WriteResult writeResult, Context context) throws Exception {

    }

    @Override
    protected Transaction beginTransaction() throws Exception {
        return null;
    }

    @Override
    protected void preCommit(Transaction transaction) throws Exception {

    }

    @Override
    protected void commit(Transaction transaction) {

    }

    @Override
    protected void abort(Transaction transaction) {

    }
}
