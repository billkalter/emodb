package com.bazaarvoice.emodb.common.cassandra.cqldriver;

import com.datastax.driver.core.ColumnDefinitions;
import com.datastax.driver.core.ExecutionInfo;
import com.datastax.driver.core.PagingState;
import com.datastax.driver.core.ResultSet;
import com.datastax.driver.core.Row;
import com.datastax.driver.core.Session;
import com.datastax.driver.core.Statement;
import com.datastax.driver.core.exceptions.FrameTooLongException;
import com.datastax.driver.core.exceptions.ReadTimeoutException;
import com.google.common.collect.AbstractIterator;
import com.google.common.util.concurrent.ListenableFuture;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.util.Iterator;
import java.util.List;
import java.util.Spliterator;
import java.util.Spliterators;
import java.util.function.Consumer;
import java.util.stream.Collectors;
import java.util.stream.StreamSupport;

public class AdaptiveResultSet implements ResultSet {

    private final Logger _log = LoggerFactory.getLogger(AdaptiveResultSet.class);

    private final Session _session;
    private ResultSet _delegate;

    public AdaptiveResultSet(Session session, ResultSet delegate) {
        _session = session;
        _delegate = delegate;
    }

    @Override
    public Row one() {
        Row row;
        try {
            row = _delegate.one();
        } catch (FrameTooLongException | ReadTimeoutException e) {
            if (!reduceFetchSize(e)) {
                throw e;
            }
            return one();
        }
        return row;
    }

    private boolean reduceFetchSize(Throwable reason) {
        ExecutionInfo executionInfo = _delegate.getExecutionInfo();
        Statement statement = executionInfo.getStatement();
        PagingState pagingState = executionInfo.getPagingState();
        int fetchSize = statement.getFetchSize();

        while (fetchSize > 2) {
            fetchSize = fetchSize / 2;
            _log.error("BJK: Retrying query at next page with fetch size {} due to {}", fetchSize, reason.getMessage());
            statement.setFetchSize(fetchSize);
            statement.setPagingState(pagingState);
            try {
                _delegate = _session.execute(statement);
                return true;
            } catch (FrameTooLongException e) {
                // Ok, continue to the next iteration
            }
        }

        return false;
    }

    @Override
    public Iterator<Row> iterator() {
        return new AbstractIterator<Row>() {
            @Override
            protected Row computeNext() {
                Row next = one();
                if (next != null) {
                    return next;
                }
                return endOfData();
            }
        };
    }

    @Override
    public Spliterator<Row> spliterator() {
        return Spliterators.spliteratorUnknownSize(iterator(), 0);
    }

    @Override
    public List<Row> all() {
        return StreamSupport.stream(spliterator(), false).collect(Collectors.toList());
    }

    // Remaining methods require no additional logic beyond forwarding calls to the ResultSet delegate.

    @Override
    public ColumnDefinitions getColumnDefinitions() {
        return _delegate.getColumnDefinitions();
    }

    @Override
    public boolean wasApplied() {
        return _delegate.wasApplied();
    }

    @Override
    public boolean isExhausted() {
        return _delegate.isExhausted();
    }

    @Override
    public boolean isFullyFetched() {
        return _delegate.isFullyFetched();
    }

    @Override
    public int getAvailableWithoutFetching() {
        return _delegate.getAvailableWithoutFetching();
    }

    @Override
    public ListenableFuture<ResultSet> fetchMoreResults() {
        return _delegate.fetchMoreResults();
    }

    @Override
    public ExecutionInfo getExecutionInfo() {
        return _delegate.getExecutionInfo();
    }

    @Override
    public List<ExecutionInfo> getAllExecutionInfo() {
        return _delegate.getAllExecutionInfo();
    }

    @Override
    public void forEach(Consumer<? super Row> action) {
        _delegate.forEach(action);
    }
}
