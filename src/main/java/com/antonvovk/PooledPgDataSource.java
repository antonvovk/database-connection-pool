package com.antonvovk;

import lombok.SneakyThrows;
import org.postgresql.jdbc.PgConnection;

import javax.sql.DataSource;
import java.io.PrintWriter;
import java.sql.Connection;
import java.sql.SQLException;
import java.sql.SQLFeatureNotSupportedException;
import java.time.Duration;
import java.util.ArrayList;
import java.util.List;
import java.util.logging.Logger;

public class PooledPgDataSource implements DataSource {

    private final DataSource delegate;
    private final List<PgConnectionProxy> pool = new ArrayList<>();
    private final Duration shutdownTimeout = Duration.ofSeconds(5);

    public PooledPgDataSource(DataSource dataSource) {
        this(dataSource, 10);
    }

    @SneakyThrows
    public PooledPgDataSource(DataSource dataSource, int poolSize) {
        delegate = dataSource;
        for (int i = 0; i < poolSize; ++i) {
            pool.add(new PgConnectionProxy("Con-" + (i + 1), (PgConnection) dataSource.getConnection()));
        }
    }

    @SneakyThrows
    public void shutdown() {
        long start = System.currentTimeMillis();
        while (poolHasUnreleasedConnections() && (System.currentTimeMillis() - start) < shutdownTimeout.toMillis()) {
            System.out.println("Waiting for pool connections to be released...");
            sleepFor(Duration.ofMillis(100));
        }
        pool.forEach(this::closeConnectionPhysically);
    }

    private boolean poolHasUnreleasedConnections() {
        return pool.stream().anyMatch(connection -> !connection.isReleased());
    }

    private void sleepFor(Duration duration) {
        try {
            Thread.sleep(duration);
        } catch (Throwable e) {
            System.out.println("Failed to wait: " + e.getMessage());
        }
    }

    private void closeConnectionPhysically(PgConnectionProxy connectionProxy) {
        try {
            connectionProxy.closePhysically();
        } catch (Throwable e) {
            System.out.println("Failed to close connection physically: " + e.getMessage());
        }
    }

    @Override
    public Connection getConnection() throws SQLException {
        synchronized (pool) {
            var connection = pool.stream().filter(PgConnectionProxy::isReleased).findAny()
                    .orElseThrow(() -> new SQLException("No available connections in the pool"));
            connection.markAsTaken();
            return connection;
        }
    }

    @Override
    public Connection getConnection(String username, String password) throws SQLException {
        return delegate.getConnection(username, password);
    }

    @Override
    public PrintWriter getLogWriter() throws SQLException {
        return delegate.getLogWriter();
    }

    @Override
    public void setLogWriter(PrintWriter out) throws SQLException {
        delegate.setLogWriter(out);
    }

    @Override
    public void setLoginTimeout(int seconds) throws SQLException {
        delegate.setLoginTimeout(seconds);
    }

    @Override
    public int getLoginTimeout() throws SQLException {
        return delegate.getLoginTimeout();
    }

    @Override
    public Logger getParentLogger() throws SQLFeatureNotSupportedException {
        return delegate.getParentLogger();
    }

    @Override
    public <T> T unwrap(Class<T> iface) throws SQLException {
        return delegate.unwrap(iface);
    }

    @Override
    public boolean isWrapperFor(Class<?> iface) throws SQLException {
        return delegate.isWrapperFor(iface);
    }
}
