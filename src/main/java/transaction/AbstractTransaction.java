package transaction;

import java.time.Duration;
import java.util.HashMap;
import java.util.List;
import java.util.Map;

import com.datastax.oss.driver.api.core.ConsistencyLevel;
import com.datastax.oss.driver.api.core.CqlSession;
import com.datastax.oss.driver.api.core.cql.BatchStatement;
import com.datastax.oss.driver.api.core.cql.BoundStatement;
import com.datastax.oss.driver.api.core.cql.PreparedStatement;
import com.datastax.oss.driver.api.core.cql.ResultSet;
import com.datastax.oss.driver.api.core.cql.Row;
import com.datastax.oss.driver.api.core.cql.SimpleStatement;
import com.datastax.oss.driver.api.core.cql.SimpleStatementBuilder;

public abstract class AbstractTransaction {
    private static Map<String, PreparedStatement> preparedStatementHashMap = new HashMap<>();
    private final int defaultTimeout = 30;
    protected CqlSession session;
    private ConsistencyLevel defaultConsistencyLevel;

    AbstractTransaction(CqlSession s) {
        session = s;
        defaultConsistencyLevel = ConsistencyLevel.QUORUM;
    }

    public abstract void execute();

    protected List<Row> executeQuery(String query) {
        SimpleStatement statement = new SimpleStatementBuilder(query)
                .setConsistencyLevel(getConsistencyLevel(query))
                .setTimeout(Duration.ofSeconds(defaultTimeout))
                .build();
        ResultSet res = session.execute(statement);

        return res.all();
    }

    protected BoundStatement bindPreparedQuery(String query, Object... values) {
        PreparedStatement preparedStatement;
        if (preparedStatementHashMap.containsKey(query)) {
            preparedStatement = preparedStatementHashMap.get(query);
        } else {
            preparedStatement = session.prepare(query);
            preparedStatementHashMap.put(query, preparedStatement);
        }

        return preparedStatement
                .bind(values)
                .setTimeout(Duration.ofSeconds(defaultTimeout))
                .setConsistencyLevel(getConsistencyLevel(query));
    }

    protected List<Row> executeQuery(String query, Object... values) {
        PreparedStatement preparedStatement;
        if (preparedStatementHashMap.containsKey(query)) {
            preparedStatement = preparedStatementHashMap.get(query);
        } else {
            preparedStatement = session.prepare(query);
            preparedStatementHashMap.put(query, preparedStatement);
        }
        BoundStatement statement = preparedStatement
                .bind(values)
                .setTimeout(Duration.ofSeconds(defaultTimeout))
                .setConsistencyLevel(getConsistencyLevel(query));

        ResultSet res = session.execute(statement);

        return res.all();
    }

    protected List<Row> executeQueryWithTimeout(String query, int timeout, Object... values) {
        PreparedStatement preparedStatement;
        if (preparedStatementHashMap.containsKey(query)) {
            preparedStatement = preparedStatementHashMap.get(query);
        } else {
            preparedStatement = session.prepare(query);
            preparedStatementHashMap.put(query, preparedStatement);
        }
        BoundStatement statement = preparedStatement
                .bind(values)
                .setConsistencyLevel(getConsistencyLevel(query))
                .setTimeout(Duration.ofSeconds(timeout));
        ResultSet res = session.execute(statement);

        return res.all();
    }

    protected List<Row> executeBatch(BatchStatement batch) {
        return session.execute(batch).all();
    }

    public void setDefaultConsistencyLevel(ConsistencyLevel consistencyLevel) {
        this.defaultConsistencyLevel = consistencyLevel;
    }

    public void setDefaultConsistencyLevel(String s) {
        /*
         * Not like Cassandra is AP database, Yugabyte is CP.
         * YCQL only supports two consistency levels: Quorum and ONE.
         * ONE is specifically for Follower-Reads usage.
         * Reference: https://docs.yugabyte.com/preview/admin/ycqlsh/#consistency
         */
        ConsistencyLevel level;
        switch(s) {
        case "one":
            level = ConsistencyLevel.ONE;
            break;
        case "quorum":
            level = ConsistencyLevel.QUORUM;
            break;
        default:
            level = ConsistencyLevel.QUORUM;
            break;
        }
        this.defaultConsistencyLevel = level;
    }

    private ConsistencyLevel getConsistencyLevel(String query) {
        if (this.defaultConsistencyLevel.equals(ConsistencyLevel.QUORUM)) {
            return defaultConsistencyLevel;
        }
        if (query.startsWith("SELECT")) {
            return ConsistencyLevel.ONE;
        } else {
            return defaultConsistencyLevel;
        }
    }

    public void print(String stringToPrint) {
        System.out.println(stringToPrint);
    }
}
