package transaction;

import java.time.Duration;
import java.util.HashMap;
import java.util.List;
import java.util.Map;

import com.datastax.oss.driver.api.core.ConsistencyLevel;
import com.datastax.oss.driver.api.core.CqlSession;
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
        defaultConsistencyLevel = ConsistencyLevel.ALL;
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

    public void setDefaultConsistencyLevel(ConsistencyLevel consistencyLevel) {
        this.defaultConsistencyLevel = consistencyLevel;
    }

    public void setDefaultConsistencyLevel(String s) {
        ConsistencyLevel level;
        switch(s) {
        case "any":
            level = ConsistencyLevel.ANY;
            break;
        case "one":
            level = ConsistencyLevel.ONE;
            break;
        case "two":
            level = ConsistencyLevel.TWO;
            break;
        case "three":
            level = ConsistencyLevel.THREE;
            break;
        case "quorum":
            level = ConsistencyLevel.QUORUM;
            break;
        case "all":
            level = ConsistencyLevel.ALL;
            break;
        case "local_quorum":
            level = ConsistencyLevel.LOCAL_QUORUM;
            break;
        case "each_quorum":
            level = ConsistencyLevel.EACH_QUORUM;
            break;
        case "serial":
            level = ConsistencyLevel.SERIAL;
            break;
        case "local_serial":
            level = ConsistencyLevel.LOCAL_SERIAL;
            break;
        case "local_one":
            level = ConsistencyLevel.LOCAL_ONE;
            break;
        default:
            level = ConsistencyLevel.ALL;
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
