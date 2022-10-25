package transaction;

import java.util.List;
import java.util.Map;

import com.datastax.driver.core.ConsistencyLevel;
import com.datastax.driver.core.SimpleStatement;
import com.datastax.driver.core.ResultSet;
import com.datastax.driver.core.Row;
import com.datastax.driver.core.Session;
import com.datastax.driver.core.Statement;

public abstract class AbstractTransaction {
    protected Session session;
    private ConsistencyLevel defaultConsistencyLevel;

    AbstractTransaction(Session s) {
        session = s;
        defaultConsistencyLevel = ConsistencyLevel.ALL;
    }

    public abstract void execute();

    protected List<Row> executeQuery(String query) {
        Statement statement = new SimpleStatement(query)
                .setConsistencyLevel(getConsistencyLevel(query));
        ResultSet res = session.execute(statement);

        return res.all();
    }

    protected List<Row> executeQuery(String query, Object... values) {
        Statement statement = new SimpleStatement(query, values)
                .setConsistencyLevel(getConsistencyLevel(query));
        ResultSet res = session.execute(statement);

        return res.all();
    }

    protected List<Row> executeQuery(String query, Map<String, Object> valueMap) {
        Statement statement = new SimpleStatement(query, valueMap)
                .setConsistencyLevel(getConsistencyLevel(query));
        ResultSet res = session.execute(statement);

        return res.all();
    }

    protected List<Row> executeQueryWithTimeout(String query, int timeout, Object... values) {
        Statement statement = new SimpleStatement(query, values)
                .setConsistencyLevel(getConsistencyLevel(query))
                .setReadTimeoutMillis(timeout);
        ResultSet res = session.execute(query);

        return res.all();
    }

    public void setDefaultConsistencyLevel(ConsistencyLevel consistencyLevel) {
        this.defaultConsistencyLevel = consistencyLevel;
    }

    public void setDefaultConsistencyLevel(String s) {
        ConsistencyLevel level;
        switch (s) {
        case "any": level = ConsistencyLevel.ANY;break;
        case "one": level = ConsistencyLevel.ONE;break;
        case "two": level = ConsistencyLevel.TWO;break;
        case "three": level = ConsistencyLevel.THREE;break;
        case "quorum": level = ConsistencyLevel.QUORUM;break;
        case "all": level = ConsistencyLevel.ALL;break;
        case "local_quorum": level = ConsistencyLevel.LOCAL_QUORUM;break;
        case "each_quorum": level = ConsistencyLevel.EACH_QUORUM;break;
        case "serial": level = ConsistencyLevel.SERIAL;break;
        case "local_serial": level = ConsistencyLevel.LOCAL_SERIAL;break;
        case "local_one": level = ConsistencyLevel.LOCAL_ONE;break;
        default:level = ConsistencyLevel.ALL;break;
        }
        this.defaultConsistencyLevel = level;
    }

    private ConsistencyLevel getConsistencyLevel(String query) {
        if (query.startsWith("SELECT")) {
            return ConsistencyLevel.ONE;
        } else {
            return defaultConsistencyLevel;
        }
    }
}
