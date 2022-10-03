package transaction;

import java.util.List;
import java.util.Map;

import com.datastax.driver.core.ConsistencyLevel;
import com.datastax.driver.core.SimpleStatement;
import com.datastax.driver.core.ResultSet;
import com.datastax.driver.core.Row;
import com.datastax.driver.core.Session;
import com.datastax.driver.core.Statement;

public class AbstractTransaction {
    protected Session session;
    private ConsistencyLevel defaultConsistencyLevel;

    AbstractTransaction(Session s) {
        session = s;
        defaultConsistencyLevel = ConsistencyLevel.ALL;
    }

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

    public void setDefaultConsistencyLevel(ConsistencyLevel consistencyLevel) {
        this.defaultConsistencyLevel = consistencyLevel;
    }

    private ConsistencyLevel getConsistencyLevel(String query) {
        if (query.startsWith("SELECT")) {
            return ConsistencyLevel.ONE;
        } else {
            return defaultConsistencyLevel;
        }
    }
}
