package transaction;

import java.util.List;

import com.datastax.driver.core.SimpleStatement;
import com.datastax.driver.core.ResultSet;
import com.datastax.driver.core.Row;
import com.datastax.driver.core.Session;
public class AbstractTransaction {
    protected Session session;

    AbstractTransaction(Session s) {
        session = s;
    }

    protected List<Row> executeQuery(String query) {
        // SimpleStatement statement = new SimpleStatement(query).setConsistencyLevel().build();
        ResultSet res = session.execute(query);

        return res.all();
    }

}
