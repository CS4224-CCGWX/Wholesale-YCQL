import com.datastax.driver.core.Cluster;
import com.datastax.driver.core.Session;

import parser.DataLoader;

class Main {
    public static void main(String[] args) {
        Cluster cluster = Cluster.builder()
                .addContactPoint("localhost")
                .build();
        Session session = cluster.connect();

        String schemaPath = args[0];
        String dataPath = args[1];

        DataLoader dataLoader = new DataLoader(session, schemaPath, dataPath);
        dataLoader.loadAll();
    }
}
