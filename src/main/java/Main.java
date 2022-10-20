import com.datastax.driver.core.Cluster;
import com.datastax.driver.core.Session;

import parser.DataLoader;

class Main {
    public static void main(String[] args) {
        String ip = args[0];
        String schemaPath = args[1];
        String dataPath = args[2];

        Cluster cluster = Cluster.builder()
                .addContactPoint(ip)
                .build();
        Session session = cluster.connect();



        DataLoader dataLoader = new DataLoader(session, schemaPath, dataPath);
        dataLoader.loadAll();
    }
}
