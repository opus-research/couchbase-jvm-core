import com.couchbase.client.java.*;
import com.couchbase.client.java.document.*;
import com.couchbase.client.java.document.json.*;
import com.couchbase.client.java.query.*;
import com.couchbase.client.java.env.CouchbaseEnvironment;
import com.couchbase.client.java.env.DefaultCouchbaseEnvironment;
public class Client {
    public static void main(String[] args) {

        CouchbaseEnvironment env = DefaultCouchbaseEnvironment
            .builder()
            .sslEnabled(true)
            .sslKeystoreFile("keystore.jks")
            .sslKeystorePassword("123456")
            .computationPoolSize(5)
            .bootstrapCarrierSslPort(11996)
            .bootstrapHttpDirectPort(9000)
            .bootstrapHttpSslPort(19000)
            .bootstrapCarrierDirectPort(12000)
            .build();

        String[] nodes = {"localhost"};
        CouchbaseCluster cluster = CouchbaseCluster.create(env, nodes);

        Bucket bucket = cluster.openBucket("test");
        // Create a JSON document and store it with the ID "helloworld"
        JsonObject content = JsonObject.create().put("hello", "world");
        bucket.upsert(JsonDocument.create("test1",content));

        // Read the document and print the "hello" field
        JsonDocument found = bucket.get("helloworld");
        cluster.disconnect();
    }
}
