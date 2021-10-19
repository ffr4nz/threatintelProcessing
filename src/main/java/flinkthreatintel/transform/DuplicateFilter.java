package flinkthreatintel.transform;

import com.mongodb.ConnectionString;
import com.mongodb.MongoClientSettings;
import com.mongodb.client.FindIterable;
import com.mongodb.client.MongoClient;
import com.mongodb.client.MongoClients;
import com.mongodb.client.MongoDatabase;
import flinkthreatintel.utils.ConfigProperties;
import org.apache.flink.api.common.functions.FilterFunction;
import org.apache.flink.api.java.tuple.Tuple2;
import org.bson.Document;

import java.util.HashMap;

public class DuplicateFilter implements FilterFunction<Tuple2<String, HashMap>> {

    private String Status;
    private String Collection;
    private String Database;

    public DuplicateFilter(String status, String collection, String database) {
        Status = status;
        Collection = collection;
        Database = database;
    }

    @Override
    public boolean filter(Tuple2<String, HashMap> domainHashMap) throws Exception {

        String connString = ConfigProperties.getProp("connection_uri");
        ConnectionString connectionString = new ConnectionString(connString);
        MongoClientSettings settings = MongoClientSettings.builder()
                .applyConnectionString(connectionString)
                .build();
        MongoClient mongoClient = MongoClients.create(settings);
        MongoDatabase database = mongoClient.getDatabase(Database);
        Document document = new Document();
        document.append("domain", domainHashMap.f0);
        //document.append("status", Status);
        FindIterable<Document> documents = database.getCollection(Collection).find(document);
        Integer documentCount = 0;
        for (Document d : documents) {
            documentCount = documentCount +1;
            break;
        }
        mongoClient.close();
        if (documentCount > 0){
            // Duplicated domain. Stop stream.
            return false;
        }
        else{
            // New domain in the system. The stream must go on.
            return true;
        }
    }
}