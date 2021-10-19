package flinkthreatintel.sink;

import com.mongodb.ConnectionString;
import com.mongodb.MongoClientSettings;
import com.mongodb.client.MongoClient;
import com.mongodb.client.MongoClients;
import com.mongodb.client.MongoDatabase;
import com.mongodb.client.model.Filters;
import com.mongodb.client.model.UpdateOptions;
import com.mongodb.client.result.UpdateResult;
import flinkthreatintel.utils.ConfigProperties;
import org.apache.flink.api.common.functions.RichFlatMapFunction;
import org.apache.flink.api.java.tuple.Tuple2;
import org.apache.flink.util.Collector;
import org.bson.Document;

import java.util.HashMap;

public class UpdateMongoDB extends RichFlatMapFunction<Tuple2<String, HashMap>, Tuple2<String, HashMap>> {

    private String Status;
    private String Collection;
    private String Database;

    public UpdateMongoDB(String status, String collection, String database) {
        Status = status;
        Collection = collection;
        Database = database;
    }

    @Override
    public void flatMap(Tuple2<String, HashMap> domainHashMap, Collector<Tuple2<String, HashMap>> collector) throws Exception {
        String connString = ConfigProperties.getProp("connection_uri");
        ConnectionString connectionString = new ConnectionString(connString);
        MongoClientSettings settings = MongoClientSettings.builder()
                .applyConnectionString(connectionString)
                .build();
        MongoClient mongoClient = MongoClients.create(settings);
        MongoDatabase database = mongoClient.getDatabase(Database);
        Document document = new Document();
        document.append("domain", domainHashMap.f0);
        document.append("status", Status);
        database.getCollection(Collection).updateOne(
                Filters.eq("domain", domainHashMap.f0),
                new Document("$set", document),
                new UpdateOptions().upsert(true));
        mongoClient.close();
    }
}
