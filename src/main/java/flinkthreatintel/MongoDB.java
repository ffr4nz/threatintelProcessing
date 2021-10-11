package flinkthreatintel;

import org.apache.flink.api.common.functions.RichFlatMapFunction;
import org.apache.flink.api.java.tuple.Tuple2;
import org.apache.flink.util.Collector;
import com.mongodb.*;
import com.mongodb.client.MongoClient;
import com.mongodb.client.MongoClients;
import com.mongodb.client.MongoDatabase;
import org.bson.Document;

import java.util.HashMap;

public class MongoDB extends RichFlatMapFunction<Tuple2<String, HashMap>, Tuple2<String, HashMap>> {

    @Override
    public void flatMap(Tuple2<String, HashMap> domainHashMap, Collector<Tuple2<String, HashMap>> collector) throws Exception {
        ConnectionString connectionString = new ConnectionString("mongodb://usereoi:pcNjYKsiOuWe7XMS@clusterf-shard-00-00.2sems.mongodb.net:27017,clusterf-shard-00-01.2sems.mongodb.net:27017,clusterf-shard-00-02.2sems.mongodb.net:27017/myFirstDatabase?ssl=true&replicaSet=ClusterF-shard-0&authSource=admin&retryWrites=true&w=majority");
        MongoClientSettings settings = MongoClientSettings.builder()
                .applyConnectionString(connectionString)
                .build();
        MongoClient mongoClient = MongoClients.create(settings);
        MongoDatabase database = mongoClient.getDatabase("domainStatus");
        Document document = new Document();
        document.append("domain", domainHashMap.f0);
        document.append("status", "born");
        database.getCollection("status").insertOne(document);
    }
}
