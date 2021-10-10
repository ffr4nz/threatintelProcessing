package flinkthreatintel;

import flinkthreatintel.features.ThreatIntelFeature;

import org.apache.flink.api.common.functions.FlatMapFunction;
import org.apache.flink.api.common.functions.MapFunction;
import org.apache.flink.api.common.functions.RichFlatMapFunction;
import org.apache.flink.api.java.tuple.Tuple2;
import org.apache.flink.api.java.tuple.Tuple3;
import org.apache.flink.streaming.api.CheckpointingMode;
import org.apache.flink.streaming.api.datastream.DataStream;
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;
import org.apache.flink.util.Collector;
import twitter4j.JSONException;
import twitter4j.JSONObject;

import java.util.ArrayList;
import java.util.HashMap;
import java.util.Map;

public class StreamingJob {
    private  static final String THREAT_INTEL = "http://127.0.0.1:3000/ff/FEATURE/DOMAIN";

    public static void main(String[] args) throws Exception {

        StreamExecutionEnvironment env = StreamExecutionEnvironment.getExecutionEnvironment();
        env.getCheckpointConfig().setCheckpointingMode(CheckpointingMode.EXACTLY_ONCE);
        env.setParallelism(3);

        // DataStream source testing. SocketTextStream
        DataStream<String> domainStream = env.socketTextStream("localhost", 10000);
        // DataStream source production. RMQSource


        // Data model transform
        DataStream<Tuple2<String, HashMap>> domainHashMapStream = domainStream.map(new MapFunction<String, Tuple2<String, HashMap>>() {
            @Override
            public Tuple2<String, HashMap> map(String s) throws Exception {
                return new Tuple2<String,HashMap>(s, new HashMap());
            }
        });

        // Features
        DataStream<Tuple2<String, HashMap>> ddns = domainHashMapStream.flatMap(new getFeature("ddns"));
        DataStream<Tuple2<String, HashMap>> idnhattack = ddns.flatMap(new getFeature("idnhattack"));
        DataStream<Tuple2<String, HashMap>> favicon = idnhattack.flatMap(new getFeature("favicon"));
        DataStream<Tuple2<String, HashMap>> strcomparison = favicon.flatMap(new getFeature("strcomparison"));
        DataStream<Tuple2<String, HashMap>> webshell = strcomparison.flatMap(new getFeature("webshell"));
        DataStream<Tuple2<String, HashMap>> domainage = webshell.flatMap(new getFeature("domainage"));
        DataStream<Tuple2<String, HashMap>> dnsttl = domainage.flatMap(new getFeature("dnsttl"));
        DataStream<Tuple2<String, HashMap>> fw = dnsttl.flatMap(new getFeature("fw"));
        DataStream<Tuple2<String, HashMap>> numberips = fw.flatMap(new getFeature("numberips"));
        DataStream<Tuple2<String, HashMap>> numbercountries = numberips.flatMap(new getFeature("numbercountries"));
        DataStream<Tuple2<String, HashMap>> subdomains = numbercountries.flatMap(new getFeature("subdomains"));
        DataStream<Tuple2<String, HashMap>> hsts = subdomains.flatMap(new getFeature("hsts"));
        DataStream<Tuple2<String, HashMap>> iframe = hsts.flatMap(new getFeature("iframe"));
        DataStream<Tuple2<String, HashMap>> sfh = iframe.flatMap(new getFeature("sfh"));
        DataStream<Tuple2<String, HashMap>> formmail = sfh.flatMap(new getFeature("formmail"));
        DataStream<Tuple2<String, HashMap>> msltags = formmail.flatMap(new getFeature("msltags"));
        DataStream<Tuple2<String, HashMap>> phishingbrands = msltags.flatMap(new getFeature("phishingbrands"));
        DataStream<Tuple2<String, HashMap>> tor = phishingbrands.flatMap(new getFeature("tor"));
        DataStream<Tuple2<String, HashMap>> tldprice = tor.flatMap(new getFeature("tldprice"));
        DataStream<Tuple2<String, HashMap>> entropy = tldprice.flatMap(new getFeature("entropy"));
        DataStream<Tuple2<String, HashMap>> vowels = entropy.flatMap(new getFeature("vowels"));
        DataStream<Tuple2<String, HashMap>> consonants = vowels.flatMap(new getFeature("consonants"));
        DataStream<Tuple2<String, HashMap>> length = consonants.flatMap(new getFeature("length"));
        DataStream<Tuple3<String,String, String>> features = length.flatMap(new allFeatures());
        features.print();
        // Execute Flow
        env.execute("ThreatIntel");


    }

    public static class allFeatures implements FlatMapFunction<Tuple2<String, HashMap>, Tuple3<String, String, String>> {

        @Override
        public void flatMap(Tuple2<String, HashMap> domainHashMap, Collector<Tuple3<String, String, String>> collector) throws Exception {

            //Get features HashMap
            HashMap<String,String> hashMapResult;
            hashMapResult = (HashMap<String,String>) domainHashMap.f1;

            // Get domain
            String domain = domainHashMap.f0;

            // Declare features + keys arrays
            ArrayList<String> calculatedFeatures = new ArrayList<String>();
            ArrayList<String> featuresKeys = new ArrayList<String>();

            for (Map.Entry<String, String> entry : hashMapResult.entrySet()) {
                String key = entry.getKey();
                featuresKeys.add(key);
                String value = entry.getValue();
                try {
                    JSONObject jsonObject = new JSONObject(value);
                    String result = jsonObject.getString("result");
                    calculatedFeatures.add(result);
                }catch (JSONException err) {
                    System.out.println("[Error] " + err.toString());
                }
            }

            collector.collect(new Tuple3<String, String, String>( domain, String.join(";", featuresKeys),String.join(";", calculatedFeatures)));
        }
    }

    public static class getFeature extends RichFlatMapFunction<Tuple2<String, HashMap>, Tuple2<String, HashMap>> {
        private String featureName;
        public getFeature(String parameters) {featureName = parameters;}

        @Override
        public void flatMap(Tuple2<String, HashMap> domainHashMap, Collector<Tuple2<String, HashMap>> collector) throws Exception {
            System.out.println("Executing feature: " + featureName);
            String result = ThreatIntelFeature.threatIntelFeature(domainHashMap.f0, featureName, THREAT_INTEL);
            HashMap<String,String> hashMapResult;
            hashMapResult = (HashMap<String,String>) domainHashMap.f1;
            hashMapResult.put(featureName,result);
            collector.collect(new Tuple2<>(domainHashMap.f0, hashMapResult));
        }
    }





}