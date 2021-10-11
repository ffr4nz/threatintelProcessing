package flinkthreatintel.features;


import org.apache.flink.api.common.functions.FlatMapFunction;
import org.apache.flink.api.java.tuple.Tuple2;
import org.apache.flink.api.java.tuple.Tuple3;
import org.apache.flink.util.Collector;
import twitter4j.JSONException;
import twitter4j.JSONObject;

import java.util.ArrayList;
import java.util.HashMap;
import java.util.Map;

public class AllFeatures implements FlatMapFunction<Tuple2<String, HashMap>, Tuple3<String, String, String>> {

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