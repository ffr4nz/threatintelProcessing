package flinkthreatintel;

import flinkthreatintel.features.ThreatIntelFeature;
import flinkthreatintel.utils.ConfigProperties;
import org.apache.flink.api.common.functions.RichFlatMapFunction;
import org.apache.flink.api.java.tuple.Tuple2;
import org.apache.flink.util.Collector;

import java.io.IOException;
import java.util.HashMap;

public class GetFeature extends RichFlatMapFunction<Tuple2<String, HashMap>, Tuple2<String, HashMap>> {
    private  static final String THREAT_INTEL;

    static {
        String THREAT_INTEL1;
        try {
            THREAT_INTEL1 = ConfigProperties.getProp("threat_intel_url");
        } catch (IOException e) {
            THREAT_INTEL1 ="";
            e.printStackTrace();
        }
        THREAT_INTEL = THREAT_INTEL1;
    }

    private String featureName;
    public GetFeature(String parameters) {featureName = parameters;}

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