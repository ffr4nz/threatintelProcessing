package flinkthreatintel;


import flinkthreatintel.features.AllFeatures;
import flinkthreatintel.features.GetFeature;
import org.apache.flink.api.common.functions.MapFunction;
import org.apache.flink.api.java.tuple.Tuple2;
import org.apache.flink.api.java.tuple.Tuple3;
import org.apache.flink.streaming.api.CheckpointingMode;
import org.apache.flink.streaming.api.datastream.DataStream;
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;


import java.util.HashMap;

public class StreamingJob {

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

        DataStream<Tuple2<String, HashMap>> mongodb = domainHashMapStream.flatMap(new MongoDB());

        mongodb.print();

        // Features
        DataStream<Tuple2<String, HashMap>> ddns = domainHashMapStream.flatMap(new GetFeature("ddns"));
        DataStream<Tuple2<String, HashMap>> idnhattack = ddns.flatMap(new GetFeature("idnhattack"));
        DataStream<Tuple2<String, HashMap>> favicon = idnhattack.flatMap(new GetFeature("favicon"));
        DataStream<Tuple2<String, HashMap>> strcomparison = favicon.flatMap(new GetFeature("strcomparison"));
        DataStream<Tuple2<String, HashMap>> webshell = strcomparison.flatMap(new GetFeature("webshell"));
        DataStream<Tuple2<String, HashMap>> domainage = webshell.flatMap(new GetFeature("domainage"));
        DataStream<Tuple2<String, HashMap>> dnsttl = domainage.flatMap(new GetFeature("dnsttl"));
        DataStream<Tuple2<String, HashMap>> fw = dnsttl.flatMap(new GetFeature("fw"));
        DataStream<Tuple2<String, HashMap>> numberips = fw.flatMap(new GetFeature("numberips"));
        DataStream<Tuple2<String, HashMap>> numbercountries = numberips.flatMap(new GetFeature("numbercountries"));
        DataStream<Tuple2<String, HashMap>> subdomains = numbercountries.flatMap(new GetFeature("subdomains"));
        DataStream<Tuple2<String, HashMap>> hsts = subdomains.flatMap(new GetFeature("hsts"));
        DataStream<Tuple2<String, HashMap>> iframe = hsts.flatMap(new GetFeature("iframe"));
        DataStream<Tuple2<String, HashMap>> sfh = iframe.flatMap(new GetFeature("sfh"));
        DataStream<Tuple2<String, HashMap>> formmail = sfh.flatMap(new GetFeature("formmail"));
        DataStream<Tuple2<String, HashMap>> msltags = formmail.flatMap(new GetFeature("msltags"));
        DataStream<Tuple2<String, HashMap>> phishingbrands = msltags.flatMap(new GetFeature("phishingbrands"));
        DataStream<Tuple2<String, HashMap>> tor = phishingbrands.flatMap(new GetFeature("tor"));
        DataStream<Tuple2<String, HashMap>> tldprice = tor.flatMap(new GetFeature("tldprice"));
        DataStream<Tuple2<String, HashMap>> entropy = tldprice.flatMap(new GetFeature("entropy"));
        DataStream<Tuple2<String, HashMap>> vowels = entropy.flatMap(new GetFeature("vowels"));
        DataStream<Tuple2<String, HashMap>> consonants = vowels.flatMap(new GetFeature("consonants"));
        DataStream<Tuple2<String, HashMap>> length = consonants.flatMap(new GetFeature("length"));
        DataStream<Tuple3<String,String, String>> features = length.flatMap(new AllFeatures());
        features.print();
        // Execute Flow
        env.execute("ThreatIntel");
    }
}