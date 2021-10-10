package flinkthreatintel.features;

import twitter4j.JSONObject;

import java.io.BufferedReader;
import java.io.InputStreamReader;
import java.net.HttpURLConnection;
import java.net.URL;

public class ThreatIntelFeature {

    public static String getHTML(String urlToRead) throws Exception {
        StringBuilder result = new StringBuilder();
        URL url = new URL(urlToRead);
        HttpURLConnection conn = (HttpURLConnection) url.openConnection();
        conn.setRequestMethod("GET");
        BufferedReader rd = new BufferedReader(new InputStreamReader(conn.getInputStream()));
        String line;
        while ((line = rd.readLine()) != null) {
            result.append(line);
        }
        rd.close();
        return result.toString();
    }
    public static String threatIntelFeature(String domain, String featureName, String THREAT_INTEL) throws Exception {
        String URL = THREAT_INTEL.replaceAll("FEATURE", featureName).replaceAll("DOMAIN", domain);
        try{
            String response = getHTML(URL);
            JSONObject obj = new JSONObject(response);
            return obj.toString();
        } catch (Exception a){
            System.out.println(a.toString());
            return "{}";
        }

    }
}
