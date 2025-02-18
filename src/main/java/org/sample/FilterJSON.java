package org.sample;

import java.io.FileReader;
import java.io.FileWriter;
import java.io.IOException;

import org.json.simple.JSONArray;
import org.json.simple.JSONObject;
import org.json.simple.parser.JSONParser;
import org.json.simple.parser.ParseException;

public class FilterJSON {

    public static void main(String[] args) {
        JSONParser parser = new JSONParser();

        try (FileReader reader = new FileReader("C:\\work\\Flink_POC\\test2.json");
             FileReader reader2 = new FileReader("C:\\work\\Flink_POC\\test_icpcr.json")) {
            // Read JSON file
            Object obj = parser.parse(reader);
            JSONArray personList = (JSONArray) obj;
            //personList.add(obj);
            JSONArray additionalData = (JSONArray) parser.parse(reader2);

            // Extract icpsr values from additionalData
            JSONArray icpsrList = new JSONArray();
            for (Object obj1 : additionalData) {
                JSONObject jsonObject = (JSONObject) obj1;
                Long icpsr = (Long) jsonObject.get("icpsr");
                if (icpsr != null) {
                    icpsrList.add(icpsr);
                }
            }

            // Create a new JSON array to hold filtered data
            JSONArray filteredList = new JSONArray();


            // Iterate over person array and add icpsr values
            int icpsrIndex = 0;
            for (Object personObj : personList) {
                JSONObject person = (JSONObject) personObj;

                // Create a new JSON object to hold filtered fields
                JSONObject filteredPerson = new JSONObject();
                filteredPerson.put("bioguide_id", person.get("bioguide_id"));
                filteredPerson.put("bioname", person.get("bioname"));
                filteredPerson.put("born", person.get("born"));

                // Add icpsr field from icpsrList
                if (icpsrIndex < icpsrList.size()) {
                    filteredPerson.put("icpsr", icpsrList.get(icpsrIndex));
                    icpsrIndex++;
                }

                // Add filtered person to the new JSON array
                filteredList.add(filteredPerson);
            }

            // Write filtered data to a new JSON file
            try (FileWriter file = new FileWriter("C:\\work\\Flink_POC\\filtered_voteview_persons.json")) {
                file.write(filteredList.toJSONString());
                file.flush();
            }

        } catch (IOException | ParseException e) {
            e.printStackTrace();
        }
    }
}
