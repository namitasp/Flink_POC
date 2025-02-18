
package org.sample;

import software.amazon.awssdk.auth.credentials.AwsBasicCredentials;
import software.amazon.awssdk.auth.credentials.StaticCredentialsProvider;
import software.amazon.awssdk.regions.Region;
import software.amazon.awssdk.services.s3.S3Client;
import software.amazon.awssdk.services.s3.model.GetObjectRequest;
import software.amazon.awssdk.services.s3.model.PutObjectRequest;
//import org.apache.flink.api.common.functions.MapFunction;
//import org.apache.flink.streaming.api.datastream.DataStream;
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;
import org.json.simple.JSONArray;
import org.json.simple.JSONObject;
import org.json.simple.parser.JSONParser;
//import org.json.simple.parser.ParseException;
//import software.amazon.awssdk.services.s3.model.S3Exception;

import java.io.BufferedReader;
import java.io.File;
import java.io.FileWriter;
//import java.io.IOException;
import java.io.InputStreamReader;
import java.nio.file.Paths;

public class FlinkJSON {
    public static void main(String[] args) {
        // Set up the execution environment
        final StreamExecutionEnvironment env = StreamExecutionEnvironment.getExecutionEnvironment();

        // AWS S3 configuration
        String bucketName = "prameya-development";
        String inputKey1 = "src/test2.json";
        String inputKey2 = "src/test3.json";
        String outputKey = "dest/filtered_voteview_persons.json";

        AwsBasicCredentials awsCreds = AwsBasicCredentials.create("AKIA6JMOE4OIQEYUSYXL", "cDYQvBVUt9ac+yv7o3mvopYtec9wUefiu7XRZ0UZ");
        S3Client s3 = S3Client.builder()
                .region(Region.US_EAST_1)
                .credentialsProvider(StaticCredentialsProvider.create(awsCreds))
                .build();

        try {
            // Read JSON files from S3
            GetObjectRequest getObjectRequest1 = GetObjectRequest.builder()
                    .bucket(bucketName)
                    .key(inputKey1)
                    .build();
            GetObjectRequest getObjectRequest2 = GetObjectRequest.builder()
                    .bucket(bucketName)
                    .key(inputKey2)
                    .build();

            BufferedReader reader1 = new BufferedReader(new InputStreamReader(s3.getObject(getObjectRequest1)));
            BufferedReader reader2 = new BufferedReader(new InputStreamReader(s3.getObject(getObjectRequest2)));
            JSONParser parser = new JSONParser();
            JSONArray personList = (JSONArray) parser.parse(reader1);
            JSONArray additionalData = (JSONArray) parser.parse(reader2);

            // Extract icpsr values from additionalData
            JSONArray icpsrList = new JSONArray();
            for (Object obj : additionalData) {
                JSONObject jsonObject = (JSONObject) obj;
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
            File tempFile = File.createTempFile("filtered_voteview_persons", ".json");
            try (FileWriter file = new FileWriter(tempFile)) {
                file.write(filteredList.toJSONString());
                file.flush();
            }

            // Upload the filtered JSON file to S3
            PutObjectRequest putObjectRequest = PutObjectRequest.builder()
                    .bucket(bucketName)
                    .key(outputKey)
                    .build();

            s3.putObject(putObjectRequest, Paths.get(tempFile.getAbsolutePath()));

            // Execute the Flink job
            env.execute("Filter JSON Data");

        } catch (Exception e) {
            e.printStackTrace();
        }
    }
}
