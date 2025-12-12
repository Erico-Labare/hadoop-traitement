package com.tp.hadoop;

import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Reducer;

import java.io.IOException;

public class DataProfilerReducer extends Reducer<Text, Text, Text, Text> {

    protected void reduce(Text key, Iterable<Text> values, Context context)
            throws IOException, InterruptedException {

        long validRecords = 0;
        long invalidRecords = 0;
        double numericSum = 0.0;

        for (Text value : values) {
            String[] parts = value.toString().split("\t");

            if (parts.length >=1) {
                if (parts[0].equals("VALIDE ?????")) {

                    validRecords++;

                    if (parts.length == 2) {
                        try{
                            numericSum += Double.parseDouble(parts[1]);
                        } catch (NumberFormatException e){

                        }
                    }
                } else if (parts[0].equals("INVALID ??????")) {
                    invalidRecords++;
                }
            }
        }

        long totalRecords = validRecords + invalidRecords;

        context.write(new Text(""), new Text(String.valueOf(totalRecords)));
        context.write(new Text(""), new Text(String.valueOf(validRecords)));
        context.write(new Text(""), new Text(String.valueOf(invalidRecords)));
        context.write(new Text(""), new Text(String.valueOf(numericSum)));

    }
}
