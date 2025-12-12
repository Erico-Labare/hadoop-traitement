package com.tp.hadoop;

import org.apache.hadoop.io.*;
import org.apache.hadoop.mapreduce.Mapper;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.fs.FSDataInputStream;

import java.io.IOException;
import java.util.Properties;

public class DataProfilerMapper extends Mapper<LongWritable, Text, Text, DoubleWritable> {

    private boolean skipHeader = true;
    private boolean headerSkipped = false;

    private String delimiter = ",";
    private int expectedColumns = -1;
    private int numericColumn = -1;

    private static final Text TOTAL_RECORDS = new Text("TOTAL_RECORDS");
    private static final Text TOTAL_INVALID = new Text("TOTAL_INVALID");
    private static final Text TOTAL_AMOUNT = new Text("TOTAL_AMOUNT");

    @Override
    protected void setup(Context context) throws IOException {
        Configuration conf = context.getConfiguration();

        // Récupérer le chemin du fichier properties depuis la configuration
        String propertiesPath = conf.get("custom.properties.path");
        if (propertiesPath == null) {
            throw new IOException("Le chemin du fichier .properties n'a pas été fourni au Mapper !");
        }

        System.out.println("Chargement configuration depuis HDFS : " + propertiesPath);

        // Lire le fichier properties depuis HDFS
        Properties props = new Properties();
        Path propPath = new Path(propertiesPath);
        FileSystem fs = FileSystem.get(conf);
        if (!fs.exists(propPath)) {
            throw new IOException("Le fichier properties n'existe pas dans HDFS : " + propertiesPath);
        }

        try (FSDataInputStream in = fs.open(propPath)) {
            props.load(in);
        }

        delimiter = props.getProperty("delimiter", ",");
        expectedColumns = Integer.parseInt(props.getProperty("expected_columns", "-1"));
        numericColumn = Integer.parseInt(props.getProperty("numeric_column", "-1"));
        skipHeader = Boolean.parseBoolean(props.getProperty("skip_header", "true"));
    }

    @Override
    protected void map(LongWritable key, Text value, Context context) throws IOException, InterruptedException {

        String line = value.toString().trim();
        if (line.isEmpty()) return;

        // Ignorer l'en-tête si nécessaire
        if (skipHeader && !headerSkipped) {
            headerSkipped = true;
            context.getCounter("DATA_PROFILE_STATS", "HEADER_LINE").increment(1);
            return;
        }

        String[] fields = line.split(delimiter, -1);

        // Vérification du nombre de colonnes
        if (expectedColumns > 0 && fields.length != expectedColumns) {
            context.getCounter("DATA_PROFILE_STATS", "MALFORMED_RECORDS").increment(1);
            System.err.println("ERREUR: Ligne malformée : " + line);
            context.write(TOTAL_INVALID, new DoubleWritable(1));
            return;
        }

        // Ligne valide
        context.write(TOTAL_RECORDS, new DoubleWritable(1.0));

        // Calcul numérique optionnel
        if (numericColumn >= 0 && numericColumn < fields.length) {
            try {
                String numericValue = fields[numericColumn].replace(",", ".").trim();
                double amount = Double.parseDouble(numericValue);
                context.write(TOTAL_AMOUNT, new DoubleWritable(amount));
            } catch (Exception e) {
                context.getCounter("DATA_PROFILE_STATS", "INVALID_NUMERIC_FORMAT").increment(1);
                System.err.println("ERREUR: Valeur numérique invalide : " + fields[numericColumn]);
            }
        }
    }
}
