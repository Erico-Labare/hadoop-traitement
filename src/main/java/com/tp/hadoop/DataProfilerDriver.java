package com.tp.hadoop;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.mapreduce.Job;
import org.apache.hadoop.mapreduce.lib.input.FileInputFormat;
import org.apache.hadoop.mapreduce.lib.output.FileOutputFormat;

public class DataProfilerDriver {

    public static void main(String[] args) throws Exception {

        if (args.length < 2) {
            System.err.println("Usage : DataProfilerDriver <input_csv_file> <output_path>");
            System.exit(1);
        }

        String inputPath = args[0];
        String outputPath = args[1];

        // Utiliser le fichier .properties déjà existant dans le même dossier que le CSV
        Path input = new Path(inputPath);
        String parent = input.getParent().toString(); // dossier HDFS
        String fileName = input.getName();            // nom du CSV
        String propertiesPath = parent + "/" + fileName.replace(".csv", ".properties");

        // Vérifier que le fichier properties existe dans HDFS
        Configuration conf = new Configuration();
        FileSystem fs = FileSystem.get(conf);
        Path propPath = new Path(propertiesPath);
        if (!fs.exists(propPath)) {
            System.err.println("Fichier properties introuvable dans HDFS : " + propertiesPath);
            System.exit(1);
        }

        // Injecter le chemin du properties dans la configuration Hadoop
        conf.set("custom.properties.path", propertiesPath);
        System.out.println("🔧 Utilisation du fichier properties existant : " + propertiesPath);

        // Configuration du Job
        Job job = Job.getInstance(conf, "CSV Data Profiler");
        job.setJarByClass(DataProfilerDriver.class);

        job.setMapperClass(DataProfilerMapper.class);
        job.setReducerClass(DataProfilerReducer.class);

        // Types de sortie du Mapper
        job.setMapOutputKeyClass(org.apache.hadoop.io.Text.class);
        job.setMapOutputValueClass(org.apache.hadoop.io.DoubleWritable.class);

        // Types de sortie du Reducer
        job.setOutputKeyClass(org.apache.hadoop.io.Text.class);
        job.setOutputValueClass(org.apache.hadoop.io.DoubleWritable.class);

        // Input / Output
        FileInputFormat.addInputPath(job, new Path(inputPath));
        FileOutputFormat.setOutputPath(job, new Path(outputPath));

        // 3) Lancer le Job
        System.exit(job.waitForCompletion(true) ? 0 : 1);
    }
}


