
/**
 * Licensed to the Apache Software Foundation (ASF) under one
 * or more contributor license agreements.  See the NOTICE file
 * distributed with this work for additional information
 * regarding copyright ownership.  The ASF licenses this file
 * to you under the Apache License, Version 2.0 (the
 * "License"); you may not use this file except in compliance
 * with the License.  You may obtain a copy of the License at
 *
 *     http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */

import java.io.IOException;
import java.util.StringTokenizer;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.io.IntWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Job;
import org.apache.hadoop.mapreduce.Mapper;
import org.apache.hadoop.mapreduce.Reducer;
import org.apache.hadoop.mapreduce.lib.input.FileInputFormat;
import org.apache.hadoop.mapreduce.lib.output.FileOutputFormat;
import org.apache.hadoop.mapreduce.lib.input.MultipleInputs;
import org.apache.hadoop.mapreduce.lib.input.TextInputFormat;
import org.apache.hadoop.util.GenericOptionsParser;


public class SQL {

    //Mapper for city.txt
    public static class CityMapper
            extends Mapper<Object, Text, Text, Text> {
        private Text country_code = new Text();
        public void map(Object key, Text value, Context context
        ) throws IOException, InterruptedException {
            String record = value.toString();
            String[] parts = record.split("\t");
            //parts[2] - CountryCode
            //parts[1] - Name
            //parts[4] - Population
            country_code.set(parts[2]);
            if (Integer.parseInt(parts[4]) >= 1000000){
                context.write(country_code, new Text("City\t" + parts[1]));
            }
        }
    }

    //Mapper for country.txt
    public static class CountryMapper
            extends Mapper<Object, Text, Text, Text> {
        private Text code = new Text();
        public void map(Object key, Text value, Context context
        ) throws IOException, InterruptedException {
            String record = value.toString();
            String[] parts = record.split("\t");
            //parts[0] - Code
            //parts[1] - Name
            code.set(parts[0]);
                context.write(code, new Text("Country\t" + parts[1]));
        }
    }


    public static class CountReducer
            extends Reducer<Text,Text,Text,Text> {
        //Reducer
        public void reduce(Text key, Iterable<Text> values,
                           Context context
        ) throws IOException, InterruptedException {
            String name = "";
            int count = 0;
            for(Text t : values){
                String parts[] = t.toString().split("\t");
                if (parts[0].equals("Country")) {
                    name = parts[1];
                }
                else if (parts[0].equals("City")){
                    count ++;
                }
            }
            if(count >=3 ){
                context.write(new Text(name),new Text(Integer.toString(count)));
            }
            }
            }


    public static void main(String[] args) throws Exception {
        Configuration conf = new Configuration();
        String[] otherArgs = new GenericOptionsParser(conf, args).getRemainingArgs();
        if (otherArgs.length < 2) {
            System.err.println("Usage: SQLCount <in> [<in>...] <out>");
            System.exit(2);
        }
        Job job = Job.getInstance(conf, "SQL Count");
        job.setJarByClass(SQL.class);
        job.setReducerClass(SQL.CountReducer.class);
        job.setOutputKeyClass(Text.class);
        job.setOutputValueClass(Text.class);
        MultipleInputs.addInputPath(job, new Path(args[0]),TextInputFormat.class, CityMapper.class);
        MultipleInputs.addInputPath(job, new Path(args[1]),TextInputFormat.class, CountryMapper.class);
        Path outputPath = new Path(args[2]);

        FileOutputFormat.setOutputPath(job, outputPath);
        //outputPath.getFileSystem(conf).delete(outputPath);
        System.exit(job.waitForCompletion(true) ? 0 : 1);
    }
}
