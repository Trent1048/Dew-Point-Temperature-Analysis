import java.io.IOException;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Job;
import org.apache.hadoop.mapreduce.Mapper;
import org.apache.hadoop.mapreduce.Reducer;
import org.apache.hadoop.mapreduce.lib.input.FileInputFormat;
import org.apache.hadoop.mapreduce.lib.output.FileOutputFormat;

// $HADOOP_HOME/bin/hadoop com.sun.tools.javac.Main DewTemp.java
// jar cf dt.jar DewTemp*.class
// $HADOOP_HOME/bin/hadoop jar dt.jar DewTemp ./input ./output

public class DewTemp {

    public static class DewTempMapper
        extends Mapper<Object, Text, Text, Text>{

        private Text date = new Text();
        private Text temperatures = new Text();

        public void map(Object key, Text value, Context context
            ) throws IOException, InterruptedException {

            String[] tokens = value.toString().split("\\s*,\\s*");
            if (tokens[0].equals("STATION"))
                return;

            date.set(tokens[5].split(" ")[0]);
            String normalTemp = tokens[6];
            String dewTemp = tokens[7];
            temperatures.set(normalTemp + ", " + dewTemp);

            context.write(date, temperatures);
        }
    }

    public static class DewTempReducer
        extends Reducer<Text,Text,Text,Text> {

        private Text result = new Text();

        public void reduce(Text key, Iterable<Text> values, Context context
            ) throws IOException, InterruptedException {
            double totalNormalTemp = 0.0;
            double totalDewTemp = 0.0;
            int count = 0;
            for (Text val : values) {
                String[] temps = val.toString().split(", ");
                totalNormalTemp += Double.valueOf(temps[0]);
                totalDewTemp += Double.valueOf(temps[1]);
                count++;
            }
            result.set(String.valueOf(totalNormalTemp / count) + ", " +
                String.valueOf(totalDewTemp / count));
            context.write(key, result);
        }
    }

    public static void main(String[] args) throws Exception {
        Configuration conf = new Configuration();
        Job job = Job.getInstance(conf, "dew temp");
        job.setJarByClass(DewTemp.class);
        job.setMapperClass(DewTempMapper.class);
        job.setCombinerClass(DewTempReducer.class);
        job.setReducerClass(DewTempReducer.class);
        job.setOutputKeyClass(Text.class);
        job.setOutputValueClass(Text.class);
        FileInputFormat.addInputPath(job, new Path(args[0]));
        FileOutputFormat.setOutputPath(job, new Path(args[1]));
        System.exit(job.waitForCompletion(true) ? 0 : 1);
    }
}