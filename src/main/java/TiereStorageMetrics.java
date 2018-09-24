

import com.google.gson.Gson;
import org.apache.hadoop.io.DoubleWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Job;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.mapreduce.Mapper;
import org.apache.hadoop.mapreduce.Reducer;
import org.apache.hadoop.fs.Path;

import org.apache.hadoop.mapreduce.lib.input.FileInputFormat;
import org.apache.hadoop.mapreduce.lib.output.FileOutputFormat;

import java.io.IOException;
import java.util.StringTokenizer;


public class TiereStorageMetrics {


    public static class TierStorageExtractor extends Mapper<Object, Text, Text, DoubleWritable> {

        private Text tier = new Text();
        private DoubleWritable size = new DoubleWritable();


        public void map(Object key, Text val, Context con) {

//            StringTokenizer stringTokenizer = new StringTokenizer(val.toString(), ",");
//            System.out.println(val.toString());
//            Double size = 0.0;
            Gson gson = new Gson();
            Data data = gson.fromJson(val.toString(),Data.class);

            try{

                System.out.println(data.tier+data.size);

                tier.set(data.tier);
                size.set(data.size);
                con.write(tier,size);

            }catch (IOException e){

                e.printStackTrace();

            }catch (InterruptedException e){
                e.printStackTrace();

            }



//            while (stringTokenizer.hasMoreTokens()) {
//
//                String temp = stringTokenizer.nextToken();
//                if (temp.contains("HOT")) {
//
//                    size = Double.parseDouble(temp.split(":")[1]);
//                    try {
//                        con.write(new Text("HOT"), new DoubleWritable(size));
//                    } catch (InterruptedException e) {
//
//                    } catch (IOException e) {
//
//                    }
//
//                } else if (temp.contains("COLD")) {
//
//                    size = Double.parseDouble(temp.split(":")[1]);
//                    try {
//                        con.write(new Text("COLD"), new DoubleWritable(size));
//                    } catch (InterruptedException e) {
//
//                    } catch (IOException e) {
//
//                    }
//
//
//                } else if (temp.contains("SSD")) {
//
//                    size = Double.parseDouble(temp.split(":")[1]);
//                    try {
//                        con.write(new Text("SSD"), new DoubleWritable(size));
//                    } catch (InterruptedException e) {
//
//                    } catch (IOException e) {
//
//                    }
//                }
//            }
        }


    }

    public static class TierStorageAggregator extends Reducer<Text, DoubleWritable, Text, DoubleWritable> {
        DoubleWritable result =  new DoubleWritable();

        public void reduce(Text key, Iterable<DoubleWritable> values, Context con){
            double sum = 0.0;

            for(DoubleWritable val : values){

                sum += val.get();
            }
            try{
                System.out.println(sum);
                result.set(sum);
                con.write(key,result);

            }catch (IOException e){

                e.printStackTrace();

            }catch (InterruptedException e){

                e.printStackTrace();
            }

        }


    }


    public static void main(String[] args) {

        try {

            Configuration conf = new Configuration();
            Job job = Job.getInstance(conf, "storageMetrics");
            job.setNumReduceTasks(3);
            job.setMapperClass(TierStorageExtractor.class);
            job.setCombinerClass(TierStorageAggregator.class);

            job.setReducerClass(TierStorageAggregator.class);

            FileInputFormat.addInputPath(job, new Path(args[0]));
            FileOutputFormat.setOutputPath(job, new Path(args[1]));

            job.setJarByClass(TiereStorageMetrics.class);
            job.setMapOutputKeyClass(Text.class);
            job.setMapOutputValueClass(DoubleWritable.class);
            job.setOutputKeyClass(Text.class);
            job.setOutputValueClass(DoubleWritable.class);

            job.waitForCompletion(true);

        } catch (IOException e) {

            System.out.println(e.getStackTrace());
        } catch (InterruptedException e) {

        } catch (ClassNotFoundException e) {

        }


    }
}
