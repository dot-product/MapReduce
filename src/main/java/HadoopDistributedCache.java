
import org.apache.hadoop.io.IntWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Job;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.mapreduce.Mapper;
import org.apache.hadoop.mapreduce.Reducer;
//import org.apache.hadoop.util.GenericOptionsParser;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.mapreduce.lib.input.FileInputFormat;
import org.apache.hadoop.mapreduce.lib.output.FileOutputFormat;
import org.apache.hadoop.filecache.DistributedCache;

import java.io.BufferedReader;
import java.io.FileReader;
import java.io.IOException;
import java.util.HashSet;
import java.util.Set;
import java.util.StringTokenizer;


public class HadoopDistributedCache {

    public static  class TokenizerMapper extends Mapper<Object,Text,Text,IntWritable>{

        private  static IntWritable writable = new IntWritable(1);
        private  Text text = new Text();
        private Set patternSet = new HashSet();

        @Override
        /*
        * setup is the first function executed by mapreduce framework before executing the Map Task.
        *
        * Cleanup is the last function executed after completion of Map task.
        *
        * */
        protected void setup(Context context) throws IOException, InterruptedException {
            Path[] files = DistributedCache.getLocalCacheFiles(context.getConfiguration());
            BufferedReader bufferedReader;
            String line = null;
            if (files != null && files.length != 0) {
                for (Path path : files) {

                    bufferedReader = new BufferedReader(new FileReader(path.toString()));
                    while ((line = bufferedReader.readLine()) != null) {

                        patternSet.add(line.trim());
                    }

                }
            }
        }

        public void map(Object key, Text val, Context context){

            StringTokenizer tokenizer = new StringTokenizer(val.toString());
            String token;

            while (tokenizer.hasMoreTokens()){

                try{

                    token = tokenizer.nextToken();
                    if(patternSet.contains(token)){

                        continue;
                    }

                    text.set(token);
                    context.write( text,writable);

                }catch (IOException e){

                }catch (InterruptedException e){

                }

            }

        }

    }

    public static class CountReducer extends Reducer<Text,IntWritable,Text, IntWritable>{

        private int sum =0;
        private IntWritable intWritable = new IntWritable();

        public void reduce(Text key, Iterable<IntWritable> iterator, Context context){

            for(IntWritable itr: iterator){

                sum += itr.get();

            }
            intWritable.set(sum);
            try {
                context.write(key, intWritable);
            }catch (IOException e){

            }catch (InterruptedException e){

            }
        }


    }



    public static void main(String[] args){

        Configuration configuration = new Configuration();
        try {
            Job job = Job.getInstance(configuration, "wordcountDisCache");

//            GenericOptionsParser parser = new GenericOptionsParser(configuration, args);
//            String[] remain = parser.getRemainingArgs();
//
//            if ((remain.length != 2) && (remain.length != 4)) {
//                System.err.println("Usage: wordcount <in> <out> [-skip skipPatternFile]");
//                System.exit(2);
//            }
//            List<String> otherArgs = new ArrayList<String>();
//            for (int i=0; i < remain.length; ++i) {
//                if ("-skip".equals(remain[i])) {
//                    job.setD
//
//                    (new Path(remain[++i]).toUri());
//                    job.getConfiguration().setBoolean("wordcount.skip.patterns", true);
//                } else {
//                    otherArgs.add(remain[i]);
//                }
//            }


            DistributedCache.addCacheFile(new Path(args[2]).toUri(), configuration);

            FileInputFormat.addInputPath(job, new Path(args[0]));
            FileOutputFormat.setOutputPath(job, new Path(args[1]));

            job.setJarByClass(HadoopDistributedCache.class);
            job.setMapperClass(TokenizerMapper.class);
            job.setReducerClass(CountReducer.class);
            job.setNumReduceTasks(3);
            job.setCombinerClass(CountReducer.class);
            job.setOutputKeyClass(Text.class);
            job.setOutputValueClass(IntWritable.class);

            try{

                job.submit();

            }catch (InterruptedException e){

            }catch (ClassNotFoundException e){

            }




        }catch (IOException e){

            System.out.println(e.getStackTrace());

        }



    }
}
