package FinalProject;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.io.LongWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Job;
import org.apache.hadoop.mapreduce.Mapper;
import org.apache.hadoop.mapreduce.Reducer;
import org.apache.hadoop.mapreduce.lib.input.FileInputFormat;
import org.apache.hadoop.mapreduce.lib.input.TextInputFormat;
import org.apache.hadoop.mapreduce.lib.output.FileOutputFormat;
import org.apache.hadoop.mapreduce.lib.output.TextOutputFormat;
import org.apache.hadoop.util.GenericOptionsParser;

import java.io.IOException;

public class ProductReviewMR1 {
    public static class Map extends Mapper<LongWritable, Text, Text, Text> {
        @Override
        public void map(LongWritable key, Text value, Context context) throws IOException, InterruptedException {
            String header = "^marketplace.*";
            String line = value.toString();
            if(line.matches(header)){
                return;
            }

            String[] fields = line.split("\t");
            int field_num = fields.length;
            if(field_num != 15){
                return;
            }

            String marketplace = fields[0];
            String customer_id = fields[1];
            String review_id = fields[2];
            String product_id = fields[3];
            String product_parent = fields[4];
            String product_title = fields[5];
            String product_category = fields[6];
            String star_rating = fields[7];
            String helpful_votes = fields[8];
            String total_votes = fields[9];
            String vine = fields[10];
            String verified_purchase = fields[11];
            String review_headline = fields[12];
            String review_body = fields[13];
            String review_date = fields[14];


            Text outputKey = new Text();
            Text outputValue = new Text();

            outputKey.set(product_id);
            outputValue.set(star_rating);
            context.write(outputKey, outputValue);
        }
    }


    public static class Reduce extends Reducer<Text, Text, Text, Text> {
        String prev_id = null;
        int total_stars = 0;
        int total_reviews = 0;
        Text outputKey = new Text();
        Text outputValue = new Text();

        @Override
        public void reduce(Text product_id, Iterable<Text> values, Context context)
                throws IOException, InterruptedException {
            for(Text value : values){
                int stars = Integer.valueOf(value.toString());
                if(!product_id.toString().equals(prev_id)){
//                    if(prev_id != null){
//                        outputKey.set(prev_id);
//                        outputValue.set(" has " + total_stars + " stars in " + total_reviews +
//                                        " total review avg="+ String.format("%.1f", (float)total_stars/total_reviews));
//                        context.write(outputKey, outputValue);
//                    }
                    prev_id = product_id.toString();
                    total_stars = 0;
                    total_reviews = 0;
                }
                total_reviews += 1;
                total_stars += stars;
            }
            outputKey.set(prev_id);
            outputValue.set(" has " + total_stars + " stars in " + total_reviews +
                            " total review avg="+ String.format("%.1f", (float)total_stars/total_reviews));
            context.write(outputKey, outputValue);
        }
    }
    public static void main(String[] args) throws IOException, ClassNotFoundException, InterruptedException {
        Configuration conf = new Configuration();
        String[] otherArgs = new GenericOptionsParser(conf, args).getRemainingArgs();
        if (otherArgs.length != 2) {
            System.err.println("Usage: ProductReviewMR1 <in> <out>");
            System.exit(2);
        }

        conf.set("mapred.textoutputformat.separator", "\t");
        // create a new job
        Job job = Job.getInstance(conf, "productReview-mapreduce");
        job.setJarByClass(ProductReviewMR1.class);

        // specify output types
        job.setOutputKeyClass(Text.class);
        job.setOutputValueClass(Text.class);

        // specify a mapper
        job.setMapperClass(Map.class);

        // specify a reducer
        job.setReducerClass(Reduce.class);

        job.setInputFormatClass(TextInputFormat.class);
        job.setOutputFormatClass(TextOutputFormat.class);

        // specify input and output dirs
        FileInputFormat.addInputPath(job, new Path(otherArgs[0]));
        FileOutputFormat.setOutputPath(job, new Path(otherArgs[1]));

        //job.waitForCompletion(true);
        System.exit(job.waitForCompletion(true) ? 0 : 1);
    }
}
