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
import java.util.ArrayList;
import java.util.List;

public class ProductReviewMR6 {
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

            outputKey.set(product_id + "," + review_id);
            outputValue.set(helpful_votes + "," + total_votes);
            context.write(outputKey, outputValue);
        }
    }


    public static class Reduce extends Reducer<Text, Text, Text, Text> {
        String prev_product = null;
        String prev_review = null;
        float max_helpful_rate = 0;
        int helpful_count = 0;
        List<String> best_reviews = null;
        Text outputKey = new Text();
        Text outputValue = new Text();

        @Override
        public void reduce(Text key, Iterable<Text> values, Context context)
                throws IOException, InterruptedException {
            String[] k = key.toString().split(",");
            String product_id = k[0];
            String review_id = k[1];
            for(Text value : values){
                String[] v = value.toString().split(",");
                int helpful_votes = Integer.valueOf(v[0]);
                int total_votes = Integer.valueOf(v[1]);

                if(!product_id.equals(prev_product)){
                    prev_product = product_id;
                    prev_review = review_id;
                    max_helpful_rate = total_votes == 0 ? 0 : (float)helpful_votes / total_votes;
                    helpful_count = 0;
                    best_reviews = new ArrayList<>();
                    best_reviews.add(review_id);
                }else if(!prev_review.equals(review_id)){
                    float helpful_rate = total_votes == 0 ? 0 : (float)helpful_votes / total_votes;
                    if(helpful_rate > max_helpful_rate){
                        max_helpful_rate = helpful_rate;
                        helpful_count = helpful_votes;
                        best_reviews.clear();
                        best_reviews.add(review_id);
                    }else if(helpful_rate > 0 && helpful_rate == max_helpful_rate){
                        if(helpful_votes > helpful_count){
                            helpful_count = helpful_votes;
                            best_reviews.clear();
                            best_reviews.add(review_id);
                        }else if(helpful_votes == helpful_count){
                            best_reviews.add(review_id);
                        }
                    }
                    prev_review = review_id;
                }
            }

            if(max_helpful_rate > 0){
                outputKey.set(best_reviews.toString());
                outputValue.set(" is(are) the most helpful review for " + prev_product + ", with rate " +
                        String.format("%.3f", max_helpful_rate) + ", helpful vote count=" + helpful_count);
                context.write(outputKey, outputValue);
            }
        }
    }
    public static void main(String[] args) throws IOException, ClassNotFoundException, InterruptedException {
        Configuration conf = new Configuration();
        String[] otherArgs = new GenericOptionsParser(conf, args).getRemainingArgs();
        if (otherArgs.length != 2) {
            System.err.println("Usage: ProductReviewMR6 <in> <out>");
            System.exit(2);
        }

        conf.set("mapred.textoutputformat.separator", "\t");
        // create a new job
        Job job = Job.getInstance(conf, "productReview-mapreduce");
        job.setJarByClass(ProductReviewMR6.class);

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
