package amazonsales; 

import org.apache.hadoop.conf.Configuration;
 
import org.apache.hadoop.fs.Path;
 
import org.apache.hadoop.io.IntWritable;
 
import org.apache.hadoop.io.Text;
 
import org.apache.hadoop.mapreduce.Job;
 
import org.apache.hadoop.mapreduce.Mapper;
 
import org.apache.hadoop.mapreduce.Reducer;
 
import org.apache.hadoop.mapreduce.lib.input.FileInputFormat;
 
import org.apache.hadoop.mapreduce.lib.output.FileOutputFormat;
 
import java.io.IOException;

public class sales {

    public static class SalesMapper extends Mapper<Object, Text, Text, IntWritable> {

        private static int productNameIdx = -1;
 
        private static int quantityIdx = -1;

        private Text product = new Text();
 
        private IntWritable quantity = new IntWritable();

        public void map(Object key, Text value, Context context) throws IOException, InterruptedException {

            String line = value.toString();
 
            String[] fields = line.split(",");

            if (fields.length < 2) return;

            if (fields[0].equalsIgnoreCase("OrderID")) {
 
                for (int i = 0; i < fields.length; i++) {
 
                    String col = fields[i].trim();
 
                    if (col.equalsIgnoreCase("ProductName")) productNameIdx = i;
 
                    if (col.equalsIgnoreCase("Quantity")) quantityIdx = i;
 
                }
 
                return;
 
            }

            if (productNameIdx == -1 || quantityIdx == -1) return;
 
            if (fields.length <= productNameIdx || fields.length <= quantityIdx) return;

            String productName = fields[productNameIdx].trim();
 
            String qtyStr = fields[quantityIdx].trim();

            if (productName.length() == 0 || qtyStr.length() == 0) return;

            int qty;
 
            try {
 
                qty = Integer.parseInt(qtyStr);
 
            } catch (Exception e) {
 
                return;
 
            }

            product.set(productName);
 
            quantity.set(qty);
 
            context.write(product, quantity);
 
        }
 
    }

    public static class SalesReducer extends Reducer<Text, IntWritable, Text, IntWritable> {

        private String[] topProducts = new String[10];
 
        private int[] topQuantities = new int[10];

        public void reduce(Text key, Iterable<IntWritable> values, Context context)
 
                throws IOException, InterruptedException {

            int sum = 0;
 
            for (IntWritable v : values) {
 
                sum += v.get();
 
            }

            for (int i = 0; i < 10; i++) {
 
                if (topProducts[i] == null || sum > topQuantities[i]) {
 
                    for (int j = 9; j > i; j--) {
 
                        topProducts[j] = topProducts[j - 1];
 
                        topQuantities[j] = topQuantities[j - 1];
 
                    }
 
                    topProducts[i] = key.toString();
 
                    topQuantities[i] = sum;
 
                    break;
 
                }
 
            }
 
        }

        protected void cleanup(Context context) throws IOException, InterruptedException {
 
            for (int i = 0; i < 10; i++) {
 
                if (topProducts[i] != null) {
 
                    context.write(new Text(topProducts[i]), new IntWritable(topQuantities[i]));
 
                }
 
            }
 
        }
 
    }

    public static void main(String[] args) throws Exception {

        Configuration conf = new Configuration();
 
        Job job = Job.getInstance(conf, "Top 10 Most Purchased Products");

        job.setJarByClass(sales.class);
 
        job.setMapperClass(SalesMapper.class);
 
        job.setReducerClass(SalesReducer.class);

        job.setOutputKeyClass(Text.class);
 
        job.setOutputValueClass(IntWritable.class);

        job.setNumReduceTasks(1);

        FileInputFormat.addInputPath(job, new Path(args[0]));
 
        FileOutputFormat.setOutputPath(job, new Path(args[1]));

        System.exit(job.waitForCompletion(true) ? 0 : 1);
 
    }
 
}
 
 
