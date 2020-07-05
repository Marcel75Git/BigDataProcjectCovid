package CasConta;

import org.apache.hadoop.io.IntWritable;
import org.apache.hadoop.io.LongWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Mapper;

import java.io.IOException;

public class MapperCases extends Mapper<LongWritable, Text, Text, IntWritable> {
    public void map(LongWritable key, Text value, Context context) throws IOException, InterruptedException {
        String values = value.toString();
        String line [] = values.split(",");
        String countriesAndTerritories = line[6];
        try {
            int deaths = Integer.parseInt(line[4]);
            context.write(new Text(countriesAndTerritories), new IntWritable(deaths));
        } catch (NumberFormatException nfe) {
             nfe.printStackTrace();
        }
    }
}

