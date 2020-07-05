package CasMort;

import org.apache.hadoop.io.IntWritable;
import org.apache.hadoop.io.LongWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Mapper;

import java.io.IOException;

public class MapperMort extends Mapper<LongWritable, Text, Text, IntWritable> {
    // le nombre d occurence qu apparait le continentExp
    IntWritable intWritable = new IntWritable(0);
    public void map(LongWritable key, Text value, Context context) throws IOException, InterruptedException {
        String values = value.toString();
        String line [] = values.split(",");
        String countriesAndTerritories = line[6];
        //String deaths = line[5];
        try {
            int deaths = Integer.parseInt(line[5]);
            context.write(new Text(countriesAndTerritories), new IntWritable(deaths));
        } catch (NumberFormatException nfe) {
            // nfe.printStackTrace();
        }



    }
}

