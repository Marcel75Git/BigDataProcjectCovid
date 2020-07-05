package RatioDeCadaLina;

import org.apache.hadoop.io.FloatWritable;
import org.apache.hadoop.io.IntWritable;
import org.apache.hadoop.io.LongWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Mapper;

import java.io.IOException;

public class MapperLine extends Mapper<LongWritable, Text, Text , FloatWritable> {
    public void map(LongWritable key, Text value, Context context) throws IOException, InterruptedException {
        String values = value.toString();
        String line [] = values.split(",");
        try {
            String countriesAndTerritories = line[5];
            String casos = line[4];
            int a = Integer.parseInt(countriesAndTerritories);
            int b = Integer.parseInt(casos);
            if(a != 0 && b != 0){
                float result = (float) Integer.parseInt(countriesAndTerritories)/ Integer.parseInt(casos);
                context.write(new Text(values), new FloatWritable(result));
            }
        } catch (NumberFormatException nfe) {
            nfe.printStackTrace();
        }catch (ArithmeticException e) {
            e.printStackTrace();
        }
    }
}

