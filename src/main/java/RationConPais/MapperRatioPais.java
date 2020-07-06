package RationConPais;

import org.apache.hadoop.io.DoubleWritable;
import org.apache.hadoop.io.FloatWritable;
import org.apache.hadoop.io.LongWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Mapper;

import java.io.IOException;

    public class MapperRatioPais extends Mapper<LongWritable, Text, Text , DoubleWritable> {
    public void map(LongWritable key, Text value, Context context) throws IOException, InterruptedException {
        try {
            String values = value.toString();
            String line[] = values.split(",");
            String country = line[6];
            String countriesAndTerritories = line[5];
            String casos = line[4];
            //float result = (float) Integer.parseInt(countriesAndTerritories) / Integer.parseInt(casos);
            double uno = Double.parseDouble(countriesAndTerritories);
            double dos = Double.parseDouble(casos);
            double r = uno/dos;
              if(uno >= 0 && dos > 0){
                  context.write(new Text(country), new DoubleWritable(r));
              }
        }catch (NumberFormatException e){
            e.printStackTrace();
        }catch (ArithmeticException e) {
            e.printStackTrace();
        }
    }
}
