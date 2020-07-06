package RationConPais;

import org.apache.hadoop.io.DoubleWritable;
import org.apache.hadoop.io.FloatWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Reducer;

import java.io.IOException;

public class ReducerRatioPais extends Reducer<Text, DoubleWritable, Text, DoubleWritable> {
    public void reduce(Text key, Iterable<DoubleWritable> value, Context context) throws IOException, InterruptedException {
        double total = 0;
        double contador = 0;
        for(DoubleWritable values : value){
            total += values.get();
            contador++;
            System.out.println("valor"+contador);
            // la media simple
            // el numero de values de cada pais
        }
        context.write(key, new DoubleWritable(total/contador));
    }
}
