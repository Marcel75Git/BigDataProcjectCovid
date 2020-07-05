package TopNCas;

import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Reducer;

import java.io.IOException;
import java.util.Map;
import java.util.TreeMap;

public  class TopNCasReducer extends Reducer<Text, Text, Text, Text> {
    public static final int N = 10;
    private TreeMap<Integer, String> treeMaps = new TreeMap<>();

    public void reduce(Text key, Iterable<Text> value , Context context){
        for(Text values : value){
            treeMaps.put(Integer.parseInt(key.toString()), values.toString());
            if(treeMaps.size() > N){
                treeMaps.remove(treeMaps.firstKey());
            }
        }
    }
    @Override
    public void cleanup(Context context) throws IOException, InterruptedException {

        for(Map.Entry<Integer, String> entry : treeMaps.descendingMap().entrySet()){
            context.write(new Text(String.valueOf(entry.getKey())), new Text(String.valueOf(entry.getValue())));
        }
    }
}