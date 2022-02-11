package transformer.mr.au;

import java.io.IOException;

import org.apache.hadoop.mapreduce.Reducer;

import transformer.model.dim.StatsUserDimension;
import transformer.model.value.map.TimeOutputValue;

/**
 * combine类
 * 
 * @author root
 *
 */
public class ActiveUserCombine extends Reducer<StatsUserDimension, TimeOutputValue, StatsUserDimension, TimeOutputValue> {
    @Override
    protected void reduce(StatsUserDimension key, Iterable<TimeOutputValue> values, Context context) throws IOException, InterruptedException {
        for (TimeOutputValue tov : values) {
            context.write(key, tov);
        }
    }
}
