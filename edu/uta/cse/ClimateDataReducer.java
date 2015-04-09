/**
 * Jyoti Salitra
 * UTA ID: ***********
 * Cloud Computing (CSE - 6331) - David Levine
 * Programming Assignment # 2
 * Date: 10/05/2014
 */
package edu.uta.cse;

import java.io.IOException;

import org.apache.hadoop.io.LongWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.io.Writable;
import org.apache.hadoop.mapreduce.Reducer;

import edu.uta.cse.LongArrayWritable;

/**
 * Reducer implementation
 */
public class ClimateDataReducer extends Reducer<Text, LongArrayWritable, Text, Text> {

	public void reduce(Text key, Iterable<LongArrayWritable> values, Context context) throws IOException, InterruptedException {
		
		// an array that will hold sum of the precipitation, temperature, and wind speed values for the duration of the interval
		long sums[] = new long[3];
		long count = 0;
		Text output = new Text();

		// loop over mapped collection
		for (LongArrayWritable tmp : values) {
			int i = 0;
			for (Writable element : tmp.get()) {
				LongWritable longWritable = (LongWritable) element;
				sums[i] += longWritable.get();
				i++;
			}
			count++;
		}
		
		// calculate average value for the duratio
		double avgPrcp = (double) sums[0] / count;// prep
		double avgTemp = (double) sums[1] / count;// temp
		double avgAwnd = (double) sums[2] / count;// awnd

		//set output value
		output.set("AVG PRCP=" + String.format("%.2f", avgPrcp) + ", AVG TEMP="
				+ String.format("%.2f", avgTemp) + ", AVG WIND="
				+ String.format("%.2f", avgAwnd));
		context.write(key, output);
	}
}
