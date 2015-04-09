/**
 * Jyoti Salitra
 * UTA ID: ***********
 * Cloud Computing (CSE - 6331) - David Levine
 * Programming Assignment # 2
 * Date: 10/05/2014
 */
package edu.uta.cse;

import java.io.IOException;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.io.ArrayWritable;
import org.apache.hadoop.io.LongWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Mapper;

import edu.uta.cse.LongArrayWritable;

/**
 * Mapper implementation
 */
public class ClimateDataMapper extends Mapper<LongWritable, Text, Text, ArrayWritable> {
	// NOAA station
	private Text station = new Text();

	public void map(LongWritable key, Text value, Context context) throws IOException, InterruptedException {
		String line = value.toString();
		// split csv line
		String csvs[] = line.split(",");
		
		//initialize precipitation, temperature, and average wind speed to zero
		long prcp = 0;
		long temp = 0;
		long awnd = 0;

		// get precipitation, temperature, and wind speed for each line
		try {
			prcp = Long.parseLong(csvs[3]);
			if (prcp < 0) {
				prcp = 0;
			}
		} catch (Exception e) {
		}

		try {
			temp = Long.parseLong(csvs[4]);
			if (temp < 0) {
				temp = 0;
			}
		} catch (Exception e) {
		}

		try {
			awnd = Long.parseLong(csvs[6]);
			if (awnd < 0) {
				awnd = 0;
			}
		} catch (Exception e) {
		}

		// get intervalKey based on the jobType and the date value
		Text intervalKey = new Text();

		//get configuration object set by the ClimateDataRunner class
		Configuration conf = context.getConfiguration();
		String jobType = conf.get("jobType");

		// csvs[2] = 20141005, the date format
		if (jobType.equals("MONTH")) {
			//2014
			intervalKey.set(csvs[2].substring(0, 6));
			
		} else if (jobType.equals("YEAR")) {
			//2014
			intervalKey.set(csvs[2].substring(0, 4));
			
		} else if (jobType.equals("SEASON")) {
			//2014-SPRING, 2014-FALL etc.
			intervalKey.set(getYearSeason(csvs[2]));
		}

		// create key using stations and interval
		station.set(csvs[1] + "-" + intervalKey);

		// create an ArrayWritable for the station key with precipitation, temperature, and wind speed value
		long[] data = new long[3];
		data[0] = prcp;
		data[1] = temp;
		data[2] = awnd;
		LongArrayWritable ptaData = new LongArrayWritable(data);
		context.write(station, ptaData);
	}

	/**
	 * Returns YEAR-SEASON string 
	 */
	private String getYearSeason(String date) {
		String result = date.substring(0, 4);
		String month = date.substring(4, 6);
		if ("01|02|03".indexOf(month) > -1) {
			result += "-SPRING";
		} else if ("04|05|06".indexOf(month) > -1) {
			result += "-SUMMER";
		} else if ("07|08|09".indexOf(month) > -1) {
			result += "-FALL";
		} else if ("10|11|12".indexOf(month) > -1) {
			result += "-WINTER";
		}
		return result;
	}
}
