/**
 * Jyoti Salitra
 * UTA ID: ***********
 * Cloud Computing (CSE - 6331) - David Levine
 * Programming Assignment # 2
 * Date: 10/05/2014
 */
package edu.uta.cse;

import org.apache.hadoop.io.ArrayWritable;
import org.apache.hadoop.io.LongWritable;

/**
 * A custom ArrayWritable wrapper for LongWritable data type.
 * This is done to allow Reducer to iterate over ArrayWritable collection. 
 * 
 * References:
 * 1. http://hadoop.apache.org/docs/current/api/org/apache/hadoop/io/ArrayWritable.html
 * 	
 */
public class LongArrayWritable extends ArrayWritable {
	public LongArrayWritable() {
		super(LongWritable.class);
	}

	public LongArrayWritable(long[] longs) {
		super(LongWritable.class);
		LongWritable[] texts = new LongWritable[longs.length];
		for (int i = 0; i < longs.length; i++) {
			texts[i] = new LongWritable(longs[i]);
		}
		set(texts);
	}
}