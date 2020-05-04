package org.hadoop.praveen;

import java.io.DataInput;
import java.io.DataOutput;
import java.io.IOException;

import org.apache.hadoop.io.WritableComparable;
import org.apache.hadoop.io.WritableUtils;

public class MyWritable implements WritableComparable<MyWritable>{
	
	private String word;
	
	public MyWritable() {
		set("");
	}
	
	public MyWritable(String word) {
		set(word);
	}
	
	public void set(String word) {
		this.word = word;
	}
	
	public String getWord() {
		return this.word;
	}

	@Override
	public void readFields(DataInput arg0) throws IOException {
			this.word = WritableUtils.readString(arg0);
		
	}

	@Override
	public void write(DataOutput arg0) throws IOException {
		WritableUtils.writeString(arg0, this.word);
		
	}

	@Override
	public int compareTo(MyWritable o) {
		
		return this.word.compareTo(o.getWord());
	}
	
	@Override
	public String toString() {
		return this.word;
	}

}
