package cn.edu.ecnu.mapreduce.java.pagerank;

import org.apache.hadoop.io.Writable;

import java.io.DataInput;
import java.io.DataOutput;
import java.io.IOException;

public class ReducePageRankWritable implements Writable {

	// 保存网页连接关系或网页排名元组
	private String data;
	// 标识当前对象保存的元组来自网页连接关系还是网页排名
	private String tag;

	// 用于标识的常量
	public static final String PAGE_INFO = "1";
	public static final String PR_L = "2";

	@Override
	public void write(DataOutput dataOutput) throws IOException {
		dataOutput.writeUTF(tag);
		dataOutput.writeUTF(data);
	}

	@Override
	public void readFields(DataInput dataInput) throws IOException {
		tag = dataInput.readUTF();
		data = dataInput.readUTF();
	}

	// get和set方法
	public String getData() {
		return data;
	}

	public void setData(String data) {
		this.data = data;
	}

	public String getTag() {
		return tag;
	}

	public void setTag(String tag) {
		this.tag = tag;
	}
}
