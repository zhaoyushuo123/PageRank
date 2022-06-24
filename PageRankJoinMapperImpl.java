package DSPPCode.mapreduce.common_pagerank.impl;

import DSPPCode.mapreduce.common_pagerank.question.PageRankJoinMapper;
import DSPPCode.mapreduce.common_pagerank.question.utils.ReduceJoinWritable;
import DSPPCode.mapreduce.common_pagerank.question.utils.ReducePageRankWritable;
import org.apache.hadoop.io.LongWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Mapper;
import org.apache.hadoop.mapreduce.Mapper.Context;
import org.apache.logging.log4j.core.pattern.AbstractStyleNameConverter.Red;
import java.io.IOException;
import java.util.regex.Matcher;
import java.util.regex.Pattern;

public class PageRankJoinMapperImpl extends PageRankJoinMapper {
  @Override
  public void map(LongWritable key, Text value,
      Mapper<LongWritable, Text, Text, ReduceJoinWritable>.Context context)
      throws IOException, InterruptedException {
    String outline = value.toString();
    String[] outlist = outline.split(" ", 2);
    ReduceJoinWritable isMO = new ReduceJoinWritable();
    isMO.setData(outlist[1]);
    Pattern pattern = Pattern.compile("[0-9]*\\.?[0-9]+");//正则
    Matcher isNum = pattern.matcher(outlist[1]);
    if(isNum.matches())
    {isMO.setTag("2");}
    else {isMO.setTag("1");}
    context.write(new Text(outlist[0]), isMO);
  }
}
