package DSPPCode.mapreduce.common_pagerank.impl;

import DSPPCode.mapreduce.common_pagerank.question.PageRankReducer;
import DSPPCode.mapreduce.common_pagerank.question.PageRankRunner;
import DSPPCode.mapreduce.common_pagerank.question.utils.ReducePageRankWritable;
import org.apache.hadoop.io.NullWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Counter;
import org.apache.hadoop.mapreduce.Reducer;
import org.apache.hadoop.mapreduce.Reducer.Context;
import java.io.IOException;

public class PageRankReducerImpl extends PageRankReducer {

  private static double D=0.85;
  public static final double DELTA = PageRankRunner.DELTA;
  @Override
  public void reduce(Text key, Iterable<ReducePageRankWritable> values,
      Reducer<Text, ReducePageRankWritable, Text, NullWritable>.Context context)
      throws IOException, InterruptedException {
    double pr;
    double last_pr;
    String out_page = "";
    pr=last_pr=0.0;
    for(ReducePageRankWritable value : values){
      if(value.getTag().equals(ReducePageRankWritable.PR_L)){
        try {
          pr += Double.parseDouble(value.getData());
        } catch (NumberFormatException e){
          e.printStackTrace();
        }
      }
      else{
        String[] outlist = value.getData().split(" ", 3);
        out_page += outlist[2];
        last_pr = Double.parseDouble(outlist[1]);
      }
    }
    int totalPage = context.getConfiguration().getInt("1", 0);
    pr = D * pr + (1 - D) / totalPage;
    if(Math.abs(pr - last_pr) < PageRankRunner.DELTA){
      Counter counter =context.getCounter(PageRankRunner.GROUP_NAME, PageRankRunner.COUNTER_NAME);
      counter.increment(1L);
    }
    context.write(new Text(key.toString() + " " + String.valueOf(pr) + " " + out_page), NullWritable.get());
  }
}
