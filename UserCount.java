import java.io.IOException;
import java.util.Random;
import java.util.Iterator;
import java.util.StringTokenizer;
import java.util.ArrayList;
import org.json.*;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.io.IntWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.io.WritableComparable;
import org.apache.hadoop.mapreduce.Job;
import org.apache.hadoop.mapreduce.Mapper;
import org.apache.hadoop.mapreduce.Reducer;
import org.apache.hadoop.mapreduce.lib.input.FileInputFormat;
import org.apache.hadoop.mapreduce.lib.output.FileOutputFormat;
import org.apache.hadoop.mapreduce.lib.input.SequenceFileInputFormat;
import org.apache.hadoop.util.GenericOptionsParser;
import org.apache.hadoop.mapreduce.lib.jobcontrol.ControlledJob;
import org.apache.hadoop.mapreduce.lib.jobcontrol.JobControl;
	 

	 

public class UserCount{

	public static class CountMapper extends Mapper<Object, Text, Text, Text>
	{	     
      // private final static IntWritable number = new IntWritable(1);
        private Text number=new Text();
         private Text word = new Text();
	    public void map(Object key, Text value, Context context) throws IOException, InterruptedException
		{  
		  try{
		 
		  String s = value.toString();
		  JSONObject js = new JSONObject(s);
		  String id = null;
		  String name=null;
        if ( js.has("user_id"))
        {
			
            id = js.optString("user_id");
			
            word.set(id);
             context.write(word,new Text("1"));
        }
        if(js.has("name"))
        {
            id = js.optString("_id");
// 			JSONObject js1=new JSONObject(id);
// 			if(js1.has("$numberLong"))
// 			{
// 				id=js1.optString("$numberLong");
// 			}
            name = js.optString("name");
            word.set(id);
			
            context.write(word,new Text("2 "+name));
        }
          
       
		   }catch(JSONException e)
		   {}
	        //implement here
			//使用IntWritable.set(int)和Text.set(String)来对IntWritable和Text的object赋值
			//可以参考http://wiki.apache.org/hadoop/WordCount来写程序
	    }
	}

	public static class CountReducer extends Reducer<Text,Text,Text,Text> 
	{
		public void reduce(Text key, Iterable<Text> values, Context context) throws IOException, InterruptedException 
		{
			//implement here
			int nameNum=0;
			int countNum=0;
			int sum = 0;

            ArrayList<String> a = new ArrayList<String>();
            ArrayList<String> b = new ArrayList<String>();
			 String name=null;
			 for (Text val : values) {
			  String record=val.toString();
			    char type=record.charAt(0);
			    if(type=='1')
			    {   
			       
			       sum += Integer.parseInt(val.toString().trim());;
			    }
               if(type=='2')
			    {
			       
			        name=record.substring(2);
			    }
            }
            if(name!=null&&String.valueOf(sum)!=null)
            {
			  a.add(String.valueOf(sum));
			  b.add(","+name);
			  context.write(new Text(a.get(0)),new Text(b.get(0)));
            }
                
            }
	}
	
	public static class SortMapper extends Mapper<Object, Text, IntWritable,Text>
	{
		 IntWritable a=new IntWritable();//存储词频
		private Text b=new Text();//存储文本
		public void map(Object key, Text value, Context context) throws IOException, InterruptedException 
		{
			//implement here
		String words[]=value.toString().split(",");

       if(words[0]!=null&&words.length==2){
         a.set(Integer.parseInt(words[0].trim()));
         b.set(words[1].trim());
         context.write(a, b);//map阶段输出，默认按key排序
       }
		}
	}

	public static class SortReducer extends Reducer<IntWritable,Text,IntWritable,Text> 
	{
		
		public void reduce(IntWritable key,Iterable<Text> values, Context context)throws IOException, InterruptedException 
		{
			//implement here
			for(Text t:values){
         context.write(key, t);//输出排好序后的K,V
       }
		}
	}
	
	private static class IntDecreasingComparator extends IntWritable.Comparator 
	{
		//注意默认的comparator是Increasing的，所以你完全没有必要明白下面两个method的意义
		//返回值为-1,0,1中的一个
// 		protected IntDecreasingComparator(){
// 		    super(IntWritable.class,true);
// 		}
// 		protected IntDecreasingComparator() 
// 		{ 
// 			super(IntWritable.class); 
// 		}
		public int compare(WritableComparable a, WritableComparable b) 
		{
			//implement here
		
			return  -super.compare(a, b);
		}

		public int compare(byte[] b1, int s1, int l1, byte[] b2, int s2, int l2) 
		{
			//implement here
		
			return  -super.compare(b1,s1,l1,b2, s2, l2);
		}
	}


	public static void main(String[] args) throws Exception 
	{
	
		
		Configuration conf = new Configuration();
		Job job = new Job(conf, "NameCount-count");
		job.setJarByClass(UserCount.class);
		job.setMapperClass(CountMapper.class);
		job.setReducerClass(CountReducer.class);
		job.setMapOutputKeyClass(Text.class);
        job.setMapOutputValueClass(Text.class);
		job.setOutputKeyClass(Text.class);
		job.setOutputValueClass(Text.class);
		
       
//         ControlledJob ctrljob=new  ControlledJob(conf);
// 		ctrljob.setJob(job);
	
		FileInputFormat.addInputPath(job, new Path("/input-user"));
		Path tempDir = new Path("temp");
		FileSystem fs=FileSystem.get(conf);
		if(fs.exists(tempDir)){
        fs.delete(tempDir, true);
       System.out.println("存在此输出路径，已删除！！！");
     }
		FileOutputFormat.setOutputPath(job, tempDir);
// 			System.exit(job.waitForCompletion(true) ? 0 : 1);
	
		//implement here
		//在这里你可以加入你的另一个job来进行排序
		//可以使用“job.waitForCompletion(true)“，该方法会开始job并等待job结束，返回值是true代表job成功，否则代表job失败
		//在SortJob中使用“sortJob.setSortComparatorClass(IntDecreasingComparator.class)”来把你的输出排序方式设置为你自己写的IntDecreasingComparator
	    if(job.waitForCompletion(true))
	    {
		Job job1 = new Job(conf, "sort");
		job1.setJarByClass(UserCount.class);
		job1.setMapperClass(SortMapper.class);
		job1.setReducerClass(SortReducer.class);
		job1.setSortComparatorClass(IntDecreasingComparator.class);
		job1.setMapOutputKeyClass(IntWritable.class);
        job1.setMapOutputValueClass(Text.class);
		job1.setOutputKeyClass(IntWritable.class);
		job1.setOutputValueClass(Text.class);
		
      
		
		FileInputFormat.addInputPath(job1,new Path("/user/ubuntu/temp/part-r-00000"));
		Path tempDir1 = new Path("temp1");
		FileSystem fs1=FileSystem.get(conf);
      if(fs1.exists(tempDir1)){
       fs1.delete(tempDir1, true);
       System.out.println("存在此输出路径，已删除！！！");
     }
		FileOutputFormat.setOutputPath(job1, tempDir1);
	    	System.exit(job1.waitForCompletion(true) ? 0 : 1);
	        
	    }

}
}