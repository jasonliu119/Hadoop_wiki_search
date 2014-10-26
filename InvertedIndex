

import java.io.IOException;
import java.util.Collections;
import java.util.Comparator;
import java.util.HashMap;
import java.util.Iterator;
import java.util.LinkedList;
import java.util.List;
import java.util.Map;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.io.LongWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Job;
import org.apache.hadoop.mapreduce.Mapper;
import org.apache.hadoop.mapreduce.Reducer;
import org.apache.hadoop.mapreduce.lib.input.FileInputFormat;
import org.apache.hadoop.mapreduce.lib.input.FileSplit;
import org.apache.hadoop.mapreduce.lib.output.FileOutputFormat;
import org.apache.hadoop.mapreduce.lib.output.TextOutputFormat;
import org.apache.hadoop.util.GenericOptionsParser;


public class MP4InvertedIndex {
	
	//Map: input key: offset in file, input value: a page
	//Map: output key: a word, output value: filename<>offset::frequency

	public static class InvertedIndexMapper extends
		Mapper<Object, Text, Text, Text> {
		
		String Delimiter_Title_Text;
		String Delimiter_Page_Page;
		private FileSplit split; 
		Text map_output_val = new Text();
		Text map_output_key = new Text();

		public void map(Object key, Text value, Context context)
				throws IOException, InterruptedException {
			
			
			split = (FileSplit) context.getInputSplit();
			String file_path = split.getPath().toString();
			String reverse = new StringBuilder(file_path).reverse().toString();
			int last_slash = reverse.indexOf("/");
			String file_name = file_path.substring(file_path.length()-last_slash);
			
			//Character ttsep=6;
			Delimiter_Title_Text="<__>";
			
			//Character ppsep=5;
			//Delimiter_Page_Page=ppsep.toString();
			
			String page = value.toString();
			String[] title_text = page.split(Delimiter_Title_Text);
			
			String title = title_text[0].toLowerCase().trim();
			
			if(title_text.length<2)
			{
				//System.out.println("Map: No ASCII:6 in this page!");
				return;
			}else if(title.equals(""))
			{
				return;
			}
			
			String file_offset = file_name+"<>"+((LongWritable)key).toString();
			
			//map_output_val.set(file_name+"<>"+((LongWritable)key).toString());
			
			String[] tokens = (title_text[0]+" "+title_text[1])
					.toLowerCase().split("[^a-z]");
			
			HashMap<String,Integer> comb = new HashMap<String,Integer>(); //***
			
			for(int i=0;i<tokens.length;i++)
			{
				//map_output_key.set(tokens[i]);
				//context.write(map_output_key, map_output_val);
				if(comb.containsKey(tokens[i]))
				{
					comb.put(tokens[i], comb.get(tokens[i])+1);
				}else
				{
					comb.put(tokens[i],1);
				}
			}
			
			for(String word:comb.keySet())
			{
				map_output_key.set(word);
				map_output_val.set(file_offset+":"+comb.get(word));
				
				context.write(map_output_key, map_output_val);
			}


	
		}
	}

	//reduce: input key: a word, input value: list<"filename<>offset::frequency">
	//reduce: output key: a word, output value: the sorted inverted index list of that word
	
	public static class InvertedIndexReducer 
      extends Reducer<Text,Text,Text,Text> {
		
		 public void reduce(Text word, Iterable<Text> titles, 
                 Context context
                 ) throws IOException, InterruptedException {
			 
			 if(word.toString().equals("")) return;
			 
			 HashMap<String,Integer> freq_map = new HashMap<String, Integer>();
			 
			 //int count=0;
			 
			 for(Text one_record: titles)
			 {
				 //count++;
				 String[] file_offset_freq = one_record.toString().split(":");
				 if(file_offset_freq.length<2) continue;
				 
				 
				 int freq = Integer.valueOf(file_offset_freq[1]);
				 
				 String title = file_offset_freq[0]; //use file_offset to represent the title
				 
				 if(title.equals("")) continue;
				 
				 if(freq_map.containsKey(title))
				 {
					 freq_map.put(title,freq_map.get(title)+freq);
				 }else
				 {
					 freq_map.put(title, freq);
				 }
			 }
			 
			 List<Map.Entry<String,Integer>> list = new LinkedList<Map.Entry<String,Integer>>();
			 list.addAll(freq_map.entrySet());
			 
			 Collections.sort(list, new Comparator<Map.Entry<String,Integer>>(){
				 public int compare(Map.Entry<String, Integer> o1, Map.Entry<String, Integer> o2)
				 {
					 if(o1.getValue()<o2.getValue())
					 {
						 return 1; //-1 from small to big; 1 from big to small
					 }else if(o1.getValue() == o2.getValue())
					 {
						 return 0;
					 }else
					 {
						 return -1;
					 }
				 }
			 });
			 
			 //sort the result
			 
			 StringBuilder reduce_output_val=new StringBuilder();
			 //Character seq = 6; 
			 //String seqq = seq.toString();
			 
			 for(Iterator<Map.Entry<String,Integer>> it = list.iterator();it.hasNext();)
			 {
				 Map.Entry<String, Integer> title_text = it.next();
				 
				 if(title_text.getKey().equals("")||title_text.getKey()==null) continue;
				 
				 reduce_output_val.append(title_text.getKey());
				 reduce_output_val.append(":");
				 reduce_output_val.append(String.valueOf(title_text.getValue()));
				 reduce_output_val.append("\t");

			 }
			 
			 Text title_freq_list = new Text();
			 title_freq_list.set(reduce_output_val.toString());
			 

			//context.write(word, new Text(String.valueOf(count)));
			context.write(word, title_freq_list);
			 
		 }
 }
	
	  public static void main(String[] args) throws Exception {
		    Configuration conf = new Configuration();
		    String[] otherArgs = new GenericOptionsParser(conf, args).getRemainingArgs();
		    if (otherArgs.length != 2) {
		      System.err.println("error: the number of the parameter is not 2");
		      System.exit(2);
		    }

		    Job job = new Job(conf, "CS423 MP4 Inverted Index");
		    job.setJarByClass(MP4InvertedIndex.class);
		    job.setMapperClass(InvertedIndexMapper.class);
		    //job.setCombinerClass(InvertedIndexReducer.class);
		    job.setReducerClass(InvertedIndexReducer.class);
		    
		    job.setOutputKeyClass(Text.class);
		    job.setOutputValueClass(Text.class);
    		job.setMapOutputKeyClass(Text.class);
    		job.setMapOutputValueClass(Text.class);
    		
    		job.setInputFormatClass(FileInputFormatB.class);
            job.setOutputFormatClass(TextOutputFormat.class);
            
            FileInputFormat.addInputPath(job, new Path(otherArgs[0]));
            FileOutputFormat.setOutputPath(job, new Path(otherArgs[1]));

		    System.exit(job.waitForCompletion(true) ? 0 : 1);
		  }
}
