package ComputeGraphDensity;

import java.io.IOException;
import java.util.HashSet;
import java.util.Iterator;
import java.util.LinkedList;
import java.util.Queue;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.io.IntWritable;
import org.apache.hadoop.io.LongWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Job;
import org.apache.hadoop.mapreduce.Mapper;
import org.apache.hadoop.mapreduce.Reducer;
import org.apache.hadoop.mapreduce.lib.input.FileInputFormat;
import org.apache.hadoop.mapreduce.lib.input.TextInputFormat;
import org.apache.hadoop.mapreduce.lib.output.FileOutputFormat;
import org.apache.hadoop.mapreduce.lib.output.TextOutputFormat;

public class ComputeGraphDensity
{
	public static void main(String[] args)
	{
		if(args.length < 2) return;
		
		String filenameIN = args[0];
		String directoryOUT = args[1];
		
		startJob(filenameIN, directoryOUT);
	}
	
	private static void startJob(String filename_path, String outdir_path)
	{
		Configuration graphdensityConf = new Configuration();
		
		try
		{
			Job graphdensityjob = new Job(graphdensityConf,"Compute graph density");
			graphdensityjob.setJarByClass(ComputeGraphDensity.class);
			graphdensityjob.setJobName("Compute graph density");
			
			// Set output key and value class
			graphdensityjob.setOutputKeyClass(Text.class);
			graphdensityjob.setOutputValueClass(Text.class);
			
			// Set Map class
			graphdensityjob.setMapperClass(GraphDensityMapper.class);
			// Set Combiner class
			//graphdensityjob.setCombinerClass(GraphDensityReducer.class);
			// Set Reducer class
			graphdensityjob.setReducerClass(GraphDensityReducer.class);
			
			// Set Map output key and value classes
			graphdensityjob.setMapOutputKeyClass(IntWritable.class);
			graphdensityjob.setMapOutputValueClass(Text.class);
			
			// Set input and output format classes
			graphdensityjob.setInputFormatClass(TextInputFormat.class);
			graphdensityjob.setOutputFormatClass(TextOutputFormat.class);
			
			// Set input and output path
			FileInputFormat.addInputPath(graphdensityjob, new Path(filename_path));
			FileOutputFormat.setOutputPath(graphdensityjob, new Path(outdir_path));
			
			// Start MapReduce job
			graphdensityjob.waitForCompletion(true);
		}
		catch (IOException e) { e.printStackTrace(); }
		catch (ClassNotFoundException e) { e.printStackTrace(); }
		catch (InterruptedException e) { e.printStackTrace(); }
	}
	
	public static class GraphDensityMapper
		extends Mapper<LongWritable, Text, IntWritable, Text>	//<K_in, V_in, K_out, V_out>
	{
		public void map(LongWritable key, Text edge/*arco "[src]<tab>[dst]"*/, Context context) throws IOException
		{
			if(edge.toString().isEmpty()) return;
			
			//coppia <chiave, valore> come <1, arco>
			try
			{
				context.write(new IntWritable(1), edge);
			}
			catch (InterruptedException e) { e.printStackTrace(); }
		}
	}

	public static class GraphDensityReducer
		extends Reducer<IntWritable, Text, Text, Text>			//<K_in, V_in, K_out, V_out>
	{
		public void reduce(IntWritable key, Iterable<Text> edges, Context context) throws IOException
		{
			HashSet<Integer> hashSet = new HashSet<>();
			HashSet<String> edgeSet = new HashSet<>();
			
			Iterator iter = edges.iterator();
			Queue<String> queue = new LinkedList<>();
			String hashEdge;
			//while there are edges left
			while(iter.hasNext())
			{
				Text line = (Text) iter.next();
				String edge[] = line.toString().split("\\t");
				int src = Integer.valueOf(edge[0]);		//first node of this edge
				int dst = Integer.valueOf(edge[1]);		//second node of this edge
				
				if(src < dst) hashEdge = src + " - " + dst;
				else hashEdge = dst + " - " + src;
				
				if(edgeSet.contains(hashEdge)) continue;	//in case of directed graph, skip the repeating edges
				edgeSet.add(hashEdge);
				
				queue.add(line.toString());
				hashSet.add(src);
				hashSet.add(dst);
			}
			
			//computing density of the whole graph as |E|/|V|
			int edgeCount = queue.size();
			int nodeCount = hashSet.size();
			double graph_density = (double)edgeCount/(double)nodeCount;
			
			Text K_out = new Text(), V_out = new Text();
			while(!queue.isEmpty())
			{
				/* the output file will be as follows:
				 * ###############################################
				 * #     [src]<tab>[dst]<tab>[graph_density]     #
				 * # ... [src]<tab>[dst]<tab>[graph_density] ... #
				 * # ............................................#
				 * ###############################################
				 */
				K_out.set(queue.poll());
				V_out.set(String.valueOf(graph_density));
				try
				{
					context.write(K_out, V_out);
				}
				catch (InterruptedException e) { e.printStackTrace(); }
			}
		}
	}
}
