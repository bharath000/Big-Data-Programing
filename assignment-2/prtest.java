package HadoopExamples;

import java.io.BufferedReader;
import java.io.File;
import java.io.FileReader;
import java.io.IOException;
import java.io.InputStreamReader;
import java.net.URI;
import java.util.HashMap;
import java.util.Map;
import java.util.StringTokenizer;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.io.DoubleWritable;
import org.apache.hadoop.io.IntWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Job;
import org.apache.hadoop.mapreduce.Mapper;
import org.apache.hadoop.mapreduce.Reducer;
import org.apache.hadoop.mapreduce.lib.input.FileInputFormat;
import org.apache.hadoop.mapreduce.lib.output.FileOutputFormat;

public class prtest {
    public static int a;
  public static class PowerIterationMapper
       extends Mapper<Object, Text, IntWritable, DoubleWritable>{
	  
	// The PageRank Values of all the nodes; the PageRank vector
	private Map<Integer, Double> vPRValues = new HashMap<Integer, Double>();
	// The variables for this node and its out-neighbor nodes
    private Integer nThisNodeIndex = 0;
    private IntWritable nNeighborNodeIndex = new IntWritable();
    private Double dThisNodePRValue = 0.0;
    private Integer nThisNodeOutDegree = 0;
    private DoubleWritable dThisNodePassingValue = new DoubleWritable();
    
    @Override
    protected void setup(
            Mapper<Object, Text, IntWritable, DoubleWritable>.Context context)
            throws IOException, InterruptedException {
        if (context.getCacheFiles() != null
                && context.getCacheFiles().length > 0) {
        	URI[] cacheFiles = context.getCacheFiles();
			String sCacheFileName = cacheFiles[0].toString();
			System.out.println(sCacheFileName);
        	FileSystem aFileSystem = FileSystem.get(context.getConfiguration());
        	Path aPath = new Path(sCacheFileName);
            BufferedReader br = new BufferedReader(new InputStreamReader(aFileSystem.open(aPath)));
        	String line;
        	System.out.println("PR Values");
        	// Read the PageRank values of all nodes in this iteration.
        	while ((line = br.readLine()) != null) {
        		// process the line.
        		Integer nOneNodeIndex = 0;
        		Double  dOneNodePRValue = 0.0;
        		StringTokenizer itr = new StringTokenizer(line);
        		nOneNodeIndex = Integer.parseInt(itr.nextToken());
        		dOneNodePRValue = Double.parseDouble(itr.nextToken());
        		vPRValues.put(nOneNodeIndex, dOneNodePRValue);
				System.out.println(nOneNodeIndex + " " + dOneNodePRValue);
			}
			// here a gives number of nodes in graph
            a = vPRValues.size();
        }
        super.setup(context);
    }
    
    public void map(Object key, Text value, Context context
                    ) throws IOException, InterruptedException {
		// You need to complete this function.
		// tokenizing the value i.e first line in adajacecey list by converting 
		//the value into string
		StringTokenizer itr = new StringTokenizer(value.toString());
		//  Degree of the node is finded by using the below line
		nThisNodeOutDegree = itr.countTokens() - 1;
		// getting first element in a line as index for hash map
		nThisNodeIndex = Integer.parseInt(itr.nextToken());
		// With the key got above use it for hashmap and acess the rank of the nodes
		dThisNodePRValue = vPRValues.get(nThisNodeIndex);
		// Loop for acessing remaining token in itr or elements in line
		// and each of these elements are conder as node j and also ouput of mapper
		// after getting the rank and transition propability from nthisNodeOutDegree
		// we compute and result as rank*transistion probability and output the result with
		// nNeighborNodeIndex as key
		while (itr.hasMoreTokens()) {
			nNeighborNodeIndex.set(Integer.parseInt(itr.nextToken()));
			Double resultValue = dThisNodePRValue * 1 / nThisNodeOutDegree;
			dThisNodePassingValue.set(resultValue);
			context.write(nNeighborNodeIndex, dThisNodePassingValue);
		}
    }
  }

  public static class PowerIterationReducer
       extends Reducer<IntWritable,DoubleWritable,IntWritable,DoubleWritable> {
    private DoubleWritable dNewPRValue = new DoubleWritable();

	

    public void reduce(IntWritable key, Iterable<DoubleWritable> values,
                       Context context
                       ) throws IOException, InterruptedException {
	  // You need to complete this function.
	  // Intialixing a variable for sum of computed rank*transistion probabilities
	  // and also the decay factor c = 0.85
            Double sum = 0.0;
			Double decay = 0.85;
			
	// A for loop is used to sum the values in a list that get after sorting and shuffling stage
			for (DoubleWritable val : values) {
				sum += val.get();
			}
			// the sum got above is used to compute new rank
			// writing this values and key as output
			sum = sum * decay + (1-decay) / a;
			dNewPRValue.set(sum);
			context.write(key, dNewPRValue);
    }
  }

  public static void main(String[] args) throws Exception {
	  // args[0] the initial PageRank values
	  String sInputPathForOneIteration = args[0];
	  // args[1] the input file containing the adjacency list of the graph
	  String sInputAdjacencyList = args[1];
	  // args[2] Output path
	  String sExpPath = args[2];
	  String sOutputFilenameForPreviousIteration = "";
	  // args[3] number of iterations
	  Integer nNumOfTotalIterations = Integer.parseInt(args[3]);
	  for (Integer nIdxOfIteration = 0; 
		 nIdxOfIteration < nNumOfTotalIterations; nIdxOfIteration++){
		  System.out.println("Iteration: " + nIdxOfIteration);
		  Configuration conf = new Configuration();
		  Job job = Job.getInstance(conf, "Power Iteration Method");
		  job.setJarByClass(prtest.class);
		  job.setMapperClass(PowerIterationMapper.class);
		  job.setReducerClass(PowerIterationReducer.class);
		  job.setOutputKeyClass(IntWritable.class);
		  job.setOutputValueClass(DoubleWritable.class);
		  if (nIdxOfIteration > 0) { // In the Iteration 2, 3, 4, ..., 
		  // the output of the previous iteration => the input of this iteration
			  sInputPathForOneIteration = sOutputFilenameForPreviousIteration;
		  }
		  job.addCacheFile(new Path(sInputPathForOneIteration).toUri());
		  FileInputFormat.addInputPath(job, new Path(sInputAdjacencyList));
		  // Change the output directory
		  String sOutputPath = sExpPath + "/Iteration" + 
				  				nIdxOfIteration.toString() + "/";
		  String sOutputFilename = sOutputPath + "part-r-00000";
		  sOutputFilenameForPreviousIteration = sOutputFilename;
		  FileOutputFormat.setOutputPath(job, new Path(sOutputPath));
		  if (nIdxOfIteration < nNumOfTotalIterations - 1) {
			  job.waitForCompletion(true);
		  } else {
			  System.exit(job.waitForCompletion(true) ? 0 : 1);
		  }
	  }
  }
}