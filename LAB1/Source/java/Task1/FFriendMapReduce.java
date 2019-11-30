package dhabbah.com.mapreduce;


import java.io.IOException; // To make sure the input file is existing, and the output is a new one. Otherwise, it will give an error.
import java.util.Arrays;
import java.util.Iterator;
import java.util.LinkedList;
import java.util.List;
import java.util.StringTokenizer; // to split an input into tokens
 


// These two are provided by hadoop to handle a massive data; data type.
import org.apache.hadoop.io.LongWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.mapred.FileInputFormat;
import org.apache.hadoop.mapred.FileOutputFormat;
import org.apache.hadoop.mapred.JobClient;
import org.apache.hadoop.mapred.JobConf;
import org.apache.hadoop.mapred.MapReduceBase; //SuperClass
import org.apache.hadoop.mapred.Mapper;
import org.apache.hadoop.mapred.OutputCollector; // To transfer the output of the mapper into the input of the reducer.
import org.apache.hadoop.mapred.Reducer;
import org.apache.hadoop.mapred.Reporter; //Display the issues with the mapper.

public class FFriendMapReduce {


    public static class MapFFriendClass extends MapReduceBase implements Mapper<LongWritable, Text, Text, Text>{
    	
            public void map(LongWritable key, Text value, OutputCollector<Text, Text> output, Reporter reporter) throws IOException{
            	// This class allows us to break the data into tokens
                    StringTokenizer t = new StringTokenizer(value.toString(), System.lineSeparator()); 
                    while(t.hasMoreTokens()){
                    		String l = t.nextToken();
                    		//This is to split the data from the file by ->
                            String[] lineAfterPerson = l.split(" -> "); 
                         // This line is to divide the friends based on the space between them
                            String[] friendList = lineAfterPerson[1].split(" "); 
                            String[] person = new String[2];
                            //The below for loop code is about gathering the perosn's friends
                            for(int f = 0; f < friendList.length; f++){
                            		person[0] = friendList[f];
                            		person[1] = lineAfterPerson[0];
                                    Arrays.sort(person);
                                    output.collect(new Text("(" + person[0] + " " + person[1] + ")"),  new Text(lineAfterPerson[1]));//Persons           
                            }
                    }
            }
    }

    public static class ReduceFFriendClass extends MapReduceBase implements Reducer<Text, Text, Text, Text>{
    	
            public void reduce(Text key, Iterator<Text> values, OutputCollector<Text, Text> output, Reporter reporter) throws IOException{
            	// here where the Suffleing and Sorting occur.
                    Text[] data = new Text[2];
                    int indexValue = 0;
                    while(values.hasNext()){data[indexValue++] = new Text(values.next());}
                    String[] fl1 = data[0].toString().split(" ");
                    String[] fl2 = data[1].toString().split(" ");
                    List<String> fcommonList = new LinkedList<String>();
                    for(String friend1 : fl1){
                            for(String friend2 : fl2){
                                    if(friend1.equals(friend2)){
                                    	fcommonList.add(friend1);
                                    }
                            }
                    }
                    
                    //This is for adding the friends
                    StringBuffer datastore = new StringBuffer();
                    for(int FL = 0; FL < fcommonList.size(); FL++){
                    		datastore.append(fcommonList.get(FL));
//                            This if statement is to add space between friends
                            if(FL != fcommonList.size() - 1)
                            		datastore.append(" ");
                    }
                    //This is to print the friends;key is the persons and datastore.tostring() is the friends
                    output.collect(key, new Text("->" + "(" + datastore.toString() + ")"));
            }
    }

    static String MapReduceInfo = "\nIn order to use the MapReduce program, follow these steps:\n"
  	         + "[dhabbah.com.mapreduce]: is the name of the package.\n"
  	         + "[FFriendMapReduce]: is the name of the class.\n"
  	         + "[in_dir]: is the path to the input file.\n"
  	         + "[out_dir]: is the path for the output files.\n"
  	         + "for instance: hadoop jar test.jar dhabbah.com.mapreduce.MatrixMultiply /user/cloudera/input /user/cloudera/output ";
    
    public static void main(String[] args) throws Exception{
    	//Setting up the arguments to two so that we can have path for input file
    	//and another once for the output file
    	if (args.length != 2) {
            System.err.println(MapReduceInfo);
            System.exit(2);
        }

            JobConf MainFConfig = new JobConf(FFriendMapReduce.class);

         // These below  lines are for setting the classes   
            MainFConfig.setMapperClass(MapFFriendClass.class);
            MainFConfig.setReducerClass(ReduceFFriendClass.class);
            
        // These are for setting the output key and output value classes
            MainFConfig.setMapOutputKeyClass(Text.class);
            MainFConfig.setMapOutputValueClass(Text.class);

        // These two are for setting the format of the input and output 
            MainFConfig.setOutputKeyClass(Text.class);
            MainFConfig.setOutputValueClass(Text.class);

        //This is for having the path for the input file
            FileInputFormat.setInputPaths(MainFConfig, new Path(args[0])); 
        //This is for having the path for the output file
            FileOutputFormat.setOutputPath(MainFConfig, new Path(args[1])); 

            JobClient.runJob(MainFConfig);
    }
}
