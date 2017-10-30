
package org.apache.storm.starter.bolt;

import java.io.FileWriter;
import java.io.IOException;

import org.apache.storm.topology.BasicOutputCollector;
import org.apache.storm.topology.OutputFieldsDeclarer;
import org.apache.storm.topology.base.BaseBasicBolt;
import org.apache.storm.tuple.Tuple;



public class PopularWordsPrinterBolt extends BaseBasicBolt {
	

  /**
	 * 
	 */
	private static final long serialVersionUID = 1L;
	public static String _fileToWrite;
	public static int maxTweets;
	
	public PopularWordsPrinterBolt(String _fileToWrite){
		this._fileToWrite = _fileToWrite;
	}

@Override
  public void execute(Tuple tuple, BasicOutputCollector collector) {
    //System.out.println(tuple);
    writeStringToFile(_fileToWrite,(String)tuple.getValue(0));
  }

  @Override
  public void declareOutputFields(OutputFieldsDeclarer ofd) {
  }
  
  public void writeStringToFile(String filePath, String outputString) {
	  try{
		  // Write the given output string to file
		  FileWriter fw = new FileWriter(filePath, true);
		  fw.write(outputString);
		  fw.write("\n");
		  fw.close();
	  }catch (IOException e){
		  System.err.println("IOException"+e.getLocalizedMessage());
	  }
  }

}