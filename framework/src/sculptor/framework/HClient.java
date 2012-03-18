/**
 * Licensed to the Apache Software Foundation (ASF) under one
 * or more contributor license agreements.  See the NOTICE file
 * distributed with this work for additional information
 * regarding copyright ownership.  The ASF licenses this file
 * to you under the Apache License, Version 2.0 (the
 * "License"); you may not use this file except in compliance
 * with the License.  You may obtain a copy of the License at
 *
 *     http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */
package sculptor.framework;

import java.io.Closeable;
import java.io.IOException;
import java.io.ObjectInputStream;
import java.io.ObjectOutputStream;
import java.util.ArrayList;
import java.util.Calendar;
import java.util.List;
import java.util.Map;

import org.apache.commons.lang.math.RandomUtils;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.hbase.HBaseConfiguration;
import org.apache.hadoop.hbase.client.Delete;
import org.apache.hadoop.hbase.client.Get;
import org.apache.hadoop.hbase.client.HTable;
import org.apache.hadoop.hbase.client.Put;
import org.apache.hadoop.hbase.client.Result;
import org.apache.hadoop.hbase.client.Scan;
import org.apache.hadoop.hbase.filter.CompareFilter.CompareOp;
import org.apache.hadoop.hbase.io.ImmutableBytesWritable;
import org.apache.hadoop.hbase.mapreduce.TableMapReduceUtil;
import org.apache.hadoop.hbase.mapreduce.TableMapper;
import org.apache.hadoop.hbase.util.Bytes;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Counter;
import org.apache.hadoop.mapreduce.Counters;
import org.apache.hadoop.mapreduce.Job;
import org.apache.hadoop.mapreduce.Reducer;
import org.apache.hadoop.mapreduce.lib.output.FileOutputFormat;
import org.apache.hadoop.mapreduce.lib.output.NullOutputFormat;
import org.apache.hadoop.mapreduce.lib.output.TextOutputFormat;

/**
 * HBase client
 * Note: this class is NOT thread safe
 *
 * @param <D> data store
 */
public abstract class HClient<D> implements Closeable {
	
	protected String tableName;
	protected HTable table;
	
	protected boolean closed = false;
	
	private static Configuration defaultConfig = HBaseConfiguration.create();
	
	private Configuration thatConfig;

	final byte[] DATA_FAMILY = Bytes.toBytes("data");
	
	// some key for MR
	public static final String SCAN_MR_OUTPUT = "sculptor.scanMR.output";
	public static final String SCAN_MR_OFFSET = "sculptor.scanMR.offset";
	public static final String SCAN_MR_LIMIT = "sculptor.scanMR.limit";
	public static final String SCAN_MR_CLIENT = "sculptor.scanMR.client";
	
	protected String MROutput;
	static final Text EMPTY_TEXT = new Text("");
	
    /** home of sculptor on DFS */
    public static final String DFS_HOME = "/tmp/sculptor";
    
    /** default count return to client via Mapreduce scan */
    public static final int MR_DEFAULT_RETURN_COUNT = 100;

	
	/**
	 * Constructor
	 * 
	 * @param tableName the table name
	 * @param that specific configuration
	 */
	public HClient(String tableName, Configuration that) {
		this.tableName = tableName;
        try {
        	
        	if (that != null) {
        		this.thatConfig = that;
        	}

        	this.table = new HTable(getConfig(), this.tableName);
        	
        } catch (IOException e) {
            throw new RuntimeException("can not connect to HBase for table: " + this.tableName, e);
        }

	}
		
	/**
	 * Constructor
	 * 
	 * @param tableName the table name
	 */
	public HClient(String tableName) {
		this(tableName, null);
	}
		
	/**
	 * この接続の設定情報を取得
	 * 
	 * @return 設定情報
	 */
	public Configuration getConfig() {
		return thatConfig == null ? defaultConfig : thatConfig;
	}
	
	public void close() {
		if (table != null) {
			try {
				table.close();
			} catch (IOException e) {
				// ingore
			}
		}
		closed = true;
	}
	
	public boolean isClosed() {
		return closed;
	}

	public String getTableName() {
		return tableName;
	}
	
	public HTable getTable() {
		return table;
	}
	
	/**
	 * HBaseの一行を取得。1 familyのみ取得。
	 * 
	 * @param rowkey row key
	 * @param family family
	 * @return 一行
	 * @throws IOException
	 */
	public D get(byte[] rowkey, byte[] family) throws IOException {
		if (rowkey == null || rowkey.length == 0) {
			return null;
		}
		Get g = new Get(rowkey);
		g.addFamily(family);
		g.setMaxVersions();
		Result r = table.get(g);
		return toDataStore(r);
	}
	
	/**
	 * Mapreduce jobの出力base path
	 * 
	 * @return output bash path
	 */
	public String generateMROutput(String prefix) {
		String username = System.getProperty("user.name");
        int random = RandomUtils.nextInt(9999);
        String path = String.format("/%1$s/%2$tY%2$tm%2$td%2$tH%2$tM_%3$s_%4$s_%5$d", username, Calendar.getInstance(), getTableName(), prefix, random);
		return DFS_HOME + path;
	}
	
	/**
	 * Get the temporary path on DFS
	 * 
	 * @param outputName the last part of output path
	 * @return temporary path on DFS
	 */
	public static String getTemporaryPath(String outputName) {
		return String.format("%s/tmp/%s", DFS_HOME, outputName);
	}
	
	/**
	 * Get the path of serialized object on DFS
	 * 
	 * @param outputName the last part of output path
	 * @return the path of serialized object on DFS
	 */
	public static String getSerializePath(String outputName) {
		return getTemporaryPath(outputName) + "/serialized";
	}
	
	/**
	 * HCompareOp to CompareOp
	 * 
	 * @param hop the HCompareOp
	 * @return CompareOp
	 */
	public static CompareOp toCompareOp(HCompareOp hop) {
		CompareOp op;
		if (hop == HCompareOp.LESS) {
			op = CompareOp.LESS;
		} else if (hop == HCompareOp.LESS_OR_EQUAL) {
			op = CompareOp.LESS_OR_EQUAL;
		} else if (hop == HCompareOp.EQUAL) {
			op = CompareOp.EQUAL;
		} else if (hop == HCompareOp.NOT_EQUAL) {
			op = CompareOp.NOT_EQUAL;
		} else if (hop == HCompareOp.GREATER_OR_EQUAL) {
			op = CompareOp.GREATER_OR_EQUAL;
		} else if (hop == HCompareOp.GREATER) {
			op = CompareOp.GREATER;
		} else {
			throw new RuntimeException("not implemented");
		}
		return op;
	}
	
	/**
	 * "data" family の一行を取得。
	 * 
	 * @param rowkey row key
	 * @return 一行
	 * @throws IOException
	 */
	public D get(byte[] rowkey) throws IOException {
		return get(rowkey, DATA_FAMILY);
	}

	/**
	 * get one row via D
	 * 
	 * @param d data store entity
	 * @return the row
	 */
	public abstract D get(D d) throws IOException;

	/**
	 * Get the native scan.
	 * This method maybe slow. Specify the row key items to gain speed.
	 * 
	 * @param kv data store entity
	 * @param ops 項目ごとの比較条件
	 * @return the native scan
	 */
	public abstract Scan getRawScan(D kv, Map<String, HCompareOp> ops);
	
	/**
	 * Scan the table. <br>This method maybe slow. Specify the row
	 * key items to gain speed.
	 * 
	 * @param kv entity
	 * @param ops filter conditions
	 * @return the scanner
	 * @throws IOException
	 */
	public abstract HScanner<D> scan(D kv, Map<String, HCompareOp> ops) throws IOException;
	
	/**
	 * Scan using mapreduce.
	 * 
	 * @param kv entity
	 * @param ops filter conditions
	 * @return Result of the scan.<br>Also write to HDFS. Use {@link #getMROutputPath()} to get the output path.
	 * @throws Exception
	 */
	public List<D> scanMR(D kv, Map<String, HCompareOp> ops) throws Exception {
		return scanMR(kv, ops, 0, -1);
	}
	
	/**
	 * Scan using mapreduce.
	 * 
	 * @param kv entity
	 * @param ops filter conditions
	 * @param offset result offset
	 * @param limit result limit
	 * @return Result of the scan.<br>Also write to HDFS. Use {@link #getMROutputPath()} to get the output path.
	 * @throws Exception
	 */
	public List<D> scanMR(D kv, Map<String, HCompareOp> ops, long offset, long limit) throws Exception {
        List<D> result = new ArrayList<D>(0);
		if (limit == 0) {
			return result; 
		}
		
		String jobName = "Scan " + tableName;
        Job job = new Job(this.getConfig(), jobName);
        job.setJarByClass(HClient.class);

        // scan setting
        Scan scan = getRawScan(kv, ops);
        scan.setCacheBlocks(false);

        // initialize the mapper
        TableMapReduceUtil.initTableMapperJob(getTableName(), scan, ScanMapper.class, ImmutableBytesWritable.class, Result.class, job, false);
        
        // the reducer
        job.setReducerClass(ScanReducer.class);
        // must do global sort by the row key
        job.setNumReduceTasks(1);
        job.setOutputFormatClass(TextOutputFormat.class);

        // set output path
        MROutput = generateMROutput("scan");
        Configuration conf = job.getConfiguration();
        conf.set(SCAN_MR_OUTPUT, MROutput);
        Path output = new Path(MROutput);
        FileSystem fs = FileSystem.get(conf);
        if (fs.exists(output)) {
        	fs.delete(output, true);
        }
        FileOutputFormat.setOutputPath(job, output);
        
        // set offset and limit
        conf.set(SCAN_MR_OFFSET, String.valueOf(offset));
        conf.set(SCAN_MR_LIMIT, String.valueOf(limit));
        
        // for reducer
        conf.set(SCAN_MR_CLIENT, Sculptor.descriptors.get(tableName).clientClassName);

        boolean jobResult = job.waitForCompletion(true);
        
		if (jobResult) {
			// deserialize some results from HDFS
			String outputName = MROutput.substring(MROutput.lastIndexOf("/"));
			String serializePath = HClient.getSerializePath(outputName); 

			ObjectInputStream ois = new ObjectInputStream(fs.open(new Path(serializePath)));
			result = (List<D>) ois.readObject();
			
			// delete the temporary file
			String tempPath = HClient.getTemporaryPath(outputName);
			fs.delete(new Path(tempPath), true);
		}
		return result;

	}
	
	/**
	 * Row count using mapreduce.
	 * 
	 * @param kv entity
	 * @param ops filter conditions
	 * @return row count if job completed successfully, -1 if failed.
	 * @throws Exception
	 */
	public long countMR(D kv, Map<String, HCompareOp> ops) throws Exception {
		String jobName = "Count " + tableName;
        Job job = new Job(this.getConfig(), jobName);
        job.setJarByClass(HClient.class);

        // scan setting
        Scan scan = getRawScan(kv, ops);
        scan.setCacheBlocks(false);

        // initialize the mapper
        TableMapReduceUtil.initTableMapperJob(getTableName(), scan, CountMapper.class, ImmutableBytesWritable.class, Result.class, job, false);
        job.setNumReduceTasks(0);
        job.setOutputFormatClass(NullOutputFormat.class);

        boolean jobResult = job.waitForCompletion(true);
        if (!jobResult) {
        	return -1;
        }
        Counters counters = job.getCounters();
        Counter rowCounter = counters.findCounter(CountMapper.Counters.ROWS);
		return rowCounter.getValue();
	}
	
	/**
	 * Get the output path of mapreduce job.
	 * 
	 * @return Output path of mapreduce job
	 */
	public String getMROutputPath() {
		return MROutput;
	}
	
	/**
	 * data store to Put
	 * 
	 * @param d data store entity
	 * @return the put
	 */
	public abstract Put toPut(D d);
	
	/**
	 * row to data store, simple version
	 * 
	 * @param r result
	 * @return data store entity
	 */
	public abstract D toDataStore(Result r);
	
	/**
	 * Delete one row.
	 * 
	 * @param rowkey
	 *            row key
	 * @throws IOException
	 */
	public void delete(byte[] rowkey) throws IOException {
		if (rowkey == null || rowkey.length == 0) {
			return;
		}
		Delete d = new Delete(rowkey);
		table.delete(d);
	}
	
	/**
	 * Delete one row via data store 
	 * 
	 * @param d data store
	 * @throws IOException
	 */
	public abstract void delete(D d) throws IOException;
	
	/**
	 * Put data into HBase table.
	 * 
	 * @param d Data
	 * @throws IOException
	 */
	public void put(D d) throws IOException {
        Put p = toPut(d);
        table.put(p);
	}
	
	//###### inner class
	static class CountMapper extends TableMapper<ImmutableBytesWritable, Result> {
	    /** Counter enumeration to count the actual rows. */
	    public static enum Counters {ROWS}

		@Override
		protected void map(ImmutableBytesWritable key, Result value,
				Context context) throws IOException, InterruptedException {
	          context.getCounter(Counters.ROWS).increment(1);
		}
	}
	
	/**
	 * Scan mapper
	 */
	static class ScanMapper extends TableMapper<ImmutableBytesWritable, Result> {

		@Override
		protected void map(ImmutableBytesWritable key, Result value,
				Context context) throws IOException, InterruptedException {
			context.write(key, value);
		}
		
	}
	
	/**
	 * Scan reducer
	 */
	static class ScanReducer extends Reducer<ImmutableBytesWritable, Result, Text, Text> {
		private long _recordCount = 0;
		private long _returnCount = 0;
		private HClient _hclient;
		private List _MRResult = new ArrayList(100);
		
		private long _offset;
		private long _limit;
		private long _ubound;
		private long _returnLimit;
		
		@Override
		protected void setup(Context context) throws IOException,
				InterruptedException {
			_MRResult.clear();
			
			Configuration conf = context.getConfiguration();
			_offset = Integer.parseInt(conf.get(SCAN_MR_OFFSET));
			_limit = Integer.parseInt(conf.get(SCAN_MR_LIMIT));
			_returnLimit = _limit;
			
			if (_offset < 0) {
				_offset = 0;
			}
			if (_limit < 0) {
				_limit = Long.MAX_VALUE;
				_returnLimit = MR_DEFAULT_RETURN_COUNT;
			}
			
			_ubound = _offset + _limit;
			
			// the client
			String client = conf.get(SCAN_MR_CLIENT);
			try {
				Class clientClass = Class.forName(client);
				_hclient = (HClient) clientClass.newInstance();
			} catch (Exception e) {
				throw new IOException("Can not create HClient instance.", e);
			}
		}

		@Override
		protected void reduce(ImmutableBytesWritable key, Iterable<Result> value, Context context)
				throws IOException, InterruptedException {
			if (!value.iterator().hasNext()) {
				return;
			}
			if (_recordCount >= _offset && _recordCount < _ubound) {
				// record between the offset and limit
				Text outKey = new Text(Bytes.toString(key.get()));
				Text outValue = EMPTY_TEXT;
				Object d = _hclient.toDataStore(value.iterator().next());				
				outValue = new Text(d.toString());
				context.write(outKey, outValue);

				if (_returnCount < _returnLimit) {
					// return count in the return limit
					_MRResult.add(d);
					_returnCount++;
				}
			}
			_recordCount++;
		}

		@Override
		protected void cleanup(Context context)
				throws IOException, InterruptedException {
			// serialize some results to HDFS
			Configuration conf = context.getConfiguration();
			FileSystem fs = FileSystem.get(conf);
			String output = conf.get("sculptor.scanMR.output");
			String outputName = output.substring(output.lastIndexOf("/") + 1);
			String serializePath = HClient.getSerializePath(outputName); 
			ObjectOutputStream oos = new ObjectOutputStream(fs.create(new Path(serializePath)));
			oos.writeObject(_MRResult);
		}
	}

}
