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

import java.io.BufferedReader;
import java.io.File;
import java.io.FileNotFoundException;
import java.io.FileReader;
import java.io.IOException;
import java.util.ArrayList;
import java.util.List;

import org.apache.commons.logging.Log;
import org.apache.commons.logging.LogFactory;
import org.apache.hadoop.hbase.util.Bytes;
import org.apache.hadoop.hbase.util.RegionSplitter.SplitAlgorithm;

/**
 * Split algorithm using a specified file.<br/>
 * Put your splitting keys in {@link #SPLIT_KEY_FILE}, one key per line.
 * 
 */
public class FixedSplit implements SplitAlgorithm {
	static final Log LOG = LogFactory.getLog(FixedSplit.class);

	public static final String SPLIT_KEY_FILE = "/tmp/fixed-split-keys.txt";
	private File _regions;
	
	public FixedSplit() {
		try {
			
			_regions = new File(SPLIT_KEY_FILE);
			if (!_regions.exists()) {
				throw new FileNotFoundException("split key file not found. " + SPLIT_KEY_FILE);
			}

			
		} catch (FileNotFoundException e) {
			LOG.error(String.format("put your splitting keys in [%s], one key per line", SPLIT_KEY_FILE), e);
			throw new RuntimeException("Aborted!.", e);
		}
	}

	@Override
	public byte[] firstRow() {
		// TODO Auto-generated method stub
		return null;
	}

	@Override
	public byte[] lastRow() {
		// TODO Auto-generated method stub
		return null;
	}

	@Override
	public String rowToStr(byte[] row) {
		// TODO Auto-generated method stub
		return null;
	}

	@Override
	public String separator() {
		// TODO Auto-generated method stub
		return null;
	}

	@Override
	public byte[] split(byte[] start, byte[] end) {
		// TODO Auto-generated method stub
		return null;
	}

	@Override
	public byte[][] split(int numberOfSplits) {
		try {
			
			List<byte[]> returnBytes = new ArrayList<byte[]> ();
			BufferedReader br = new BufferedReader(new FileReader(_regions));
			String line;
			while ((line = br.readLine()) != null) {
				if (line.trim().length() > 0) {
					returnBytes.add(Bytes.toBytes(line));
				}
			}
			return returnBytes.toArray(new byte[0][]);
			
		} catch (IOException e) {
			LOG.error("Error reading splitting keys from " + SPLIT_KEY_FILE, e);
			throw new RuntimeException("Aborted!.", e);
		}
	}

	@Override
	public byte[] strToRow(String input) {
		// TODO Auto-generated method stub
		return null;
	}

}
