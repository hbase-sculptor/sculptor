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
package sculptor.sample;

import java.io.BufferedReader;
import java.io.FileReader;

import junit.framework.TestCase;

import org.apache.hadoop.hbase.util.Bytes;
import org.junit.Test;

import sculptor.framework.util.ByteArray;

public class HItemDataTest extends TestCase {

	public static void main(String[] args) {
		if (args.length < 2) {
			usage();
			System.exit(-1);
		}
		String action = args[0];
		if ("loaddata".equals(action)) {
			String dataFile = args[1];
			loadData(dataFile);
		} else {
			usage();
			System.exit(-1);
		}
	}
	
	@Test
	public void testEncodeRowkey() {
		byte b0 = ByteArray.b0;

		int shopID = 48;
		int itemID = 51;
		byte[] rowkey = HItemData.encodeRowkey(shopID, itemID);

		byte[] expected = new byte[] { b0, b0, b0, ByteArray.b1, ByteArray.bg,
				b0, b0, b0, b0, b0, b0, ByteArray.b1, ByteArray.bj };
		assertEquals(true, Bytes.equals(rowkey, expected));
	}
	
	@Test
	public void testDecodeRowkey() {
		int shopID = 48;
		int itemID = 51;
		byte[] rowkey = HItemData.encodeRowkey(shopID, itemID);
		String decoded = HItemData.decodeRowkey(rowkey);
		assertEquals("48-51", decoded);
	}
	
	// TODO add test case

	private static void loadData(String dataFile) {
		BufferedReader br = null;
		HItemData client = null;
		try {
			
			br = new BufferedReader(new FileReader(dataFile));
			client = new HItemData();
			String line;
			while ((line = br.readLine()) != null)   {
				String[] columns = line.split("\\t");
				ItemData item = new ItemData();
				item.shopID = Integer.valueOf(columns[0]).intValue();
				item.itemID = Integer.valueOf(columns[1]).intValue();
				item.genreID = Integer.valueOf(columns[2]).intValue();
				item.itemName = columns[3];
				item.price = columns[4];
				
				client.put(item);
			}
			
		} catch (Exception e) {
			System.err.println("Error loading data from " + dataFile + " into HBase table.");
			e.printStackTrace();
		} finally {
			if (br != null) {
				try {
					br.close();
				} catch (Exception e) {
					// ignore
				}
			}
			if (client != null) {
				client.close();
			}
		}
	}

	private static void usage() {
		System.err.println("Usage:");
		System.err.println("HItemDataTest loaddata <data_file>");
	}
}
