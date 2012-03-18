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

import java.io.IOException;
import java.util.Calendar;
import java.util.HashMap;
import java.util.Map;
import java.util.NavigableMap;

import org.apache.commons.logging.Log;
import org.apache.commons.logging.LogFactory;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.hbase.client.Put;
import org.apache.hadoop.hbase.client.Result;
import org.apache.hadoop.hbase.client.ResultScanner;
import org.apache.hadoop.hbase.client.Scan;
import org.apache.hadoop.hbase.filter.CompareFilter.CompareOp;
import org.apache.hadoop.hbase.filter.FilterList;
import org.apache.hadoop.hbase.filter.SingleColumnValueFilter;
import org.apache.hadoop.hbase.util.Bytes;

import sculptor.framework.HClient;
import sculptor.framework.HCompareOp;
import sculptor.framework.HScanner;
import sculptor.framework.PutWrapper;
import sculptor.framework.annotation.Table;
import sculptor.framework.util.ByteArray;

/**
 * 
 * The client class for sc_item_data table in HBase.
 * 
 * Note: this class is NOT thread safe
 * 
 */
@Table(name = "sc_item_data")
public class HItemData extends HClient<ItemData> {
    /** 店舗IDの長さ（32進） */
    static final int SIZE_OF_SHOP_ID_32 = 5;

    /** 商品IDの長さ（32進） */
    static final int SIZE_OF_ITEM_ID_32 = 8;
    
    /** shop IDの最大値(10進) */
    static final int MAX_SHOP_ID = 9999999;
    
    /** item IDの最大値(10進) */
    static final int MAX_ITEM_ID = Integer.MAX_VALUE;

    /** sc_item_dataのrow keyの長さ */
    static final int SIZE_OF_ROWKEY_ITEM_DATA_32 = 13;

    final byte[] DATA_FAMILY = Bytes.toBytes("data");
    
	/** put bufferの初期値 */
	static final int DEFAULT_PUT_BUFFER = 256;

	/** default # of row to cache when excuting a scan */
	static final int DEFAULT_SCAN_CACHE = 256;

	/** default # of row to cache when excuting a scan for key only */
	static final int DEFAULT_SCAN_KEY_ONLY_CACHE = 2048;

	static final byte[] META_FAMILY = Bytes.toBytes("meta");

	private static Log log = LogFactory.getLog(HItemData.class);
	
	/**
	 * the constructor
	 * 
	 * @param countryCode
	 *            country code
	 * @param that
	 *            specific configuration
	 */
	public HItemData(Configuration that) {
		super("sc_item_data", that);
	}

	/**
	 * the constructor
	 */
	public HItemData() {
		super("sc_item_data");
	}

	/**
	 * hbaseのsc_item_dataのrow-keyを生成する
	 * 
	 * @param shopID
	 *            店舗ID
	 * @param itemID
	 *            商品ID
	 * @return row-key
	 */
	public static byte[] encodeRowkey(int shopID, int itemID) {
		if (shopID == -1 || itemID == -1) {
			return new byte[0];
		}

		// row key: shop_id + item_id
		byte[] bshopid = ByteArray.toByte32Radix(shopID,
				SIZE_OF_SHOP_ID_32);
		byte[] bitemid = ByteArray.toByte32Radix(itemID,
				SIZE_OF_ITEM_ID_32);
		byte[] rowKey = ByteArray.concatenate(bshopid, bitemid);
		return rowKey;
	}

	@Override
	public byte[] toRowkey(ItemData d) {
		return encodeRowkey(d.shopID, d.itemID);
	}

	/**
	 * row keyを文字列にdecode
	 * 
	 * @param rowkey
	 *            row key
	 * @return 10進のshop_id-item_id
	 */
	public static String decodeRowkey(byte[] rowkey) {
		int srcOffset = 0;
		byte[] bShopID = new byte[SIZE_OF_SHOP_ID_32];
		System.arraycopy(rowkey, srcOffset, bShopID, 0,
				SIZE_OF_SHOP_ID_32);
		srcOffset += SIZE_OF_SHOP_ID_32;

		byte[] bItemID = new byte[SIZE_OF_ITEM_ID_32];
		System.arraycopy(rowkey, srcOffset, bItemID, 0,
				SIZE_OF_ITEM_ID_32);

		long shopID = ByteArray.toDecimal(bShopID);
		long itemID = ByteArray.toDecimal(bItemID);

		return String.format("%d-%d", shopID, itemID);
	}

	@Override
	public Scan getRawScan(ItemData d, Map<String, HCompareOp> ops) {
		int startShopID = 0;
		int startItemID = 0;

		int endShopID = MAX_SHOP_ID;
		int endItemID = MAX_ITEM_ID;

		// some performance improvement
		// shop_id指定
		HCompareOp shopIDOp = ops.get("shop_id");
		if (shopIDOp == HCompareOp.EQUAL) {
			startShopID = d.shopID;
			endShopID = startShopID;
		}

		// item idも指定
		HCompareOp itemIDOp = ops.get("item_id");
		if (itemIDOp == HCompareOp.EQUAL) {
			startItemID = d.itemID;
			endItemID = startItemID;
		}

		log.info(String
				.format("scan start row, shop_id=%d, item_id=%d", startShopID, startItemID));
		log.info(String
				.format("scan stop row, shop_id=%d, item_id=%d", endShopID, endItemID));

		byte[] startRow = encodeRowkey(startShopID, startItemID);
		byte[] endRow = encodeRowkey(endShopID, endItemID);
		Scan s = new Scan(startRow, endRow);
		s.addFamily(DATA_FAMILY);
		s.addFamily(META_FAMILY);
		s.setCacheBlocks(false);
		s.setMaxVersions();
		s.setCaching(DEFAULT_SCAN_CACHE);

		FilterList fl = new FilterList();
		for (String column : ops.keySet()) {
			byte[] value;
			byte[] family = DATA_FAMILY;

			if ("ctime".equals(column)) {
				value = Bytes.toBytes(d.ctime);
				family = META_FAMILY;

			} else if ("shop_id".equals(column)) {
				value = Bytes.toBytes(d.shopID);

			} else if ("item_id".equals(column)) {
				value = Bytes.toBytes(d.itemID);

			} else if ("genre_id".equals(column)) {
				value = Bytes.toBytes(d.genreID);

			} else if ("price".equals(column)) {
				value = Bytes.toBytes(d.price);

			} else if ("full_item_url".equals(column)) {
				value = Bytes.toBytes(d.fullItemUrl);

			} else if ("item_name".equals(column)) {
				value = Bytes.toBytes(d.itemName);

			} else {
				// ignore
				continue;
			}

			byte[] qualifier = Bytes.toBytes(column);
			HCompareOp hop = ops.get(column);
			CompareOp op = HClient.toCompareOp(hop);
		
			SingleColumnValueFilter filter = new SingleColumnValueFilter(
					family, qualifier, op, value);
			filter.setFilterIfMissing(true);
			fl.addFilter(filter);
		}

		s.setFilter(fl);
		return s;
	}

	/**
	 * HBase更新用Putに変換
	 * 
	 * @param item
	 *            itemdata
	 * @return put
	 */
	public Put toPut(ItemData item) {

		byte[] rowKey = encodeRowkey(item.shopID, item.itemID);
		if (rowKey.length == 0) {
			return null;
		}

		Put p = new Put(rowKey);
		PutWrapper wrapper = new PutWrapper(p);

		// add column to family "meta"
		long ctime;
		if (item.ctime != -1l) {
			ctime = item.ctime;
		} else {
			ctime = Calendar.getInstance().getTimeInMillis();
		}
		wrapper.add("meta", "ctime", ctime);

		// add column to family "data"
		if (item.shopID != -1) {
			wrapper.add("data", "shop_id", item.shopID);
		}

		if (item.itemID != -1) {
			wrapper.add("data", "item_id", item.itemID);
		}

		if (item.genreID != -1) {
			wrapper.add("data", "genre_id", item.genreID);
		}

		if (item.itemName != null) {
			wrapper.add("data", "item_name", item.itemName);
		}

		if (item.fullItemUrl != null) {
			wrapper.add("data", "full_item_url", item.fullItemUrl);
		}

		if (item.price != null) {
			wrapper.add("data", "price", item.price);
		}

		return wrapper.getInstance();

	}

	/**
	 * HBaseの一行をItemDataに変換
	 * 
	 * @param r
	 *            row in HBase
	 * @return 商品情報
	 */
	public ItemData toEntity(Result r) {
		if (r == null || r.isEmpty()) {
			return null;
		}

		ItemData item = null;

		// meta family
		NavigableMap<byte[], byte[]> map = r.getFamilyMap(META_FAMILY);
		if (map != null && !map.isEmpty()) {
			item = new ItemData();

			// ctime
			byte[] bCtime = map.get(Bytes.toBytes("ctime"));
			if (bCtime != null) {
				item.ctime = Bytes.toLong(bCtime);
			}
		}

		// data family
		map = r.getFamilyMap(DATA_FAMILY);
		if (map != null && !map.isEmpty()) {
			if (item == null) {
				item = new ItemData();
			}

			// shop ID
			byte[] bShopID = map.get(Bytes.toBytes("shop_id"));
			if (bShopID != null) {
				item.shopID = Bytes.toInt(bShopID);
			}

			// item ID
			byte[] bItemID = map.get(Bytes.toBytes("item_id"));
			if (bItemID != null) {
				item.itemID = Bytes.toInt(bItemID);
			}

			// genre ID
			byte[] bGenreID = map.get(Bytes.toBytes("genre_id"));
			if (bGenreID != null) {
				item.genreID = Bytes.toInt(bGenreID);
			}

			// item name
			byte[] bItemName = map.get(Bytes.toBytes("item_name"));
			if (bItemName != null) {
				item.itemName = Bytes.toString(bItemName);
			}

			// full item url
			byte[] bFullItemUrl = map.get(Bytes.toBytes("full_item_url"));
			if (bFullItemUrl != null) {
				item.fullItemUrl = Bytes.toString(bFullItemUrl);
			}

			// price
			byte[] bPrice = map.get(Bytes.toBytes("price"));
			if (bPrice != null) {
				item.price = Bytes.toString(bPrice);
			}
		}

		if (item == null) {
			item = new ItemData();
		}
		item.rowkey = r.getRow();

		return item;
	}
	
	@Override
	public HScanner<ItemData> scan(ItemData kv, Map<String, HCompareOp> ops)
			throws IOException {
		Scan s = getRawScan(kv, ops);
		ResultScanner rs = table.getScanner(s);
		return new ItemDataScanner(this, rs);
	}


	// ######## inner class ##########
	protected class ItemDataScanner extends HScanner<ItemData> {

		public ItemDataScanner(HClient<ItemData> client, ResultScanner rs) {
			super(client, rs);
		}
	}

	public static void main(String[] args) {
		HItemData hi = null;
		try {
			byte b0 = ByteArray.b0;

			// for unit test
			int shopID = 48;
			int itemID = 51;
			byte[] rowkey = encodeRowkey(shopID, itemID);

			byte[] expected = new byte[] { b0, b0, b0, ByteArray.b1, ByteArray.bg,
					b0, b0, b0, b0, b0, b0, ByteArray.b1, ByteArray.bj };
			assert (Bytes.equals(rowkey, expected));

			String decoded = decodeRowkey(rowkey);
			assert ("48-51".equals(decoded));
			
			hi = new HItemData();
			ItemData id = new ItemData();
			id.shopID = 235063;
			Map<String, HCompareOp> ops = new HashMap<String, HCompareOp>();
			ops.put("shop_id", HCompareOp.EQUAL);
			HScanner<ItemData> scanner = hi.scan(id, ops);
			assert (scanner.hasNext());
			if (scanner.hasNext()) {
				System.out.println(scanner.next());
			}
			
			System.out.println("unit test passed");
		} catch (Exception e) {
			e.printStackTrace();
		} finally {
			if (hi != null) {
				hi.close();
			}
		}
	}
}
