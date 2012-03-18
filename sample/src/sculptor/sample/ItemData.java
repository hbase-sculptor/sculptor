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

import sculptor.framework.HEntity;
import sculptor.framework.annotation.Column;
import sculptor.framework.annotation.Rowkey;
import sculptor.framework.annotation.Table;

/**
 * The entity class for sc_item_data table in HBase.
 * 
 */
@Table(name = "sc_item_data")
public class ItemData extends HEntity {

	private static final long serialVersionUID = 7861331319218391209L;
	public static int maxQulifierLength;
	private static int disQulifierLangth;
	
	static {
		maxQulifierLength = HEntity.getMaxQulifierLen(ItemData.class);
		disQulifierLangth = maxQulifierLength + 2;
	}

	byte[] rowkey;

	public byte[] getRowkey() {
		return rowkey;
	}

	// data column family
	/** 店舗ID */
	@Rowkey
	@Column(family = "data", qulifier = "shop_id")
	public int shopID = -1;

	/** 商品ID */
	@Rowkey
	@Column(family = "data", qulifier = "item_id")
	public int itemID = -1;

	/** ジャンルID */
	@Column(family = "data", qulifier = "genre_id")
	public int genreID = -1;

	/** 商品名 */
	@Column(family = "data", qulifier = "item_name")
	public String itemName;

	/** 商品URL */
	@Column(family = "data", qulifier = "full_item_url")
	public String fullItemUrl;

	/** 価格 */
	@Column(family = "data", qulifier = "price")
	public String price;

	// meta column family
	/** 更新日時 */
	@Column(family = "meta", qulifier = "ctime")
	public long ctime = -1l;
	
	private void appendName(StringBuilder sb, String fieldName) {
		super.appendFieldName(sb, fieldName, disQulifierLangth);
	}

	@Override
	public String toString() {
		StringBuilder sb = new StringBuilder();
		
		appendln(sb, "----------------------");

		appendName(sb, "rowkey");
		appendln(sb, rowkey);
		
		appendName(sb, "shop_id");
		appendln(sb, shopID);
		
		appendName(sb, "item_id");
		appendln(sb, itemID);
		
		appendName(sb, "genre_id");
		appendln(sb, genreID);
		
		appendName(sb, "item_name");
		appendln(sb, itemName);
		
		appendName(sb, "full_item_url");
		appendln(sb, fullItemUrl);
		
		appendName(sb, "price");
		appendln(sb, price);
		
		appendName(sb, "ctime");
		appendln(sb, ctime);

		appendln(sb, "----------------------");
		return sb.toString();
	}

}
