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

import java.util.List;
import java.util.Set;

/**
 * HBase上のテーブルに紐づいたクラスを定義
 *
 */
public class HClassDescriptor {
	/** Client class name */
	String clientClassName;
	/** Entity class name */
	String entityClassName;
	/** 紐づいているテーブル名 */
	String table;
	/** List of column family */
	Set<String> columnFamilies;
	/** フィールドのリスト */
	List<HFieldDescriptor> hFieldDescriptors;

	/**
	 * Get entity's canonical name.
	 * 
	 * @return the entity's canonical name
	 */
	public String getEntityClassName() {
		return entityClassName;
	}
	
	/**
	 * Get client's canonical name.
	 * 
	 * @return the client's canonical name
	 */
	public String getClientClassName() {
		return clientClassName;
	}

	/**
	 * Get the table name
	 * 
	 * @return the table name
	 */
	public String getTable() {
		return table;
	}

	/**
	 * Get all fields
	 * 
	 * @return all fields
	 */
	public List<HFieldDescriptor> gethFieldDescriptors() {
		return hFieldDescriptors;
	}
	
	public Set<String> getColumnFamilies() {
		return columnFamilies;
	}

}
