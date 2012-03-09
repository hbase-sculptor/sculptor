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

/**
 * HBaseのcolumnをJava classの filedで表す
 * 
 */
public class HFieldDescriptor {
	/** java classのfiled名 */
	String fieldName;
	
	/** hbase column family */
	String family;
	
	/** hbase column */
	String qualifier;
	
	/** hbaseのrowkey項目かどうか */
	boolean isRowkey;
	
	/** java classのfieldの正規名 */
	String canonicalName;
	
	public String getFieldName() {
		return fieldName;
	}

	public String getFamily() {
		return family;
	}

	public String getQualifier() {
		return qualifier;
	}

	public boolean isRowkey() {
		return isRowkey;
	}
	
	public String getCanonicalName() {
		return canonicalName;
	}
	
}
