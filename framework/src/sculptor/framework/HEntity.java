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

import java.io.Serializable;
import java.lang.reflect.Field;
import java.util.ArrayList;
import java.util.List;


import org.apache.commons.lang.StringUtils;
import org.apache.hadoop.hbase.util.Bytes;

import sculptor.framework.annotation.Column;
import sculptor.framework.annotation.Rowkey;
import sculptor.framework.annotation.Table;

/**
 * HBase上のデータを表す
 * 
 */
public class HEntity implements Serializable {

	private static final long serialVersionUID = -1195335302706430611L;
	
	/**
	 * Get the class descriptor
	 * 
	 * @param clazz the class
	 * @return class descriptor
	 */
	public static HClassDescriptor getClassInfo(Class<? extends HEntity> clazz) {
		HClassDescriptor hClassDescriptor = new HClassDescriptor();
		hClassDescriptor.entityClassName = clazz.getCanonicalName();
		hClassDescriptor.table = clazz.getAnnotation(Table.class).name();
		hClassDescriptor.hFieldDescriptors = getFields(clazz);

		return hClassDescriptor;
	}
	
	/**
	 * Get all fields descriptor
	 * 
	 * @param clazz the class
	 * @return all fields descriptor
	 */
	public static List<HFieldDescriptor> getFields(Class<? extends HEntity> clazz) {
		Field[] fields = clazz.getFields();
		List<HFieldDescriptor> hFields = new ArrayList<HFieldDescriptor>();

		for (Field f : fields) {
			HFieldDescriptor hf = toHField(f);
			hFields.add(hf);
		}

		return hFields;

	}
	
	/**
	 * Get the maximum length of the qualifiers
	 * 
	 * @param clazz the class
	 * @return maximum qualifier
	 */
	public static int getMaxQulifierLen(Class<? extends HEntity> clazz) {
		List<HFieldDescriptor> fields = getFields(clazz);
		int maxLength = 0;
		for (HFieldDescriptor field : fields) {
			String qualifier = field.getQualifier();
			if (qualifier == null) {
				continue;
			}
			int length = qualifier.length();
			if (length > maxLength) {
				maxLength = length;
			}
		}
		return maxLength;
	}

	/**
	 * class情報を取得
	 * 
	 * @return class情報
	 */
	public HClassDescriptor getClassInfo() {
		return getClassInfo(this.getClass());
	}

	/**
	 * すべてのfield情報を取得
	 * 
	 * @return すべてのfield情報
	 */
	public List<HFieldDescriptor> getFields() throws SecurityException {
		return getFields(this.getClass());
	}

	/**
	 * 指定のfiledの情報を取得
	 * 
	 * @param fieldName
	 *            filed name
	 * @return field情報
	 * @throws NoSuchFieldException
	 * @throws SecurityException
	 */
	public HFieldDescriptor getField(String fieldName)
			throws SecurityException, NoSuchFieldException {
		Field f = this.getClass().getField(fieldName);
		return toHField(f);
	}

	private static HFieldDescriptor toHField(Field f) {
		HFieldDescriptor hf = new HFieldDescriptor();
		hf.fieldName = f.getName();
		hf.canonicalName = f.getType().getCanonicalName();
		hf.isRowkey = f.getAnnotation(Rowkey.class) != null;
		Column column = f.getAnnotation(Column.class);
		if (column != null) {
			hf.family = column.family();
			hf.qualifier = column.qulifier();
		}
		return hf;
	}
	
	protected void appendln(StringBuilder sb, String msg) {
		if (msg == null) {
			sb.append("").append("\n");
		} else {
			sb.append(msg).append("\n");
		}
	}
	
	protected void appendln(StringBuilder sb, byte msg) {
		sb.append(msg).append("\n");
	}
	
	protected void appendln(StringBuilder sb, byte[] msg) {
		sb.append(Bytes.toString(msg)).append("\n");
	}
	
	protected void appendln(StringBuilder sb, long msg) {
		sb.append(String.valueOf(msg)).append("\n");
	}
	
	protected void appendFieldName(StringBuilder sb, String fieldName, int length) {
		String rpad = StringUtils.rightPad(fieldName, length);
		sb.append(rpad).append("=> ");
	}

}
