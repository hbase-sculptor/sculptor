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
import java.io.FileReader;
import java.util.Map;
import java.util.TreeMap;

import org.apache.commons.logging.Log;
import org.apache.commons.logging.LogFactory;

import sculptor.framework.annotation.Table;

/**
 * Factory class for creating HClient instances.
 * 
 */
public class Sculptor {

	private static Log log = LogFactory.getLog(Sculptor.class);
	public static File sculptorRoot;

	public static Map<String, Class<? extends HEntity>> entities;
	public static Map<String, Class<? extends HClient>> clients;
	public static Map<String, HClassDescriptor> descriptors;

	/**
	 * Initialize Sculptor.
	 * 
	 * @param root
	 *            the root path of Sculptor
	 * @throws Exception
	 *             Something wrong during the initialization.
	 */
	public static void initialize(String root) throws Exception {
		try {
			sculptorRoot = new File(root);

			entities = new TreeMap<String, Class<? extends HEntity>>();
			clients = new TreeMap<String, Class<? extends HClient>>();
			descriptors = new TreeMap<String, HClassDescriptor>();

			String tables = sculptorRoot.getAbsolutePath() + "/conf/tables";
			String tableDefine;
			BufferedReader br = new BufferedReader(new FileReader(tables));
			while ((tableDefine = br.readLine()) != null) {
				if ("".equals(tableDefine.trim())
						|| tableDefine.startsWith("#")) {
					// ignore empty or comment lines
					continue;
				}
				String[] mapping = tableDefine.split(":");
				if (mapping.length < 3) {
					throw new Exception("Wrong table mapping: " + tableDefine);
				}

				// table name
				String tableName = mapping[0].trim();
				log.info(String.format("Loading table %s...", tableName));

				// load entity
				Class entityClass = Class.forName(mapping[1].trim());
				HClassDescriptor descriptor = HEntity.getClassInfo(entityClass);
				if (!tableName.equals(descriptor.table)) {
					throw new Exception(
							String.format(
									"Wrong HEntity table annotation, expected: %s, actual: %s",
									tableName, descriptor.table));
				}

				// load client
				String client = mapping[2].trim();
				Class clientClass = Class.forName(client);
				String annotationTableName = ((Table) clientClass
						.getAnnotation(Table.class)).name();
				if (!tableName.equals(annotationTableName)) {
					throw new Exception(
							String.format(
									"Wrong HClient table annotation, expected: %s, actual: %s",
									tableName, annotationTableName));
				}
				descriptor.clientClassName = client;

				entities.put(tableName, entityClass);
				clients.put(tableName, clientClass);
				descriptors.put(tableName, descriptor);
			}

		} catch (Exception e) {
			log.error("Can not initialize Sculptor.", e);
			throw e;
		}
	}

	public static void main(String[] args) {
		try {
			initialize("/Users/uprush/repo/sculptor");
		} catch (Exception e) {
			e.printStackTrace();
		}
	}
}
