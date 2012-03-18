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
				String[] clientAndEntity = tableDefine.split("\\s+");
				
				// load entity
				Class entityClass = Class.forName(clientAndEntity[0]);
				HClassDescriptor descriptor = HEntity.getClassInfo(entityClass);
				log.info(String.format("Loading table %s...", descriptor.table));

				// load client
				Class clientClass = Class.forName(clientAndEntity[1]);
				String tableName = ((Table) clientClass.getAnnotation(Table.class)).name();
				if (!tableName.equals(descriptor.table)) {
					throw new Exception("Entity and client implmentation should be associated with same table.");
				}
				descriptor.clientClassName = clientAndEntity[1];
				
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
