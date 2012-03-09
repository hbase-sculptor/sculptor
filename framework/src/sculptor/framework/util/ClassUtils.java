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
package sculptor.framework.util;

import java.io.IOException;
import java.util.ArrayList;
import java.util.Enumeration;
import java.util.List;
import java.util.jar.JarEntry;
import java.util.jar.JarFile;
import java.util.regex.Matcher;
import java.util.regex.Pattern;

/**
 * Utility class
 * 
 */
public class ClassUtils {

	/**
	 * 指定したpackage以下のクラス一覧を取得
	 * 
	 * @param packageName
	 * @return 指定したpackage以下のクラス名のリスト
	 * @throws IOException
	 */
	public static List<String> getJavaClasses(String packageName,
			String jarFilePath) throws IOException {
		List<String> classNameList = new ArrayList<String>();

		JarFile jarFile = new JarFile(jarFilePath);
		Enumeration<JarEntry> jarEntries = jarFile.entries();

		packageName += ".";
		Pattern p = Pattern.compile("(" + packageName.replaceAll("\\.", "/")
				+ ".*)\\.class");

		while (jarEntries.hasMoreElements()) {
			JarEntry entry = jarEntries.nextElement();
			String str = entry.getName();

			Matcher m = p.matcher(str);
			if (m.find()) {
				classNameList.add(m.group(1).replaceAll("/", "\\."));
			}

		}

		return classNameList;
	}

	public static void main(String[] args) throws IOException {
		String packageName = "sculptor.framework";
		List<String> li = getJavaClasses(packageName,
				"../sculptor-0.0.1.jar");
		for (String c : li) {
			System.out.println(c);
		}
	}
}
