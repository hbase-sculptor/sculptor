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

import java.io.Closeable;
import java.io.IOException;
import java.util.ArrayList;
import java.util.List;

import org.apache.hadoop.hbase.client.Result;
import org.apache.hadoop.hbase.client.ResultScanner;

/**
 * Abstract scanner for table on HBase.
 * 
 * @param <D> the type returned with iteration
 */
public class HScanner<D extends HEntity> implements Closeable {
	protected HClient<D> client;
	protected ResultScanner rs;
	protected boolean closed = false;

	// The next RowResult, possibly pre-read
	private D next = null;

	public HScanner(HClient<D> client, ResultScanner rs) {
		this.client = client;
		this.rs = rs;
	}
	
	public D next() throws IOException {

		// since hasNext() does the real advancing, we call this to
		// determine
		// if there is a next before proceeding.
		if (!hasNext()) {
			return null;
		}

		// if we get to here, then hasNext() has given us an item to
		// return.
		// we want to return the item and then null out the next
		// pointer, so
		// we use a temporary variable.
		D temp = next;
		next = null;
		return temp;

	}
	
	public List<D> next(int nbRows) throws IOException {
		Result[] rows = rs.next(nbRows);
		List<D> resultSets = new ArrayList<D>();
		for (Result r : rows) {
			D ds = client.toEntity(r);
			if (ds != null) {
				resultSets.add(ds);
			}
		}
		
		return resultSets;
	}
	
	public boolean hasNext() {
		if (closed) {
			return false;
		}

		if (next == null) {
			try {

				Result r = rs.next();
				if (r == null) {
					return false;
				}
				next = client.toEntity(r);
				return next != null;
				
			} catch (IOException e) {
				throw new RuntimeException(e);
			}
		}
		return true;
	}

	public void close() {
		if (rs != null) {
			rs.close();
			rs = null;
		}
		closed = true;
	}

}
