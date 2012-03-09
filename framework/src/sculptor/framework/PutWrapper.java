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

import org.apache.hadoop.hbase.client.Put;
import org.apache.hadoop.hbase.util.Bytes;

/**
 * Putのラッパークラス
 *
 */
public class PutWrapper {

    /** putインスタンス */
    private Put put = null;

    /** カラムファミリー名 */
    private byte[] family = null;
    
    // for performance
    private static final byte[] dataFamily = Bytes.toBytes("data");
    private static final byte[] metaFamily = Bytes.toBytes("meta");

    /**
     *  コンストラクタ
     */
    public PutWrapper(Put put) {
        this.put = put;
    }

    /**
     *  コンストラクタ
     */
    public PutWrapper(Put put, String family) {
        this.put = put;
        
        // for performance
        byte[] theFamily;
        if ("data".equals(family)) {
        	theFamily = dataFamily;
        } else if ("meta".equals(family)) {
        	theFamily = metaFamily;
        } else {
        	theFamily = Bytes.toBytes(family); 
        }
        
        this.family = theFamily;
    }

    /**
     *  コンストラクタ
     */
    public PutWrapper(Put put, byte[] family) {
        this.put = put;
        this.family = family;
    }

    /**
     *  putインスタンスを返す
     */
    public Put getInstance() {
        return put;
    }


    // family指定タイプ
    public void add(String family, String qualifier, byte[] value) {
    	put.add(Bytes.toBytes(family), Bytes.toBytes(qualifier), value);
    }

    public void add(String family, String qualifier, byte value) {
        // byte型変数をbyte配列に変換する
        byte[] array = new byte[1];
        array[0] = value;
        put.add(Bytes.toBytes(family), Bytes.toBytes(qualifier), array);
    }

    public void add(String family, String qualifier, int value) {
        put.add(Bytes.toBytes(family), Bytes.toBytes(qualifier), Bytes.toBytes(value));
    }

    public void add(String family, String qualifier, short value) {
        put.add(Bytes.toBytes(family), Bytes.toBytes(qualifier), Bytes.toBytes(value));
    }

    public void add(String family, String qualifier, long value) {
        put.add(Bytes.toBytes(family), Bytes.toBytes(qualifier), Bytes.toBytes(value));
    }

    public void add(String family, String qualifier, float value) {
        put.add(Bytes.toBytes(family), Bytes.toBytes(qualifier), Bytes.toBytes(value));
    }

    public void add(String family, String qualifier, double value) {
        put.add(Bytes.toBytes(family), Bytes.toBytes(qualifier), Bytes.toBytes(value));
    }

    public void add(String family, String qualifier, String value) {
        put.add(Bytes.toBytes(family), Bytes.toBytes(qualifier), Bytes.toBytes(value));
    }

    public void add(String family, String qualifier, boolean value) {
        put.add(Bytes.toBytes(family), Bytes.toBytes(qualifier), Bytes.toBytes(value));
    }


    // family省略タイプ
    public void add(String qualifier, byte[] value) {
    	put.add(family, Bytes.toBytes(qualifier), value);
    }
    
    public void add(String qualifier, byte value) {
        // byte型変数をbyte配列に変換する
        byte[] array = new byte[1];
        array[0] = value;
        put.add(family, Bytes.toBytes(qualifier), array);
    }

    public void add(String qualifier, int value) {
        put.add(family, Bytes.toBytes(qualifier), Bytes.toBytes(value));
    }

    public void add(String qualifier, short value) {
        put.add(family, Bytes.toBytes(qualifier), Bytes.toBytes(value));
    }

    public void add(String qualifier, long value) {
        put.add(family, Bytes.toBytes(qualifier), Bytes.toBytes(value));
    }

    public void add(String qualifier, float value) {
        put.add(family, Bytes.toBytes(qualifier), Bytes.toBytes(value));
    }

    public void add(String qualifier, double value) {
        put.add(family, Bytes.toBytes(qualifier), Bytes.toBytes(value));
    }

    public void add(String qualifier, String value) {
        put.add(family, Bytes.toBytes(qualifier), Bytes.toBytes(value));
    }

    public void add(String qualifier, boolean value) {
        put.add(family, Bytes.toBytes(qualifier), Bytes.toBytes(value));
    }
}
