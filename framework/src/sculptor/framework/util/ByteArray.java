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

import org.apache.hadoop.hbase.util.Bytes;

/**
 * byte関連処理
 * 
 */
public class ByteArray {
	
	public static final byte b0 = Bytes.toBytes("0")[0];
	public static final byte b1 = Bytes.toBytes("1")[0];
	public static final byte b2 = Bytes.toBytes("2")[0];
	public static final byte b3 = Bytes.toBytes("3")[0];
	public static final byte b4 = Bytes.toBytes("4")[0];
	public static final byte b5 = Bytes.toBytes("5")[0];
	public static final byte b6 = Bytes.toBytes("6")[0];
	public static final byte b7 = Bytes.toBytes("7")[0];
	public static final byte b8 = Bytes.toBytes("8")[0];
	public static final byte b9 = Bytes.toBytes("9")[0];
	public static final byte ba = Bytes.toBytes("a")[0];
	public static final byte bb = Bytes.toBytes("b")[0];
	public static final byte bc = Bytes.toBytes("c")[0];
	public static final byte bd = Bytes.toBytes("d")[0];
	public static final byte be = Bytes.toBytes("e")[0];
	public static final byte bf = Bytes.toBytes("f")[0];
	public static final byte bg = Bytes.toBytes("g")[0];
	public static final byte bh = Bytes.toBytes("h")[0];
	public static final byte bi = Bytes.toBytes("i")[0];
	public static final byte bj = Bytes.toBytes("j")[0];
	public static final byte bk = Bytes.toBytes("k")[0];
	public static final byte bl = Bytes.toBytes("l")[0];
	public static final byte bm = Bytes.toBytes("m")[0];
	public static final byte bn = Bytes.toBytes("n")[0];
	public static final byte bo = Bytes.toBytes("o")[0];
	public static final byte bp = Bytes.toBytes("p")[0];
	public static final byte bq = Bytes.toBytes("q")[0];
	public static final byte br = Bytes.toBytes("r")[0];
	public static final byte bs = Bytes.toBytes("s")[0];
	public static final byte bt = Bytes.toBytes("t")[0];
	public static final byte bu = Bytes.toBytes("u")[0];
	public static final byte bv = Bytes.toBytes("v")[0];
	
	
    /** 32進基数変換テーブル */
	public static final byte[] RADIX_CONVERT_TABLE = new byte[] {
         b0
        ,b1
        ,b2
        ,b3
        ,b4
        ,b5
        ,b6
        ,b7
        ,b8
        ,b9
        ,ba
        ,bb
        ,bc
        ,bd
        ,be
        ,bf
        ,bg
        ,bh
        ,bi
        ,bj
        ,bk
        ,bl
        ,bm
        ,bn
        ,bo
        ,bp
        ,bq
        ,br
        ,bs
        ,bt
        ,bu
        ,bv
    };
    
    /**
     * 10進数から32進数に基数変換を行います。
     *
     * @param decimal 10進数（long型なので19桁までの正数値しか扱えない）
     * @param length 基数変換後の桁数
     * @return 基数変換結果
     */
    public static byte[] toByte32Radix(long decimal, int length) {

        // 基数変換結果が最大桁に達しないかもしれないので、とりあえず配列を0で埋めておく。
        byte[] bArray = new byte[length];
        for (int i = 0; i < length; i++) {
            bArray[i] = RADIX_CONVERT_TABLE[0];
        }

        // 商
        long q = decimal;
        // 剰余
        long rem = 0;
        // 配列インデックス
        // 基数変換結果は下桁から求まるので、配列の最後から結果を埋めていく。
        int i = length - 1;

        /* 基数変換 */
        while (q >= 32) {
            // 剰余算
            rem = q & ~(Long.MAX_VALUE << 5);
            // 除算
            q = (q - rem) >> 5;
            // 基数変換テーブルから基数表現（0～9,a～v）を取得する。
            bArray[i] = RADIX_CONVERT_TABLE[(int)rem];
            // 次の基数変換結果は、上桁にセットする。
            i--;
        }
        bArray[i] = RADIX_CONVERT_TABLE[(int)q];

        return bArray;
    }
    
    /**
     * 32進数に変換した数字を10進に逆変換。
     * 
     * @param byte32 32進数の数字
     * @return 10進数字
     */
    public static long toDecimal(byte[] byte32) {
    	int length = byte32.length;
    	if (length == 0) {
    		return -1L;
    	}
    	
    	byte b;
    	int decimal = 0;
    	int place = 0;
    	for (int i = length - 1; i >= 0; i--) {
    		b = byte32[i];
    		decimal += (long)Math.pow(32, place) * toSingleDecimal(b);
    		place++;
    	}
    	return decimal;
    }

    /**
     * 基数変換を行います。
     *
     * @param decimal 10進数（long型に変換するので19桁までの数値しか扱えない）
     * @param length 基数変換後の桁数
     * @return 基数変換結果
     */
    public static byte[] toByte32Radix(String decimal, int length) {
        return toByte32Radix(Long.parseLong(decimal), length);
    }


    /**
     * byte配列の連結します。
     *
     * @param bArrays byte配列
     * @return byte配列の連結結果
     */
    public static byte[] concatenate(byte[]... bArrays) {

        if (bArrays.length == 0) {
            return new byte[0];
        }

        byte[] b = bArrays[0];
        for (int i = 1; i < bArrays.length; i++) {
            b = Bytes.add(b, bArrays[i]);
        }
        return b;

    }

    /**
     * 0から31の10進数が対応した32進のbyte変換
     * 
     * @param decimal 10進数(0~31)
     * @return 32進のbyte。
     *         0~31以外の10進数の場合、-0x01
     */
    public static byte toSingleByte32(int decimal) {
    	if (decimal < 0 || decimal > 31) {
    		return -0x01;
    	}
    	return RADIX_CONVERT_TABLE[decimal];
    }

    /**
     * 0~zの32進byteを10進の数字に変換
     * 
     * @param byte32 0~zの32進byte
     * @return 10進の数字。
     *         0-zの32進byte以外の場合、-1
     */
    public static int toSingleDecimal(byte byte32) {
    	if (byte32 < b0 || byte32 > bv) {
    		return -1;
    	}
    	
    	int decimal = -1;
    	if (byte32 == b0) {
    		decimal = 0;
    	} else if (byte32 == b1) {
    		decimal = 1;
    	} else if (byte32 == b2) {
    		decimal = 2;
    	} else if (byte32 == b3) {
    		decimal = 3;
    	} else if (byte32 == b4) {
    		decimal = 4;
    	} else if (byte32 == b5) {
    		decimal = 5;
    	} else if (byte32 == b6) {
    		decimal = 6;
    	} else if (byte32 == b7) {
    		decimal = 7;
    	} else if (byte32 == b8) {
    		decimal = 8;
    	} else if (byte32 == b9) {
    		decimal = 9;
    	} else if (byte32 == ba) {
    		decimal = 10;
    	} else if (byte32 == bb) {
    		decimal = 11;
    	} else if (byte32 == bc) {
    		decimal = 12;
    	} else if (byte32 == bd) {
    		decimal = 13;
    	} else if (byte32 == be) {
    		decimal = 14;
    	} else if (byte32 == bf) {
    		decimal = 15;
    	} else if (byte32 == bg) {
    		decimal = 16;
    	} else if (byte32 == bh) {
    		decimal = 17;
    	} else if (byte32 == bi) {
    		decimal = 18;
    	} else if (byte32 == bj) {
    		decimal = 19;
    	} else if (byte32 == bk) {
    		decimal = 20;
    	} else if (byte32 == bl) {
    		decimal = 21;
    	} else if (byte32 == bm) {
    		decimal = 22;
    	} else if (byte32 == bn) {
    		decimal = 23;
    	} else if (byte32 == bo) {
    		decimal = 24;
    	} else if (byte32 == bp) {
    		decimal = 25;
    	} else if (byte32 == bq) {
    		decimal = 26;
    	} else if (byte32 == br) {
    		decimal = 27;
    	} else if (byte32 == bs) {
    		decimal = 28;
    	} else if (byte32 == bt) {
    		decimal = 29;
    	} else if (byte32 == bu) {
    		decimal = 30;
    	} else if (byte32 == bv) {
    		decimal = 31;
    	}
    	
    	return decimal;
    }
    
    public static void main(String[] args) {
    	// for unit test
    	long decimal;
    	long tmp;
    	byte[] byte32;
    	
    	decimal = 0;
    	byte32 = toByte32Radix(decimal, 3);
    	assert ( Bytes.equals(byte32, new byte[] {b0, b0, b0}) );

    	tmp = toDecimal(byte32);
    	assert ( decimal == tmp );

    	decimal = 48;
    	byte32 = toByte32Radix(decimal, 4);
    	assert ( Bytes.equals(byte32, new byte[] {b0, b0, b1, bg}) );
    	
    	tmp = toDecimal(byte32);
    	assert ( decimal == tmp );
    	
    	System.out.println("unit test passed");
    }
}
