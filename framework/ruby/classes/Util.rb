#
# Copyright 2010 The Apache Software Foundation
#
# Licensed to the Apache Software Foundation (ASF) under one
# or more contributor license agreements.  See the NOTICE file
# distributed with this work for additional information
# regarding copyright ownership.  The ASF licenses this file
# to you under the Apache License, Version 2.0 (the
# "License"); you may not use this file except in compliance
# with the License.  You may obtain a copy of the License at
#
#     http://www.apache.org/licenses/LICENSE-2.0
#
# Unless required by applicable law or agreed to in writing, software
# distributed under the License is distributed on an "AS IS" BASIS,
# WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
# See the License for the specific language governing permissions and
# limitations under the License.
#

#
#=ユーティリティクラス
#雑多に
#
#
class Util
  #
  #===コンテンツアシストにに表示するワード一覧を取得
  #
  #====return
  #コンテンツアシストにに表示するワード一覧
  def self.getCompletionWords
    words = [
      "view",
      "process",
      "from",
      "offset",
      "limit"
    ];
    return words
  end

  #
  #===Javaのフィールド名をrubyにあわせて変換
  #キャメルでケースで定義されたフィールド名をスネークに変換
  #その際にイレギュラーにも対応する
  #
  #==== args
  #fName :: Javaのフィールド名
  #==== return
  #rubyのフィールド名
  def self.getFieldName(fName)
    fieldName = fName.toSnake();
    fieldName = fieldName.sub("_i_d", "_id");

    return fieldName;
  end

  #
  #===文字列からjava.util.Dateオブジェクトを生成
  #
  #==== args
  #yyyymmdd :: 日付文字列
  #==== return
  #指定された日付のjava.util.Dateオブジェクト
  def self.createJavaUtilDate(yyyymmdd)
    import 'java.util.Date';
    import 'java.text.SimpleDateFormat';

    date = nil;

    if (yyyymmdd != nil)
      yyyymmdd = yyyymmdd.to_s;

      msg = "";
      count = 0;

      df = DateFormat.new;
      while (dateFormat = df.next())

        if (count > 0)
          msg += ", ";
        end
        msg += dateFormat;

        begin
          sdf = SimpleDateFormat.new(dateFormat);
          date = sdf.parse(yyyymmdd);
        rescue NativeException
        else
          return date;
        end
        count = count + 1;
      end
      raise HBaseClientException.new("日付は以下のフォーマットで入力してください。" + msg);
    end
  end

  #
  #===java.util.Dateを指定したフォーマットの文字列に変換
  #
  #==== args
  #value :: java.util.Dateオブジェクト
  #format :: フォーマット文字列
  #==== return
  #指定したフォーマットの日付文字列
  def self.JavaDate2String(value, format)
    import 'java.util.Date';
    import 'java.text.SimpleDateFormat';

    date = SimpleDateFormat.new(format).format(value);

    return date;
  end

  #
  #===FixNum変換(nilチェックつき)
  #
  #==== args
  #value :: 数値文字列
  #==== return
  #FixNumに変換された文字列。文字列がnilの場合はnil
  def self.to_i_with_nil_check(value)
    ret = nil;
    if (value != nil)
      ret = value.to_i;
    end
    return ret;
  end

  #
  #===配列の要素の最大長を取得
  #
  #==== args
  #array :: 配列
  #==== return
  #配列内の要素の最大長
  def self.arrayMaxLength(array)
    maxLength = 0;
    array.each do |value|
      length = value.length;
      if (length > maxLength)
        maxLength = length;
      end
    end
    return maxLength;
  end

  #
  #===rubyオブジェクトをJavaオブジェクトに変換
  #
  #==== args
  #value :: 値
  #className :: Javaクラス名
  #==== return
  #Javaオブジェクトにキャストされた値
  def self.toJavaClass(value, className)
    ret = nil;

    case className
    when "java.util.Date"
      ret = self.createJavaUtilDate(value);
    when "int"
      ret = self.to_i_with_nil_check(value);
    when "java.lang.String"
      ret = value.to_s;
    when "byte"
      ret = self.to_i_with_nil_check(value);
    end
    return ret;
  end

  #
  #===フィールドの型を取得
  #
  #==== args
  #fieldInfoList :: 検索対象のフィールド一覧
  #fieldName :: フィールド名
  #==== return
  #指定したフィールドの型名
  def self.getType(fieldInfoList, fieldName)
    className = nil;
    fieldInfoList.each do |fieldInfo|
      if (fieldInfo.fieldName() == fieldName)
        className = fieldInfo.canonicalName();
      end
    end
    return className;
  end
end
