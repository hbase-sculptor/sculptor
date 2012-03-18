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

#require 'singleton'
require CLAASES_DIR + "viewer/Viewer.rb";

require 'pp'

#
#=データアクセスクラス
#
#
class TableBase
  #    include Singleton

  #テーブルのカラム名一覧
  @columnNameArray = [];
  @classInfo = nil;
  @conditionHash = nil;

  @query = nil;

  @tableClassName = "";

  public
  #
  #===コンストラクタ
  #
  def initialize(tableClassName, query)

    @query = query;
    @conditionHash = Hash.new;

    @tableClassName = tableClassName;

    # 結果データを保持するJavaクラス
    begin
      dataClass = Util.getDataClassName(@tableClassName);
      import DATASTORE_PACKAGE + "." + dataClass;
      import DATASTORE_PACKAGE + "." + tableClassName;
      @dataCls = self.class.const_get(dataClass).new();

      clazz = @dataCls.getClassInfo();

      @classInfo = ClassInfo.new(clazz);
      @columnNameArray = @classInfo.columnNameArray();
    rescue => exception
      pp exception
      raise HBaseClientException.new("テーブル名が不正です");
    end
  end

  #
  #===apiのデータアクセスクラスを呼び出す
  #
  #==== args
  #conf :: org.apache.hadoop.conf.Configuration
  #==== return
  #データアクセスクラス
  def getInstance()
    #		if (@client == nil)
    # new connection"
    @client = self.class.const_get(@tableClassName).new();
    #		else
    #			# pooled connection
    #		end
    return @client;
  end

  #
  #===テーブルのカラム名一覧を取得
  #
  #==== return
  #テーブルのカラム名一覧
  def TableBase.GetColumnNameArray()
    return @columnNameArray;
  end

  def get()
    @client = self.getInstance();

    setGetCondition();

    isScan = validRowKey();

    object = executeGet(isScan);

    Viewer.getView(object, @query, isScan, @columnNameArray, @classInfo.fieldInfoList(), @client);

    self.clearCondition();
  end

  def count()
    @client = self.getInstance();

    setGetCondition();

    isScan = validRowKey();

    count = 0;

    if (isScan)
      # scanが呼ばれた場合
      if ($process_mode == ProcessMode::NORMAL)
        object = executeGet(isScan);
        if (object != nil)
          while (object.next())
            count = count + 1;
          end
        end
      elsif ($process_mode == ProcessMode::MAP_REDUCE)
        # 検索条件セット
        setCondition();
        count = @client.countMR(@dataCls, @conditionHash);
      else
        # モードおかしい
        # チェック済みなのでありえないけど一応例外投げる
        raise HBaseClientException.new("(´・ω・｀)");
      end
    else
      # getが呼ばれた場合
      object = executeGet(isScan);
      if (object != nil)
        count = count + 1;
      end
    end

    Viewer.countView(count);

    self.clearCondition();
  end

  #
  #===指定したレコードを削除する
  #
  #==== args
  #conf ::org.apache.hadoop.conf.Configuration
  def delete()

    @client = self.getInstance();

    setGetCondition();

    isScan = validRowKey();
    # まずは条件で検索する
    object = executeGet(isScan);

    # 検索結果からrowKeyを取得し、そのrowKeyでレコードを削除する
    if (isScan)
      # scanの場合
      while (record = object.next())
        rowKey = record.getRowkey();
        @client.delete(rowKey);
      end
    else
      # getの場合
      rowKey = object.getRowkey();
      @client.delete(rowKey);
    end

    self.clearCondition();
  end

  #
  #===指定したレコードを挿入/更新する
  #
  #==== args
  #conf ::org.apache.hadoop.conf.Configuration
  #====raise
  #HBaseClientException :: rowKeyが指定されていない、または不正なクエリが入力された場合
  def put()

    setGetCondition();

    if (validRowKey())
      errMgs = "";
      count = 0;
      @classInfo.fieldInfoList().each do |fieldInfo|
        if (fieldInfo.rowkey)
          condition = @conditionHash[fieldInfo.qualifier()];
          if (condition == nil)
            if (count > 0)
              errMgs += ", ";
            end
            errMgs += fieldInfo.qualifier();
            count = count + 1;
          end
        end
      end
      raise HBaseClientException.new("条件: " + errMgs + "は必須項目です");
    end

    @client = self.getInstance();

    @query.conditionArray().each do |condition|

      unless (condition.sign() == HCompareOp::EQUAL)
        raise HBaseClientException.new("検索条件に不正な文字列があります");
      end

      fieldInfo = FieldInfo.getFieldInfoFromQualifier(@classInfo.fieldInfoList(), condition.columnName().downcase);
      fName = fieldInfo.javaFieldName();

      
      value = condition.value();
      if (value.startsWith("\"") && value.endsWith("\""))
        value = value.slice(1..value.length - 2);

      end

      className = Util.getType(@classInfo.fieldInfoList(), condition.columnName().downcase);

      eval("@dataCls." + fName + " = Util.toJavaClass(value, className);");
    end

    @client.put(@dataCls);

    self.clearCondition();
  end

  private

  #
  #===getの検索条件をセットする
  #
  #==== raise
  #HBaseClientException
  def setGetCondition()

    @query.conditionArray().each do |condition|

      begin
        # 検索条件をセット
        eval("@" + condition.columnName().downcase + " = \"" + condition.value() + "\";");
      rescue SyntaxError
        begin
          eval("@" + condition.columnName().downcase + " = " + condition.value() + ";");
        rescue SyntaxError
          raise HBaseClientException.new("検索条件に不正な文字列があります");
        end
      end
      @conditionHash[condition.columnName().downcase] = condition.sign();
    end
  end

  def setCondition()
    # 検索条件セット
    @classInfo.fieldInfoList().each do |fieldInfo|
      className =  fieldInfo.canonicalName();
      javaFieldName = fieldInfo.javaFieldName();
      fieldName = fieldInfo.fieldName();
      unless (fieldName == nil)
        eval("@dataCls." + javaFieldName + " = Util.toJavaClass(@" + fieldName + ", className);");
      end
    end
  end

  #
  #===検索を実行する
  #
  #==== args
  #isScan :: apiのscanメソッドを呼ぶがどうか
  def executeGet(isScan)
    # 検索条件セット
    setCondition();

    return isScan ? scan() : getAndsearch();
  end

  def scan()
    object = nil;
    if ($process_mode == ProcessMode::NORMAL)
      # nomalモードでは普通のscan
      object = @client.scan(@dataCls, @conditionHash);
    elsif ($process_mode == ProcessMode::MAP_REDUCE)
      # mapreduceモードではscanMR
      offset = @query.offset();
      limit = @query.limit();
      object = @client.scanMR(@dataCls, @conditionHash, offset, limit);
    else
      # モードおかしい
      # チェック済みなのでありえないけど一応例外投げる
      raise HBaseClientException.new("(´・ω・｀)");
    end

    return object;
  end

  #
  #===getでレコードを取得し、それに対して他の検索条件を当てていく
  #
  #==== return
  #結果オブジェクト
  def getAndsearch()
    object = @client.get(@dataCls);

    unless (object == nil)
      # rowKey以外の項目を当てていく
      @conditionHash.each do |key, value|
        # 検索条件で指定したカラム名を元にJavaオブジェクトのフィールド名を取得
        fieldInfo = FieldInfo.getFieldInfoFromQualifier(@classInfo.fieldInfoList(), key);
        fieldName = fieldInfo.javaFieldName();

        # 検索条件で指定したカラムに対応した検索結果
        resultValue = nil;
        eval("resultValue = object." + fieldName + ";");

        # 検索条件で指定した値
        searchValue = nil;
        # Javaオブジェクトの型を取得
        className = fieldInfo.canonicalName();

        eval("searchValue = Util.toJavaClass(@" + key + ", className);");

        # 検索条件を照らし合わせ、条件に合わない項目があれば検索結果を削除
        unless (Condition.compare(resultValue, searchValue, value))
          object = nil;
          # 1件でもあればおしまい
          break;
        end
      end
    end

    return object;
  end

  #
  #===検索条件をクリアする
  #
  def clearCondition()
    @classInfo.fieldInfoList().each do |fieldInfo|
      eval("@" + fieldInfo.fieldName() + " = nil");
    end

    dataClass = Util.getDataClassName(@tableClassName);
    @dataCls = self.class.const_get(dataClass).new();

  end

  #
  #===rowkey項目が全て揃っているかどうかチェック
  #
  #==== return
  #scanを呼ぶかどうか
  def validRowKey()

    isScan = false
    isChecked = false;
    @classInfo.fieldInfoList().each do |fieldInfo|
      if (fieldInfo.rowkey)
        isChecked = true;
        isScan |= @conditionHash[fieldInfo.qualifier()] != HCompareOp::EQUAL;
      end
    end

    if (!isChecked)
      isScan = true;
    end

    return isScan;
  end
end

#
#===クラスの情報を保持する
#
#
class ClassInfo
  #クラス名
  attr_accessor :className;
  #クラスが表すテーブル
  attr_accessor :table;
  #クラスのフィールド一覧<FieldInfo>
  attr_accessor :fieldInfoList;
  #クラスが表すテーブルのカラム名一覧
  attr_accessor :columnNameArray;
  #
  #===コンストラクタ
  #
  def initialize(hClassDescriptor)
    @className=(hClassDescriptor.getEntityClassName());
    @table=(hClassDescriptor.getTable());

    fields = hClassDescriptor.gethFieldDescriptors();

    @columnNameArray = [];
    @fieldInfoList = [];
    fields.to_array.each_with_index do |hFieldDescriptor, index|

      javaFieldName = hFieldDescriptor.getFieldName();
      fieldName = hFieldDescriptor.getQualifier();
      family = hFieldDescriptor.getFamily();
      qualifier = hFieldDescriptor.getQualifier();
      isRowkey = hFieldDescriptor.isRowkey().to_s;
      canonicalName = hFieldDescriptor. getCanonicalName();

      fieldInfo = FieldInfo.new(javaFieldName, fieldName, family, qualifier, isRowkey, canonicalName);

      unless (fieldInfo.fieldName() == nil)
        @columnNameArray << fieldName;
        @fieldInfoList << fieldInfo;
      end
    end
  end
end

#
#===フィールドの情報を保持する
#
#
class FieldInfo
  #Javaクラスのフィールド名
  attr_accessor :javaFieldName;
  #フィールド名
  attr_accessor :fieldName;
  #データファミリー
  attr_accessor :family;
  #このフィールドを表す
  attr_accessor  :qualifier;
  #このフィールドがrowKeyかどうか
  attr_accessor :rowkey;
  #このフィールドのJava型名(フル)
  attr_accessor :canonicalName;
  #
  #===コンストラクタ
  #
  def initialize(_javaFieldName, _fieldName, _family, _qualifier, _isRowkey, _canonicalName)
    @javaFieldName=(_javaFieldName);
    @fieldName=(_fieldName);
    @family=(_family);
    @qualifier=(_qualifier);
    @rowkey=(_isRowkey.to_s == "true");
    @canonicalName=(_canonicalName);
  end

  #
  #===qualifierから対応するフィールド名を取得する
  #==== args
  #fieldInfoList :: FieldInfoの配列
  #qualifier :: qualifier
  #==== return
  #フィールド名
  def self.getJavaFieldNameFromQualifier(fieldInfoList, qualifier)
    retName = "";
    fieldInfoList.each do |fieldInfo|
      if (fieldInfo.qualifier() == qualifier)
        retName = fieldInfo.javaFieldName();
        break;
      end
    end
    return retName;
  end

  #
  #===qualifierから対応するフィールドの型を取得する
  #
  #==== args
  #fieldInfoList :: FieldInfoの配列
  #qualifier :: qualifier
  #==== return
  #フィールドの型名
  def self.getCanonicalNameFromQualifier(fieldInfoList, qualifier)
    retName = "";
    fieldInfoList.each do |fieldInfo|
      if (fieldInfo.qualifier() == qualifier)
        retName = fieldInfo.canonicalName();
        break;
      end
    end
    return retName;
  end

  #
  #===qualifierからそのフィールドがrowkeyかどうかを取得する
  #==== args
  #fieldInfoList :: FieldInfoの配列
  #qualifier :: qualifier
  #==== return
  #rowkeyかどうか
  def self.getRowkeyFromQualifier(fieldInfoList, qualifier)
    ret = false;
    fieldInfoList.each do |fieldInfo|
      if (fieldInfo.qualifier() == qualifier)
        ret = fieldInfo.rowkey();
        break;
      end
    end
    return ret;
  end

  #
  #===qualifierからフィールドオブジェクトを取得する
  #==== args
  #fieldInfoList :: FieldInfoの配列
  #qualifier :: qualifier
  #==== return
  #FieldInfo
  def self.getFieldInfoFromQualifier(fieldInfoList, qualifier)
    ret = false;
    fieldInfoList.each do |fieldInfo|
      if (fieldInfo.qualifier() == qualifier)
        ret = fieldInfo;
        break;
      end
    end
    return ret;
  end
end
