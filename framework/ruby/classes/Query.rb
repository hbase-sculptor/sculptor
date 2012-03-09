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

require CLAASES_DIR + "Condition.rb";

#
#=HBaseClientのクエリを定義する
#
#
class Query

  ERROR_MSG_TEMPLATE = "%s: %sが不正です";
  ERROR_NOT_EXISTS_MSG_TEMPLATE = "%s: %sは存在しません";

  #コマンド
  attr_accessor :operation;
  #表示カラム名一覧
  attr_accessor :showColumnArray;
  #from句
  attr_accessor :from;
  #テーブル名
  attr_accessor :tableName;
  #検索条件(Conditionオブジェクト)の配列
  attr_accessor :conditionArray;
  #第二コマンド
  attr_accessor :mOperation;
  #オフセット
  attr_accessor :offset;
  #リミット
  attr_accessor :limit;

  public
  #
  #===クエリをパースし、自分自身にセットする
  #
  #==== args
  #query :: クエリ文字列
  def parse(query)

    @showColumnArray = [];
    @conditionArray = [];

    # ぐーるぐる
    Query.splitRecord(query).each_with_index do |value, index|
      if (value != nil)
        if (index == 0)
          # 最初のトークン
          #operation
          @operation = value.downcase;
        else
          if (@operation == CommandConst::SET)
            if (index == 1)
              @mOperation = value.downcase;
            else
              if (@mOperation == "view")
                # viewMode切り替え
                if (ViewMode.isDefined(value.downcase))
                  $view_mode = value.downcase;
                  eputs "view mode : " + $view_mode;
                else
                  errMsg = sprintf(ERROR_MSG_TEMPLATE, "表示モード", value.downcase);
                  raise HBaseClientException.new(errMsg);
                end
              elsif (@mOperation == "process")
                # processMode切り替え
                if (ProcessMode.isDefined(value.downcase))
                  $process_mode = value.downcase;
                  eputs "process mode : " + $process_mode;
                else
                  errMsg = sprintf(ERROR_MSG_TEMPLATE, "処理モード", value.downcase);
                  raise HBaseClientException.new(errMsg);
                end
              end
            end
          elsif (@operation == CommandConst::PUT)
            # putコマンドの場合は表示カラム名とfrom句がない
            if (index == 1)
              # 2番目のトークン
              #tableName
              @tableName = value.downcase;
            else
              # テーブル名以降は「カラム名=値」
              #condition
              condition = parseCondition(value);
              unless (condition == nil)
                @conditionArray << condition;
              end
            end
          else
            if (@from == nil)
              if (value.downcase == "from")
                #from
                @from = value.downcase;
              else
                #showColumn
                @showColumnArray << value.downcase;
              end
            else
              # from句以降
              if (@tableName == nil)
                # from句の次はテーブル名
                #tableName
                @tableName = value.downcase;
              else
                # テーブル名以降は検索条件
                #condition
                condition = parseCondition(value);
                unless (condition == nil)
                  @conditionArray << condition;
                end
              end
            end
          end
        end
      end
    end

    @offset = String::nvl(@offset, 0);
    @limit = String::nvl(@limit, -1);

    # カラム名チェック
    Query.checkParam(self);
  end

  public

  #
  #===検索条件をパースし、Conditionオブジェクトにセット
  #
  #==== args
  #query :: クエリ文字列
  #==== return
  #Conditionオブジェクト
  def parseCondition(query)

    condition = nil;
    arr = query.scan(/(\S+?)([=|<|>|\s]+)(.+)/);# TODO ここの正規表現なおす

    unless (arr == nil || arr == [])
      columnName = arr[0][0].strip();
      sign = arr[0][1].strip();
      value = arr[0][2].strip();

#      p value;
#      if (value.startsWith("\"") && value.endsWith("\""))
#        value = value.slice(1..value.length - 2);
##        value = value.gsub("\"", "\\\\\\\"");
#      end
#      p value;
      if (columnName == OptionConst::OFFSET)
        @offset = value != nil ? value.to_i : nil;
      elsif (columnName == OptionConst::LIMIT)
        @limit = value != nil ? value.to_i : nil;
      else
        condition = Condition.new();
        condition.columnName = columnName;
        condition.setSign(sign);
        condition.value = value;

        if (condition.columnName() == nil || condition.sign() == nil || condition.value() == nil)
          errMsg = sprintf(ERROR_MSG_TEMPLATE, "値", query);
          raise HBaseClientException.new(errMsg);
        end
      end
    else
      errMsg = sprintf(ERROR_MSG_TEMPLATE, "値", query);
      raise HBaseClientException.new(errMsg);
    end

    return condition;
  end

  private

  #
  #===クエリの各パラメータをチェックする
  #
  #==== args
  #query Queryオブジェクト
  #==== raise
  #HBaseClientException
  def self.checkParam(query)

    # オペレーションのチェック
    operation = query.operation();
    self.checkCommand!(operation);

    # テーブル名のチェック
    tableName = query.tableName();
    self.checkTableName(tableName);

    # 表示カラム名のチェック
    query.showColumnArray().each do |fieldName|
      self.checkColumnName(tableName, fieldName);
    end

    if (CommandConst.isIndispensableConditionCommand(operation) && query.conditionArray() == [])
      errMsg = operation + "コマンドには条件を必ず指定してください";
      raise HBaseClientException.new(errMsg);
    end

    # 検索条件のカラム名のチェック
    query.conditionArray().each do |condition|
      self.checkColumnName(tableName, condition.columnName());
      self.checkSyntax(condition.value());
    end
  end

  private

  #
  #===コマンドが定義されたものかどうかチェックする
  #※破壊メソッド
  #
  #==== args
  #command :: コマンド文字列
  def self.checkCommand!(command)
    if (command != nil && !CommandConst.isDefined(command))
      #raise HBaseClientException.new("コマンド: " + command + "が不正です");
      command = CommandConst::HELP;;
    end
  end

  private

  #
  #===テーブルが存在するものかどうかチェックする
  #
  #==== args
  #tableName :: テーブル名
  #==== raise
  #HBaseClientException :: 指定されたテーブルが存在しない場合
  def self.checkTableName(tableName)
    if (tableName != nil && !$tableNameList.include?(tableName))
      errMsg = sprintf(ERROR_NOT_EXISTS_MSG_TEMPLATE, "テーブル", tableName);
      raise HBaseClientException.new(errMsg);
    end
  end

  #
  #===カラムが指定されたテーブルに存在するものかどうかチェックする
  #
  #==== args
  #tableName :: テーブル名
  #fieldName :: フィールド名
  #==== raise
  #HBaseClientException :: 指定されたカラムがテーブルに存在しない場合
  def self.checkColumnName(tableName, fieldName)
    unless (fieldName == OptionConst::OFFSET || fieldName == OptionConst::LIMIT)
      if (tableName != nil && fieldName != nil && !$fieldNameHash[tableName].include?(fieldName))
        errMsg = sprintf(ERROR_NOT_EXISTS_MSG_TEMPLATE, "カラム", tableName + "." + fieldName);
        raise HBaseClientException.new(errMsg);
      end
    end
  end

  #
  #===入力値のSyntaxをチェックする
  #
  #==== args
  #value :: 文字列
  #==== raise
  #HBaseClientException
  def self.checkSyntax(value)
    test = nil;
    begin
      # evalに食わせてSyntaxErrorがないかチェックする
      eval("test = \"" + value.to_s + "\";");
    rescue SyntaxError => e
      begin
        eval("test = " + value.to_s + ";");
      rescue SyntaxError => e
        errMsg = sprintf(ERROR_MSG_TEMPLATE, "値", value);
        raise HBaseClientException.new(errMsg);
      end
    end
  end

  #
  #===クエリ文字列を分解する
  #
  #==== args
  #message :: クエリ文字列
  #==== return
  #分解されたクエリ文字列の配列
  def self.splitRecord(message)

    records = [];

    quotStart = false;
    msg = "";

    words = message.split(/\s/);

    words.each_with_index do |value, index|
      isPut = false;

      msg += value;

      if (quotStart)# ダブルクォートの中
        if (value.index("\"") != nil)# ダブルクォートが見つかった
          isPut = true;
          quotStart = false;
        else
          msg += " ";
        end
      else
        if (value.index("\"") != nil)# ダブルクォートが見つかった
          # ↓ダブルクォートのなか
          if (value.index("\"", value.index("\"") + 1) != nil)# 二つ目のダブルクォートがみつかった
            # ダブルクォートの外
            isPut = true;
            quotStart = false;
          else
            # ダブルクォートのなか
            # splitで半角スペース一個なくなっちゃってるから足す
            msg += " ";
            quotStart = true;
          end
        else
          if (msg != "")# ダブルクォートの外の半角スペースはひとつにまとめる
            isPut = true;
          else
            msg = "";
          end
        end
      end

      # 最後の要素は無条件で追加
      if (isPut || index + 1 == words.length())
        records << msg;
        msg = "";
      end
    end

    return records;
  end
end

