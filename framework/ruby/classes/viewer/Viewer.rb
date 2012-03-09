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
#=検索結果表示を定義する
#
#
class Viewer
  public
  #
  #===getコマンドの検索結果を表示する
  #
  #==== args
  #obj :: frameworkから返ってきた結果オブジェクト
  #query :: Queryオブジェクト
  #isScan :: 検索時にscanを呼び出したかどうか
  #columnNameArray :: 対象テーブルのカラム名一覧
  #fieldInfoList :: 対象テーブルクラスのフィールド名一覧
  #client ::
  def self.getView(obj, query, isScan, columnNameArray, fieldInfoList, client)

    count = 0;
    mrOutputPath = nil;

    eputs "-------- result --------";
    if (obj == nil)
      eputs "no data";
    else
      if (isScan)
        # scanが呼ばれた場合
        offset = query.offset();
        limit = query.limit();

        index = 1;
        if ($process_mode == ProcessMode::NORMAL)
          while (record = obj.next())
            if (offset < index)
              if (limit == -1  || index < offset + limit + 1)
                # レコードの表示
                eputs "----------------------";
                self.showRecord(record, query, columnNameArray, fieldInfoList);
                count = count + 1;
                eputs "----------------------";
              else
                break;
              end
            end

            index = index + 1;
          end
        elsif ($process_mode == ProcessMode::MAP_REDUCE)
          if (obj.size() == 0)
            eputs "no data";
          else
            count = obj.size();
            if (count == 100)
              mrOutputPath = client.getMROutputPath();
            end

            obj.each do |record|

              # レコードの表示
              eputs "----------------------";
              self.showRecord(record, query, columnNameArray, fieldInfoList);
              #                  count = count + 1;
              eputs "----------------------";

              index = index + 1;
            end

          end
        else
          # モードおかしい
          # チェック済みなのでありえないけど一応例外投げる
          raise HBaseClientException.new("(´・ω・｀)");
        end
      else
        # getが呼ばれた場合
        # レコードの表示
        self.showRecord(obj, query, columnNameArray, fieldInfoList);
        count = count + 1;
      end
    end
    eputs "-------- /result --------";
    eputs count.to_s + " result";

    unless (mrOutputPath == nil)
      eputs "検索結果が100件を超えています。100件目以降のデータは以下をご確認ください";
      eputs mrOutputPath;
    end
  end

  #
  #===countコマンドの検索結果を表示する
  #
  #==== args
  #count :: 結果のカウント
  def self.countView(count)

    eputs "-------- result --------";
    self.dispLine(CommandConst::COUNT, count, CommandConst::COUNT.length);
    eputs "-------- /result --------";
    eputs "1 result"

  end

  private

  #
  #===レコードの表示
  #
  #==== args
  #record :: frameworkから返ってきた結果オブジェクト
  #query :: Queryオブジェクト
  #columnNameArray :: 対象テーブルのカラム名一覧
  #fieldInfoList :: 対象テーブルクラスのフィールド名一覧
  def self.showRecord(record, query, columnNameArray, fieldInfoList)

    # ユーザが指定した表示カラム一覧
    showColumnArray = query.showColumnArray();
    if (showColumnArray == [])
      # 表示カラムの指定がない場合は全カラム表示
      showColumnArray = columnNameArray;
    end
    # padding用にカラム名の最大長を取得
    maxColumnNameLength = Util.arrayMaxLength(showColumnArray);

    # カラム名のリストをぐーるぐる
    showColumnArray.each do |columnName|
      value = "";
      fieldInfo = FieldInfo.getFieldInfoFromQualifier(fieldInfoList, columnName);
      fieldName = fieldInfo.javaFieldName();

      begin
        # 動的にItemRankingの変数を呼び出して値にセット
        eval("value = record." + fieldName);
        if (fieldInfo.rowkey())
          columnName = "*" + columnName;
        end
      rescue SyntaxError
        raise HBaseClientException.new("カラム名が不正です");
      end
      if ($view_mode == ViewMode::LINE)
        # 表示モード：line
        # 1件表示
        self.dispLine(columnName, value, maxColumnNameLength);
      else
        # 1項目
        self.dispLineOnTableMode(columnName, value, true);
      end
    end
  end

  #
  #===[カラム名 => 値]を表示する
  #カラム名は最長のものに合わせてpaddingされる
  #
  #==== args
  #columnName :: カラム名
  #value :: 値
  #maxColumnNameLength :: カラム名の最大長
  def self.dispLine(columnName, value, maxColumnNameLength)
    if (value.kind_of?(Java::JavaUtil::Date))
      # Date型の場合、yyyy/MM/ddに整形する
      value = Util.JavaDate2String(value, DateFormat::FORMAT1);
    elsif (value.kind_of?(Java::JavaUtil::List))
      # List型の場合、展開し、カンマつなぎの文字列にする
      tmpValue = "";

      value.each_with_index do |val, index|

        if (index == 0)
          tmpValue += "[";
        else
          tmpValue += ","
        end
        # Date型の場合、yyyy/MM/ddに整形する
        if (val.kind_of?(Java::JavaUtil::Date))
          tmpValue += Util.JavaDate2String(val, DateFormat::FORMAT1);
        elsif (val.class.name == "")
          # FIXME これで大丈夫なのかな？おそらくjRubyのバグ。プリミティブ型のclassnameとれない？
          tmpValue += org.apache.hadoop.hbase.util.Bytes.toString(val);
        else
          tmpValue += val.to_s;
        end

        if (index + 1 == value.size())
          tmpValue += "]"
        end
      end
      value = tmpValue;
    end

    eputs columnName.ljust(maxColumnNameLength + 1) + " => " + value.to_s;
  end

  def self.dispLineOnTableMode(columnName, value, withHeader)
    print "|" +  value.to_s.ljust(columnName.length);
  end
end
