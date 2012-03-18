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

#Sculptorのメインスクリプト
#
require 'etc'
require 'pp'

$userName = Etc.getlogin;

$title = <<"EOS"
+--------------------------------------------------+
|                 Sculptor                         |
|                 var 0.0.1                        |дﾟ)
+--------------------------------------------------+
EOS

#このスクリプトのベースディレクトリ
$basePath =  File::dirname(__FILE__);

# sculptor root
$SCULPTOR_ROOT = $basePath + '/../..'

# 初期設定
require $basePath + '/classes/Initialize.rb'

CommandHistory::init($tableNameList, $fieldNameList)

operation = "";

# コマンドヒストリーを読み込む
CommandHistory::readHistoryFile($userName);

while (true)
  begin
    # ユーザの入力を取得
    # -eの場合は引数から値をとる
        if(ARGV[0]) then
                readCommand = ARGV.join(" ");
        else
                readCommand = CommandHistory::readCommand();
        end
    # クエリをパース
    query = Query.new();
    query.parse(readCommand);

    operation = query.operation();
    unless (operation == nil)
      if (operation == CommandConst::EXIT)
        # exitコマンドで終了
        # コマンドヒストリーをシリアライズ
        CommandHistory::writeCommandHistory($userName);
        break;
      elsif (operation== CommandConst::HELP || !CommandConst.isDefined(operation))
        # 不正なコマンドはコマンド履歴に残さない
        if (!CommandConst.isDefined(operation))
          CommandHistory::ignore();
        end

        # helpファイル表示
        open(DOC_DIR + "help.txt") do |file|
          while (line = file.gets)
            eputs "\t" + line;
          end
        end
      end

      if (operation != CommandConst::SET && query.tableName() != nil)
        # 呼び出すコマンドクラスの名前を生成
        #className = query.operation().downcase.gsub(/\b\w/) { |word| word.upcase };

        # 呼び出すテーブルクラスの名前を生成
        tableName = query.tableName().downcase;

        obj = Command.new();
        obj.execute(query, tableName);
      end
    end
  rescue SyntaxError => e
    eputs "Command syntax error.";
  rescue => e
    pp e;
    pp e.backtrace
  end
  #-eならばループしない
          if(ARGV[0]) then
                break;
        end
end

eputs "|дﾟ)ﾉｼ < bye!";
