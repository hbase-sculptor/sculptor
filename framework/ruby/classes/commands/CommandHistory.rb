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

require 'readline'
require 'pathname'

#
#=コマンド履歴を定義する
#
#
class CommandHistory

  #コマンド履歴を保存するファイル
  COMMAND_HISTORY_FILE = ".readline.history";
  #コマンド履歴ファイルのディレクトリ
  COMMAND_HISTORY_DIR = ".sculptor/";
  #
  #===Readlineを初期化する
  #
  #==== args
  #*list :: (可変長)コマンド補完リストに追加する文字列の配列
  def self.init(*lists)
    # コンソールの補完リスト
    words = Util.getCompletionWords();
    # コマンド一覧
    words = words.concat(CommandConst.getValues());
    # オプション一覧
    words = words.concat(OptionConst.getValues());
    # 表示モード一覧
    words = words.concat(ViewMode.getValues());
    # 処理モード一覧
    words = words.concat(ProcessMode.getValues());

    lists.each do |list|
      words = words.concat(list);
    end

    words = words.uniq();

    Readline.completion_proc = proc {|word|
      words.grep(/\A#{Regexp.quote word}/)
    }
  end

  #
  #===コンソールからユーザの入力を読み込む
  #==== return
  #ユーザの入力
  def self.readCommand()
    return Readline.readline("sculptor> ", true);
  end

  #
  #===コマンド履歴をファイルから読み込む
  #
  #==== args
  #userName :: ログインユーザ名
  def self.readHistoryFile(userName)

    userHomeDir = self.getHomeDir(userName);
    userCommandHistoryFile = userHomeDir + COMMAND_HISTORY_DIR + COMMAND_HISTORY_FILE;

    if (File.exist?(userCommandHistoryFile))
      eputs "コマンド履歴を読み込んでいます...";
      commandHistoryArray = [];
      Pathname.new(userCommandHistoryFile).open("rb") do |f|
        commandHistoryArray = Marshal.load(f);
      end

      commandHistoryArray.each do |commandHistory|
        Readline::HISTORY.push(commandHistory);
      end
    end
  end

  #
  #===コマンド履歴をファイルに書きこむ
  #
  #==== args
  #userName :: ログインユーザ名
  def self.writeCommandHistory(userName)

    userHomeDir = self.getHomeDir(userName);
    userCommandHitoryDir = userHomeDir + COMMAND_HISTORY_DIR;
    if (!FileTest::directory?(userCommandHitoryDir))
      Dir::mkdir(userCommandHitoryDir, 0777);
    end

    userCommandHistoryFile = userCommandHitoryDir + COMMAND_HISTORY_FILE;

    Pathname.new(userCommandHistoryFile).open("wb") do |f|
      Marshal.dump(Readline::HISTORY.to_a, f, 100);
    end
  end

  #
  #===最後に入力されたコマンドを履歴から削除する
  #
  def self.ignore()
    Readline::HISTORY.pop;
  end

  private

  #
  #===ユーザのホームディレクトリを取得する
  #
  #==== args
  #userName :: ログインユーザ名
  def self.getHomeDir(userName)
    return "/home/" + userName + "/";
  end
end
