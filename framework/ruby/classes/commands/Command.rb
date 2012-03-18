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
#=HBaseClientのコマンドを定義する
#
#
class Command
  #
  #===コマンドを実行する
  #
  #==== args
  #conf :: org.apache.hadoop.conf.Configuration
  #query :: Query
  #tableName :: テーブル名
  def execute(query, tableName)
    clazz = TableBase.new(tableName, query);

    operation = query.operation();

    begin
      # oprationに応じたメソッドを呼ぶ
      eval("clazz." + operation + "();");
    rescue NoMethodError => e
      # 不正なコマンドの場合、helpファイルを表示することにしたので、エラー表示はなし
      #			raise HBaseClientException.new("コマンド: " + operation + "が不正です");
    rescue SyntaxError => e
      # すでにチェックを通過してきているので実装のミスがないとありえない
      raise HBaseClientException.new("コマンド: " + operation + "が不正です");
    end
  end
end
