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
require CONST_DIR + "ConstantsBase.rb"

#
#=コマンドを定義する
#
#
class CommandConst < ConstantsBase
  #コマンド :: set
  SET = "set";
  #コマンド :: get
  GET = "get";
  #コマンド :: put
  PUT = "put";
  #コマンド :: delete
  DELETE = "delete";
  #コマンド :: count
  COUNT = "count";
  #コマンド :: help
  HELP = "help";
  #コマンド :: exit
  EXIT = "exit";
  #
  #===条件が必須なコマンド一覧を取得
  #
  #==== retuen
  #条件が必須なコマンド一覧
  def self.getIndispensableConditionCommand()
    return [self::PUT, self::DELETE];
  end

  #
  #===指定したコマンドが条件必須かどうかを返す
  #
  #==== args
  #command :: コマンド
  #==== return
  #指定したコマンドが条件必須であればtrue、必須でなければfalse
  def self.isIndispensableConditionCommand(command)
    ret = false;
    self.getIndispensableConditionCommand().each do |iCommand|
      ret |= iCommand == command;
    end
    return ret;
  end
end
