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

require CONST_DIR + "ConstantsBase.rb"

#
#=日付フォーマットを定義する
#
#
class DateFormat < ConstantsBase
  FORMAT1 = "yyyy/MM/dd";
  FORMAT2 = "yyyyMMdd";
  FORMAT3 = "yyyy-MM-dd";
  
  #
  #===コンストラクタ
  #
  def initialize()
    @index = 0;
    @formatArray = [];
    DateFormat.constants.each do |constant|
      value = nil;
      eval("value = DateFormat::" + constant + ";");
      @formatArray << value;
    end
  end

  #
  #===次のindexの値を返す
  #indexの範囲を超えるとnil
  #
  #==== return
  #日付フォーマットを表す文字列
  def next()
    format = nil;

    begin
      format = @formatArray[@index];
      @index = @index + 1;
    rescue
      format = nil;
    end

    return format;
  end
end
