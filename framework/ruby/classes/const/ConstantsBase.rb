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

#
#=定数のベースクラス
#
#
class ConstantsBase
  #
  #===指定した値が定義されているかどうかを返す
  #
  #==== args
  #value :: 値
  #==== return
  #指定した値が定義されていればtrue
  def self.isDefined(value)
    ret = false;
    value.gsub!(/"/, "\\\"");
    self.constants.each do |constant|
      eval("ret |= \"" + value.to_s + "\" == " + self.to_s + "::" + constant + ".to_s");
    end
    return ret;
  end

  #
  #===定数の値一覧を返す
  #
  #==== return
  #定数の値一覧
  def self.getValues()
    ret = [];

    self.constants.each do |constant|
      eval("ret << " + self.to_s + "::" + constant + ".to_s");
    end

    return ret;
  end
end
