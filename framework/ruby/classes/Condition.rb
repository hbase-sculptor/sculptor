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
#=検索条件を定義する
#
#
class Condition
  #対象のカラム名
  attr_accessor :columnName;
  #関係演算子
  attr_accessor :sign;
  #値
  attr_accessor :value;
  #
  #===関係演算子をセットする
  #
  #==== args
  #signEnum :: sculptor.framework.HCompareOp
  def setSign(signEnum)
    @sign = nil;
    case signEnum
    when "<"
      @sign = HCompareOp::LESS;
    when "<="
      @sign = HCompareOp::LESS_OR_EQUAL;
    when "="
      @sign = HCompareOp::EQUAL;
    when ">="
      @sign = HCompareOp::GREATER_OR_EQUAL;
    when ">"
      @sign = HCompareOp::GREATER;
    end
  end

  #
  #===文字列から関係演算子Enumを取得
  #
  #==== args
  #sign :: 関係演算子を表す文字列(<|<=|=|>=|>)
  #==== return
  #signEnum :: sculptor.framework.HCompareOp
  def self.getEnum(sign)
    sign = nil;
    case sign
    when "<"
      sign = HCompareOp::LESS;
    when "<="
      sign = HCompareOp::LESS_OR_EQUAL;
    when "="
      sign = HCompareOp::EQUAL;
    when ">="
      sign = HCompareOp::GREATER_OR_EQUAL;
    when ">"
      sign = HCompareOp::GREATER;
    end

    return sign;
  end

  #
  #===2値を比較する
  #
  #==== args
  #value1 :: 値
  #value2 :: 比較したい値
  #signEnum :: sculptor.framework.HCompareOp
  #==== return
  #比較結果が正ならtrue
  def self.compare(value1, value2, sign)
    ret = false;

    case sign
    when HCompareOp::LESS
      ret = value1.to_i < value2.to_i;
    when HCompareOp::LESS_OR_EQUAL
      ret = value1.to_i <= value2.to_i;
    when HCompareOp::EQUAL
      ret = value1 == value2;
    when HCompareOp::GREATER_OR_EQUAL
      ret = value1.to_i >= value2.to_i;
    when HCompareOp::GREATER
      ret = value1.to_i < value2.to_i;
    end

    return ret;
  end
end
