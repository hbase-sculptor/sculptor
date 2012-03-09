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
#=Stringクラスの拡張クラス
#
#
class String
  #
  #===文字列が指定した文字列から始まっているかチェック
  #要はJavaのstartsWithです
  #
  #==== args
  #args :: 文字列
  #==== return
  #文字列が指定した文字列から始まっていればtrue
  def startsWith(*args)
    retult = false
    for arg in args
      result |= self[0, arg.length] == arg
      break if result
    end
    result
  end

  #
  #===文字列が指定した文字列で終わっているかチェック
  #要はJavaのendsWithです
  #
  #==== args
  #args :: 文字列
  #==== return
  #文字列が指定した文字列で終わっていればtrue
  def endsWith(*args)
    retult = false
    for arg in args
      result |= self[-arg.length, arg.length] == arg
      break if result
    end
    result
  end

  #
  #===キャメルケースをスネークに変換
  #
  def toSnake()
    return self.split(/(?![a-z])(?=[A-Z])/).map{|s| s.downcase}.join('_')
  end

  #
  #===スネークをキャメルケースに変換
  #
  def toCamel()
    return self.split('_').map{|s| s.capitalize}.join('')
  end

  def encode(encode)
    require 'kconv'

    return self.kconv(encode, Kconv::UTF8);
  end
  
  def self.nvl(value, param)
    return value == nil ? param : value;
  end
end
