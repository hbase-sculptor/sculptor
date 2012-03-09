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
#=HBaseClient独自の例外を定義する
#
#
class HBaseClientException < Exception
  #独自のメッセージ
  attr_accessor :message;
  
  #
  #===コンストラクタ
  #
  #==== args
  #エラーメッセージ
  def initialize(_message)
    @message=(_message);
  end

  #
  #===独自に設定したメッセージを取得する
  #
  #==== return
  #エラーメッセージ
  def getMessage()
    return @message;
  end
end
