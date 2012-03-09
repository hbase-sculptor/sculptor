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

require CONST_DIR + 'OSConst.rb'

#
#===出力を定義するモジュール
#
module Out
  $os = OSConst::getOsName();
  $encode = OSConst::getDefaultEncode($os);
  
  #
  #===putsのエンコード機能付き
  #
  #==== args
  #文字列
  def eputs(value)
    puts value.to_s.encode($encode);
  end
end
