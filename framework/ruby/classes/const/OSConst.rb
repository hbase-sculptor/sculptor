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

import APACHE_PACKAGE + 'commons.lang.SystemUtils';

#
#=OSの種類を定義する
#
#
class OSConst < ConstantsBase
  #windows XP
  WINDOWS_XP = "windows";
  #Linux
  LINUX = "linux";
  #
  #===実行環境のOS名を取得する
  #
  #==== return
  #OS名
  def self.getOsName()
    return SystemUtils::OS_NAME.downcase;
  end

  #
  #===Windowsかどうか
  #
  #==== args
  #os :: OSConst
  #==== return
  #windowsならtrue
  def self.isWindows(os)
    return os.downcase.startsWith(OSConst::WINDOWS_XP);
  end

  #
  #===Linuxかどうか
  #
  #==== args
  #os :: OSConst
  #==== return
  #linuxならtrue
  def self.isLunux(os)
    return os.downcase == OSConst::LINUX;
  end

  #
  #===OSのデフォルトエンコードを取得
  # TODO ちゃんとつくり直す
  #
  #==== args
  #os :: OSConst
  #==== return
  #エンコード
  def self.getDefaultEncode(os)
    encode = EncodeConst::UTF8;
    if (OSConst::isWindows(os))
      encode = EncodeConst::SJIS;
    end
    return encode;
  end
end
