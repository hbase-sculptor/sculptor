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

require 'java'
require 'pp'

#libディレクトリのパス
LIB_DIR = $basePath + "/../../lib/";
#classesディレクトリのパス
CLAASES_DIR = $basePath + "/classes/";
#constディレクトリのパス
CONST_DIR = CLAASES_DIR + "const/";
#modulesディレクトリのパス
MODULES_DIR = $basePath + "/modules/";
#docディレクトリのパス
DOC_DIR = $basePath + "/doc/";

#apacheパッケージ
APACHE_PACKAGE = "org.apache.";
#data store package
#TODO should read tables automatically
DATASTORE_PACKAGE = "sculptor.sample";

require CLAASES_DIR + 'common/String.rb'
require CLAASES_DIR + 'commands/CommandHistory.rb';

sculptorJar = "";
#LIB_DIR直下のjarファイルをぜんぶrequire
Dir::entries(LIB_DIR).each do |jarFile|
  if (jarFile.endsWith(".jar"))
    require LIB_DIR + jarFile;
  end
end

# require sculptor.jar
require $basePath + "/../../sculptor-0.0.1.jar"

# TODO read from modules
sculptorJar = $basePath + "/../../sculptor-sample-0.0.1.jar"
require sculptorJar

require CLAASES_DIR + 'Util.rb'
require CLAASES_DIR + 'Query.rb'
require CONST_DIR + 'ViewMode.rb'
require CONST_DIR + 'ProcessMode.rb'
require CONST_DIR + 'CommandConst.rb'
require CONST_DIR + 'OptionConst.rb'
require CONST_DIR + 'EncodeConst.rb'
require CONST_DIR + 'DateFormat.rb'
require CLAASES_DIR + 'exception/HBaseClientException.rb'
require CLAASES_DIR + 'commands/Command.rb'
require CLAASES_DIR + 'tables/TableBase.rb'
require MODULES_DIR + 'Out.rb'

include Out;

eputs $title;

import 'sculptor.framework.HCompareOp'
import 'sculptor.framework.util.ClassUtils'

tableClassNameList = ClassUtils.getJavaClasses(DATASTORE_PACKAGE, sculptorJar);
eputs "テーブルを読み込んでいます...";

#できるだけグローバルなんは避けたいけど、とりあえず
# 表示モード。デフォルトはline
$view_mode = ViewMode::LINE;
# 処理モード。デフォルトはnomal
$process_mode = ProcessMode::NORMAL;

log_level = org.apache.log4j.Level::ERROR
org.apache.log4j.Logger.getLogger("org.apache.zookeeper").setLevel(log_level)
org.apache.log4j.Logger.getLogger("org.apache.hadoop").setLevel(log_level)
org.apache.log4j.Logger.getLogger("sculptor").setLevel(log_level)

$tableNameList = [];
$fieldNameList = [];
$fieldNameHash = Hash.new;
tableClassNameList.each do |tableClassName|

  begin
    tClassName = tableClassName.sub(DATASTORE_PACKAGE + ".", "");

    unless (tClassName.startsWith("H"))
      import DATASTORE_PACKAGE + "." + tClassName;
      tbObj = self.class.const_get(tClassName).new();
      tableClassInfo = tbObj.getClassInfo();

      tableName = tableClassInfo.getTable();
      puts tableName;

      $tableNameList << tableName;

      tableClassFieldInfoList = tableClassInfo.gethFieldDescriptors();
      tableClassFieldInfoList.each do |tableClassFieldInfo|
        tableClassFieldName = tableClassFieldInfo.getQualifier();
        $fieldNameList << tableClassFieldName;
      end
      $fieldNameHash[tableName] = $fieldNameList;
    end
  rescue NativeException => e
            pp e;
  rescue NoMethodError => e
            pp e;
  rescue ArgumentError => e
            pp e;
  rescue NameError => e
            pp e.backtrace;
  rescue TypeError => e
            pp e;
  rescue
  end
end
$tableNameList = $tableNameList.uniq();
