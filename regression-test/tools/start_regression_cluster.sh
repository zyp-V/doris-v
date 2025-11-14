#!/usr/bin/env bash
# Licensed to the Apache Software Foundation (ASF) under one
# or more contributor license agreements.  See the NOTICE file
# distributed with this work for additional information
# regarding copyright ownership.  The ASF licenses this file
# to you under the Apache License, Version 2.0 (the
# "License"); you may not use this file except in compliance
# with the License.  You may obtain a copy of the License at
#
#   http://www.apache.org/licenses/LICENSE-2.0
#
# Unless required by applicable law or agreed to in writing,
# software distributed under the License is distributed on an
# "AS IS" BASIS, WITHOUT WARRANTIES OR CONDITIONS OF ANY
# KIND, either express or implied.  See the License for the
# specific language governing permissions and limitations
# under the License.


CLUSTER='regression_test'

DORIS_HOME='/root/doris/output'
REGRESSION_HOME='/root/doris/regression-test'
DORIS_FE_HOME=$DORIS_HOME/fe
DORIS_BE_HOME=$DORIS_HOME/be
START_FE_SH=$DORIS_FE_HOME/bin/start_regression_fe.sh
START_BE_SH=$DORIS_BE_HOME/bin/start_regression_be.sh


cd "$DORIS_HOME" && echo -e "\n\nEnter `pwd`."

# Check fe and be are compiled ?
if [ ! -d $DORIS_FE_HOME ] || [ ! -d $DORIS_BE_HOME ]; then
  echo "Error: please compile doris first."
  exit 1
fi

# Install dependent packages.
echo -e 'Install dependent packages.\n'
apt-get update && apt-get install -y --allow-unauthenticated bvc
bvc clone -f jdk /opt/tiger/jdk
bvc clone -f yarn_deploy /opt/tiger/yarn_deploy && /opt/tiger/yarn_deploy/hadoop/bin/hdfs dfs -ls
bvc clone -f dp/hive_deploy /opt/tiger/hive_deploy
apt-get update && apt-get install -y --allow-unauthenticated default-mysql-client

# Download dependent thirdparty repositories and data. 
echo -e '\n\nDownload dependent thirdparty repositories and data.\n'
cd "$REGRESSION_HOME/tools" && echo "Enter `pwd`."
wget -qO - http://maven.byted.org/assets/install_settings.sh | bash -s
wget https://tosv.byted.org/obj/doris-regression-test-cache/regressionCacheDataV2.tar.gz && tar -xzf regressionCacheDataV2.tar.gz && rm regressionCacheDataV2.tar.gz
cd ~/.m2/ && rm -r repository && wget https://tosv.byted.org/obj/doris-regression-test-cache/repositoryV2.tar.gz && tar -xzf repositoryV2.tar.gz && rm repositoryV2.tar.gz

# Remove dirty data. 
rm -rf $DORIS_BE_HOME/conf/$CLUSTER && rm -rf $DORIS_FE_HOME/conf/$CLUSTER
rm -rf $DORIS_BE_HOME/storage && rm -rf $DORIS_FE_HOME/doris-meta
rm $DORIS_BE_HOME/bin/be.pid && rm $DORIS_FE_HOME/bin/fe.pid
rm $START_BE_SH && rm $START_FE_SH

# Install fe/be startup shell and conf.
echo -e '\n\nInstall fe/be startup shell and conf.\n'

mkdir -p $DORIS_FE_HOME/conf/$CLUSTER && mkdir -p $DORIS_BE_HOME/conf/$CLUSTER
ln -s $REGRESSION_HOME/tools/conf/be.conf $DORIS_BE_HOME/conf/$CLUSTER/be.conf
ln -s $REGRESSION_HOME/tools/conf/fe.conf $DORIS_FE_HOME/conf/$CLUSTER/fe.conf
ln -s $REGRESSION_HOME/tools/script/start_be.sh $START_BE_SH
ln -s $REGRESSION_HOME/tools/script/start_fe.sh $START_FE_SH

# Start regression_test cluster
cd "$DORIS_HOME" && echo -e "\n\nEnter `pwd`.\nBegin start BE.\n"
bash $START_BE_SH --daemon --cluster $CLUSTER && sleep 45s && tail $DORIS_BE_HOME/log/be.INFO

echo -e "\n\nBegin Start FE.\n"
bash $START_FE_SH --daemon --cluster $CLUSTER && sleep 30s && tail $DORIS_FE_HOME/log/fe.log

mysql -h 127.0.0.1 -P 9030 -u root -c -A -e 'ALTER SYSTEM ADD BACKEND "127.0.0.1:9050";'
mysql -h 127.0.0.1 -P 9030 -u root -c -A -e "SET PROPERTY FOR 'root' 'max_user_connections'='1000';"
sleep 30s # waiting cluster ready.

echo -e '\n\n regression_test cluster infos.\n'
mysql -h 127.0.0.1 -P 9030 -u root -c -A -e 'SHOW FRONTENDS;'
mysql -h 127.0.0.1 -P 9030 -u root -c -A -e 'SHOW BACKENDS;'
