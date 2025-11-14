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

# Avoid setup failure due to trivial ERROR in EMR. Optimize in the future
# set -eo pipefail

curdir="$(cd "$(dirname "${BASH_SOURCE[0]}")" &>/dev/null && pwd)"

if [[ "$(uname -s)" == 'Darwin' ]] && command -v brew &>/dev/null; then
    PATH="$(brew --prefix)/opt/gnu-getopt/bin:${PATH}"
    export PATH
fi

OPTS="$(getopt \
    -n "$0" \
    -o '' \
    -l 'daemon' \
    -l 'helper:' \
    -l 'image:' \
    -l 'version' \
    -l 'metadata_failure_recovery' \
    -l 'console' \
    -l 'fe_dc:' \
    -l 'cluster:' \
    -l 'meta_vol:' \
    -l 'fe_psm_prefix:' \
    -l 'fe_origin_psm:' \
    -- "$@")"

eval set -- "${OPTS}"

RUN_DAEMON=0
RUN_CONSOLE=0
HELPER=''
IMAGE_PATH=''
IMAGE_TOOL=''
OPT_VERSION=''
METADATA_FAILURE_RECOVERY=''
FE_PSM_PREFIX=
FE_ORIGIN_PSM=
FE_DC=
META_VOL=data00
EDIT_LOG_PORT=9010

while true; do
    case "$1" in
    --daemon)
        RUN_DAEMON=1
        shift
        ;;
    --console)
        RUN_CONSOLE=1
        shift
        ;;
    --version)
        OPT_VERSION="--version"
        shift
        ;;
    --metadata_failure_recovery)
        METADATA_FAILURE_RECOVERY="-r"
        shift
        ;;
    --helper)
        HELPER="$2"
        shift 2
        ;;
    --image)
        IMAGE_TOOL=1
        IMAGE_PATH="$2"
        shift 2
        ;;
    --cluster) 
	CLUSTER=$2
	shift 2 
	;;
    --enable_ipv6)
	shift 2
	;;
    --fe_dc) 
        FE_DC=$2
        shift 2 
        ;;
    --meta_vol)
        META_VOL=$2
        shift 2
        ;;
    --fe_psm_prefix) 
        FE_PSM_PREFIX=$2
        shift 2
        ;;
    --fe_origin_psm)
        FE_ORIGIN_PSM=$2
        shift 2
        ;;
    --)
        shift
        break
        ;;
    *)
        echo "Internal error"
        exit 1
        ;;
    esac
done

if [[ ${FE_PSM_PREFIX} == "" ]]; then
    FE_PSM_PREFIX="olap.doris"
fi

if [ ${FE_PSM_PREFIX} != "inf.compute" ] && [ ${FE_PSM_PREFIX} != "olap.doris" ];then
    echo "Internal error, fe_psm_prefix illegal: '${FE_PSM_PREFIX}'"
    exit 1
fi

FE_PSM_PREFIX="${FE_PSM_PREFIX}.${CLUSTER}"
echo "fe psm: ${FE_PSM_PREFIX}"
echo "fe origin psm: ${FE_ORIGIN_PSM}"
export TCE_PSM="${FE_PSM_PREFIX}_fe"


DORIS_HOME="$(
    cd "${curdir}/.."
    pwd
)"
export DORIS_HOME
export DORIS_CLUSTER=$CLUSTER

FE_METADATA_DIR=${DORIS_HOME}/doris-meta # default meta folder

# process --helper param
function append_helper(){
    fe_service_name=$FE_PSM_PREFIX"_mysql.service."$FE_DC
    # 双栈只会获得v4的地址
    fe_list=`/opt/tiger/consul_deploy/bin/go/sd --addr-family=dual-stack lookup $fe_service_name`
    list_length=0
    for fe_item in $fe_list
    do
        host_v4=`echo $fe_item | grep '^\([1-9]\|[1-9][0-9]\|1[0-9][0-9]\|22[0-4][0-9]\|25[0-5]\)\.\([0-9]\|[1-9][0-9]\|1[0-9][0-9]\|2[0-4][0-9]\|25[[0-5]\)\.\([0-9]\|[1-9][0-9]\|1[0-9][0-9]\|2[0-4][0-9]\|25[0-5]\)\.\([0-9]]\|[1-9][0-9]\|1[0-9][0-9]\|2[0-4][0-9]\|25[0-5]\)$'`
        host_v6=`echo $fe_item | egrep '^(([0-9a-fA-F]{1,4}:){7,7}[0-9a-fA-F]{1,4}|([0-9a-fA-F]{1,4}:){1,7}:|([0-9a-fA-F]{1,4}:){1,6}:[0-9a-fA-F]{1,4}|([0-9a-fA-F]{1,4}:){1,5}(:[0-9a-fA-F]{1,4}){1,2}|([0-9a-fA-F]{1,4}:){1,4}(:[0-9a-fA-F]{1,4}){1,3}|([0-9a-fA-F]{1,4}:){1,3}(:[0-9a-fA-F]{1,4}){1,4}|([0-9a-fA-F]{1,4}:){1,2}(:[0-9a-fA-F]{1,4}){1,5}|[0-9a-fA-F]{1,4}:((:[0-9a-fA-F]{1,4}){1,6})|:((:[0-9a-fA-F]{1,4}){1,7}|:)|fe80:(:[0-9a-fA-F]{0,4}){0,4}%[0-9a-zA-Z]{1,}|::(ffff(:0{1,4}){0,1}:){0,1}((25[0-5]|(2[0-4]|1{0,1}[0-9]){0,1}[0-9]).){3,3}(25[0-5]|(2[0-4]|1{0,1}[0-9]){0,1}[0-9])|([0-9a-fA-F]{1,4}:){1,4}:((25[0-5]|(2[0-4]|1{0,1}[0-9]){0,1}[0-9]).){3,3}(25[0-5]|(2[0-4]|1{0,1}[0-9]){0,1}[0-9]))'`
        if [ ! -z $host_v4 ];
        then
            node=$host_v4":"$EDIT_LOG_PORT
            HELPER=$HELPER","$node
            let list_length+=1
        fi

        if [ ! -z $host_v6 ];
        then
            node="["$host_v6"]:"$EDIT_LOG_PORT
            HELPER=$HELPER","$node
            let list_length+=1
        fi
    done
    # Follower or Observer
    HELPER=`echo $HELPER | awk '{print substr($1,2)}'`
    if [ $list_length -le 0 ];
    then
        # first fe --- Leader
        echo "helper is empty and this fe will be Leader."
        HELPER=
    fi
}

# export env variables from fe.conf
#
# JAVA_OPTS
# LOG_DIR
# PID_DIR
export JAVA_OPTS="-Xmx1024m"
export LOG_DIR="${DORIS_HOME}/log"
export PID_DIR=`cd "$curdir"; pwd`

while read -r line; do
    envline="$(echo "${line}" |
        sed 's/[[:blank:]]*=[[:blank:]]*/=/g' |
        sed 's/^[[:blank:]]*//g' |
        grep -E "^[[:upper:]]([[:upper:]]|_|[[:digit:]])*=" ||
        true)"
    envline="$(eval "echo ${envline}")"
    if [[ "${envline}" == *"="* ]]; then
        eval 'export "${envline}"'
    fi
    # check for meta_dir
    envline=`echo $line | sed 's/[[:blank:]]*=[[:blank:]]*/=/g' | sed 's/^[[:blank:]]*//g' | egrep "^meta_dir="`
    envline=`eval "echo $envline"`
    if [[ $envline == *"="* ]]; then
        FE_METADATA_DIR=$(echo "$envline" | sed 's/meta_dir=//g')
        echo "FE_METADATA_DIR=${FE_METADATA_DIR}"
    fi
done <"${DORIS_HOME}/conf/$CLUSTER/fe.conf"

# if first deploy in this mechine
role_file_path=${FE_METADATA_DIR}/image/ROLE
if [ ! -d $FE_METADATA_DIR ] || [ ! -f $role_file_path ];
then
    echo "Doris FE first deploy in this mechine"
    mkdir -p $FE_METADATA_DIR
    if [ -z $HELPER ];
    then
        echo "append helper"
        append_helper
    fi
else
    echo "Doris FE has been deploy before"
    HELPER=
fi
# process --helper param end

if [[ -e "${DORIS_HOME}/bin/palo_env.sh" ]]; then
    # shellcheck disable=1091
    source "${DORIS_HOME}/bin/palo_env.sh"
fi

#Due to the machine not being configured with Java home, in this case, when FE cannot start, it is necessary to prompt an error message indicating that it has not yet been configured with Java home.


if [[ -z "${JAVA_HOME}" ]]; then
    if ! command -v java &>/dev/null; then
        JAVA=""
    else
        JAVA="$(command -v java)"
    fi
else
    JAVA="${JAVA_HOME}/bin/java"
fi

if [[ ! -x "${JAVA}" ]]; then
    echo "The JAVA_HOME environment variable is not defined correctly"
    echo "This environment variable is needed to run this program"
    echo "NB: JAVA_HOME should point to a JDK not a JRE"
    exit 1
fi

for var in http_proxy HTTP_PROXY https_proxy HTTPS_PROXY; do
    if [[ -n ${!var} ]]; then
        echo "env '${var}' = '${!var}', need unset it using 'unset ${var}'"
        exit 1
    fi
done

# get jdk version, return version as an Integer.
# 1.8 => 8, 13.0 => 13
jdk_version() {
    local java_cmd="${1}"
    local result
    local IFS=$'\n'

    if [[ -z "${java_cmd}" ]]; then
        result=no_java
        return 1
    else
        local version
        # remove \r for Cygwin
        version="$("${java_cmd}" -Xms32M -Xmx32M -version 2>&1 | tr '\r' '\n' | grep version | awk '{print $3}')"
        version="${version//\"/}"
        if [[ "${version}" =~ ^1\. ]]; then
            result="$(echo "${version}" | awk -F '.' '{print $2}')"
        else
            result="$(echo "${version}" | awk -F '.' '{print $1}')"
        fi
    fi
    echo "${result}"
    return 0
}

# need check and create if the log directory existed before outing message to the log file.
if [[ ! -d "${LOG_DIR}" ]]; then
    mkdir -p "${LOG_DIR}"
fi

STDOUT_LOGGER="${LOG_DIR}/fe.out"
log() {
    # same datetime format as in fe.log: 2024-06-03 14:54:41,478
    cur_date=$(date +"%Y-%m-%d %H:%M:%S,$(date +%3N)")
    if [[ "${RUN_CONSOLE}" -eq 1 ]]; then
        echo "StdoutLogger ${cur_date} $1"
    else
        echo "StdoutLogger ${cur_date} $1" >>"${STDOUT_LOGGER}"
    fi
}

# check java version and choose correct JAVA_OPTS
java_version="$(
    set -e
    jdk_version "${JAVA}"
)"
final_java_opt="${JAVA_OPTS}"
if [[ "${java_version}" -gt 16 ]]; then
    if [[ -z "${JAVA_OPTS_FOR_JDK_17}" ]]; then
        echo "JAVA_OPTS_FOR_JDK_17 is not set in fe.conf"
        exit 1
    fi
    final_java_opt="${JAVA_OPTS_FOR_JDK_17}"
elif [[ "${java_version}" -gt 8 ]]; then
    if [[ -z "${JAVA_OPTS_FOR_JDK_9}" ]]; then
        log "JAVA_OPTS_FOR_JDK_9 is not set in fe.conf"
        exit 1
    fi
    final_java_opt="${JAVA_OPTS_FOR_JDK_9}"
fi
log "using java version ${java_version}"
log "${final_java_opt}"
export JAVA_OPTS="${final_java_opt}"

# add libs to CLASSPATH
DORIS_FE_JAR=
for f in "${DORIS_HOME}/lib"/*.jar; do
    if [[ "${f}" == *"doris-fe.jar" ]]; then
        DORIS_FE_JAR="${f}"
        continue
    fi
    CLASSPATH="${f}:${CLASSPATH}"
done

# add custom_libs to CLASSPATH
if [[ -d "${DORIS_HOME}/custom_lib" ]]; then
    for f in "${DORIS_HOME}/custom_lib"/*.jar; do
        CLASSPATH="${CLASSPATH}:${f}"
    done
fi

# make sure the doris-fe.jar is at first order, so that some classed
# with same qualified name can be loaded priority from doris-fe.jar
CLASSPATH="${DORIS_FE_JAR}:${CLASSPATH}"
if [[ -n "${HADOOP_CONF_DIR}" ]]; then
    CLASSPATH="${HADOOP_CONF_DIR}:${CLASSPATH}"
fi
export CLASSPATH="${DORIS_HOME}/conf:${CLASSPATH}:${DORIS_HOME}/lib"

pidfile="${PID_DIR}/fe.pid"

if [[ -f "${pidfile}" ]] && [[ "${OPT_VERSION}" == "" ]]; then
    if kill -0 "$(cat "${pidfile}")" >/dev/null 2>&1; then
        echo "Frontend running as process $(cat "${pidfile}"). Stop it first."
        exit 1
    fi
fi

if [[ ! -f "/bin/limit" ]]; then
    LIMIT=''
else
    LIMIT=/bin/limit
fi

coverage_opt=""
if [[ -n "${JACOCO_COVERAGE_OPT}" ]]; then
    coverage_opt="${JACOCO_COVERAGE_OPT}"
fi

CUR_DATE=$(date)
log "start time: ${CUR_DATE}"

function register_consul_service(){
    if [[ ${FE_ORIGIN_PSM} != "" ]]; then
        log "register service origin psm: ${FE_ORIGIN_PSM}"
        /opt/tiger/consul_deploy/bin/sd up $FE_ORIGIN_PSM"_mysql" 9030 --dual-stack --tags "{\"env\":\"dev\",\"weight\":\"10\",\"cluster\":\""${CLUSTER}"\"}" --check-port
        /opt/tiger/consul_deploy/bin/sd up $FE_ORIGIN_PSM"_http" 8030 --dual-stack --tags "{\"env\":\"dev\",\"weight\":\"10\",\"cluster\":\""${CLUSTER}"\"}" --check-port
    else
        log "register service psm: ${FE_PSM_PREFIX}"
        /opt/tiger/consul_deploy/bin/sd up $FE_PSM_PREFIX"_mysql" 9030 --dual-stack --tags "{\"env\":\"dev\",\"weight\":\"10\",\"cluster\":\""${CLUSTER}"\"}" --check-port
        /opt/tiger/consul_deploy/bin/sd up $FE_PSM_PREFIX"_http" 8030 --dual-stack --tags "{\"env\":\"dev\",\"weight\":\"10\",\"cluster\":\""${CLUSTER}"\"}" --check-port
    fi
}

if [[ "${HELPER}" != "" ]]; then
    # change it to '-helper' to be compatible with code in Frontend
    HELPER="-helper ${HELPER}"
fi

if [[ "${IMAGE_TOOL}" -eq 1 ]]; then
    if [[ -n "${IMAGE_PATH}" ]]; then
        ${LIMIT:+${LIMIT}} "${JAVA}" ${final_java_opt:+${final_java_opt}} ${coverage_opt:+${coverage_opt}} org.apache.doris.DorisFE -i "${IMAGE_PATH}"
    else
        echo "Internal Error. USE IMAGE_TOOL like : ./start_fe.sh --image image_path"
    fi
elif [[ "${RUN_DAEMON}" -eq 1 ]]; then
    nohup ${LIMIT:+${LIMIT}} "${JAVA}" ${final_java_opt:+${final_java_opt}} -XX:-OmitStackTraceInFastThrow -XX:OnOutOfMemoryError="kill -9 %p" ${coverage_opt:+${coverage_opt}} org.apache.doris.DorisFE ${HELPER:+${HELPER}} "${METADATA_FAILURE_RECOVERY}" "$@" >>"${STDOUT_LOGGER}" 2>&1 </dev/null &
    # register_consul_service
elif [[ "${RUN_CONSOLE}" -eq 1 ]]; then
    # register_consul_service
    export DORIS_LOG_TO_STDERR=1
    ${LIMIT:+${LIMIT}} "${JAVA}" ${final_java_opt:+${final_java_opt}} -XX:-OmitStackTraceInFastThrow -XX:OnOutOfMemoryError="kill -9 %p" ${coverage_opt:+${coverage_opt}} org.apache.doris.DorisFE ${HELPER:+${HELPER}} ${OPT_VERSION:+${OPT_VERSION}} "${METADATA_FAILURE_RECOVERY}" "$@" </dev/null
else
    # register_consul_service
    ${LIMIT:+${LIMIT}} "${JAVA}" ${final_java_opt:+${final_java_opt}} -XX:-OmitStackTraceInFastThrow -XX:OnOutOfMemoryError="kill -9 %p" ${coverage_opt:+${coverage_opt}} org.apache.doris.DorisFE ${HELPER:+${HELPER}} ${OPT_VERSION:+${OPT_VERSION}} "${METADATA_FAILURE_RECOVERY}" "$@" >>"${STDOUT_LOGGER}" 2>&1 </dev/null
fi

if [[ "${OPT_VERSION}" != "" ]]; then
    exit 0
fi

echo $! >"${pidfile}"
