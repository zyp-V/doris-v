// Licensed to the Apache Software Foundation (ASF) under one
// or more contributor license agreements.  See the NOTICE file
// distributed with this work for additional information
// regarding copyright ownership.  The ASF licenses this file
// to you under the Apache License, Version 2.0 (the
// "License"); you may not use this file except in compliance
// with the License.  You may obtain a copy of the License at
//
//   http://www.apache.org/licenses/LICENSE-2.0
//
// Unless required by applicable law or agreed to in writing,
// software distributed under the License is distributed on an
// "AS IS" BASIS, WITHOUT WARRANTIES OR CONDITIONS OF ANY
// KIND, either express or implied.  See the License for the
// specific language governing permissions and limitations
// under the License.

#include "http/action/jvm_dump_action.h"

#include <jemalloc/jemalloc.h>
#include <stdlib.h>
#include <unistd.h>

#include <ctime>
#include <fstream>
#include <memory>
#include <mutex>
#include <string>

#include "common/config.h"
#include "common/object_pool.h"
#include "http/ev_http_server.h"
#include "http/http_channel.h"
#include "http/http_handler.h"
#include "http/http_method.h"
#include "io/fs/local_file_system.h"
#include "util/jni-util.h"

namespace doris {
class HttpRequest;

static std::mutex kJvmActionMutex;
class JvmHeapAction : public HttpHandler {
public:
    JvmHeapAction() = default;
    virtual ~JvmHeapAction() = default;

    virtual void handle(HttpRequest* req) override;
};

void JvmHeapAction::handle(HttpRequest* req) {
    std::lock_guard<std::mutex> lock(kJvmActionMutex);
    if(!config::enable_java_support){
        std::string str = "enable_java_support not set";
        HttpChannel::send_reply(req, str);
        return;
    }

    std::stringstream tmp_jeprof_file_name;
    std::time_t now = std::time(nullptr);
    // Build a temporary file name that is hopefully unique.
    tmp_jeprof_file_name << config::jvm_dump_dir << "/be_jvm-" << now << "-" << getpid()
                         << "-" << rand() << ".hprof";
    const std::string& tmp_file_name_str = tmp_jeprof_file_name.str();
    auto st = JniUtil::HeapDump(tmp_file_name_str);
    std::stringstream response;
    if (st.ok()) {
        response << "jvm heap dump success, dump file path: " << tmp_jeprof_file_name.str() << "\n";
    } else {
        response << "jvm heap dump failed: "<< st.to_string() << "\n";
    }
    HttpChannel::send_reply(req, response.str());
}

Status JvmAction::setup(doris::ExecEnv* exec_env, doris::EvHttpServer* http_server,
                        doris::ObjectPool& pool) {
    if (!config::jvm_dump_dir.empty()) {
        RETURN_IF_ERROR(io::global_local_filesystem()->create_directory(config::jvm_dump_dir));
    }
    http_server->register_handler(HttpMethod::GET, "/jvm/dump", pool.add(new JvmHeapAction()));
    return Status::OK();
}

} // namespace doris
