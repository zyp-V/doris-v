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

#pragma once

#include <common/config.h>
#include <atomic>
#include <chrono>
#include <condition_variable>
#include <mutex>
#include <thread>

#include "../../../common/status.h"
#include "../../../http/http_client.h"

namespace doris::ai {

class BuiltInModelInfo {
public:
    static BuiltInModelInfo& getInstance() {
        static BuiltInModelInfo instance;
        return instance;
    }

    Status get_language_model_info(const std::string& model_name,
                                   std::pair<std::string, std::string>& model_info);
    Status get_embedding_model_info(const std::string& model_name,
                                    std::pair<std::string, std::string>& model_info);
    Status get_language_model_url(std::string& language_model_url);
    Status get_embedding_model_url(std::string& embedding_model_url);

private:
    BuiltInModelInfo();
    ~BuiltInModelInfo();
    BuiltInModelInfo(const BuiltInModelInfo&) = delete;
    BuiltInModelInfo& operator=(const BuiltInModelInfo&) = delete;

    void init_from_tcc();
    void init_success();
    void periodic_init_from_tcc();

    HttpClient client;
    const std::string tcc_url =
            "https://cloud.bytedance.net/api/v1/tcc_v3_openapi/bcc/open/"
            "config/get?ns_name=flow.datamind.config&dir=/default"
            "&region=CN&conf_name=model_info";
    std::string tcc_api_key = config::datamind_tcc_api_key;
    std::unordered_map<std::string, std::pair<std::string, std::string>> language_model_infos;
    std::unordered_map<std::string, std::pair<std::string, std::string>> embedding_model_infos;
    std::string language_model_url;
    std::string embedding_model_url;
    std::atomic<bool> initialized;
    std::mutex mutex;
    std::condition_variable cv;
    std::thread init_thread;

    Status read_tcc_api_key_from_file(std::string& tcc_api_key);
};

class ModelConfig {
public:
    Status parse_from_parameter(const std::string& parameter);
    Status get_api_key(std::string& api_key) const;
    Status is_null_on_failure(bool& is_null_on_failure) const;
    Status get_retry_times(int& retry_times) const;
    Status get_timeout_ms(int& timeout_ms) const;

private:
    std::string api_key;
    bool null_on_failure = false;
    int retry_times = 3;
    int timeout_ms = 10000;
};

} // namespace doris::ai
