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

#include "config.h"

#include "json_utils.h"

namespace doris::ai {

BuiltInModelInfo::BuiltInModelInfo() : initialized(false) {
    // Get the model info at the first time.
    init_from_tcc();
    init_thread = std::thread(&BuiltInModelInfo::periodic_init_from_tcc, this);
}

BuiltInModelInfo::~BuiltInModelInfo() {
    if (init_thread.joinable()) {
        init_thread.join();
    }
}

void BuiltInModelInfo::periodic_init_from_tcc() {
    while (true) {
        std::this_thread::sleep_for(std::chrono::minutes(10));
        init_from_tcc();
    }
}

void BuiltInModelInfo::init_from_tcc() {
    std::unique_lock lock(mutex);
    if (tcc_api_key.empty()) {
        if (Status st = read_tcc_api_key_from_file(tcc_api_key); !st.ok()) {
            init_success();
            return;
        };
    }
    if (tcc_api_key.empty()) {
        init_success();
        return;
    }
    if (Status st = client.init(tcc_url); !st.ok()) {
        init_success();
        return;
    }
    client.set_method(GET);
    const std::string ak = "Bearer " + tcc_api_key;
    client.set_header("Authorization", ak.c_str());
    std::string raw_response;
    if (const auto status = client.execute(&raw_response); !status.ok()) {
        init_success();
        return;
    }
    if (Status st = parse_tcc_response_2_builtin_model_info(
                raw_response, language_model_infos, embedding_model_infos, language_model_url,
                embedding_model_url);
        !st.ok()) {
        init_success();
        return;
    }
    init_success();
}

void BuiltInModelInfo::init_success() {
    initialized = true;
    cv.notify_all();
}

Status BuiltInModelInfo::get_embedding_model_info(
        const std::string& model_name, std::pair<std::string, std::string>& model_info){
    std::unique_lock lock(mutex);
    cv.wait(lock, [this] { return initialized.load(); });
    if (embedding_model_infos.contains(model_name)) {
        model_info = embedding_model_infos.at(model_name);
    }
    return Status::OK();
}

Status BuiltInModelInfo::get_language_model_info(
        const std::string& model_name, std::pair<std::string, std::string>& model_info){
    std::unique_lock lock(mutex);
    cv.wait(lock, [this] { return initialized.load(); });
    if (language_model_infos.contains(model_name)) {
        model_info = language_model_infos.at(model_name);
    }
    return Status::OK();
}

Status BuiltInModelInfo::get_language_model_url(std::string& language_model_url) {
    std::unique_lock lock(mutex);
    cv.wait(lock, [this] { return initialized.load(); });
    language_model_url = this->language_model_url;
    if(language_model_url.empty()) {
        return Status::NotFound("language model url is not found.");
    }
    return Status::OK();
}

Status BuiltInModelInfo::get_embedding_model_url(std::string& embedding_model_url) {
    std::unique_lock lock(mutex);
    cv.wait(lock, [this] { return initialized.load(); });
    embedding_model_url = this->embedding_model_url;
    if(embedding_model_url.empty()) {
        return Status::NotFound("embedding model url is not found.");
    }
    return Status::OK();
}

Status BuiltInModelInfo::read_tcc_api_key_from_file(std::string& tcc_api_key) {
    const std::string file_path = std::string(std::getenv("DORIS_HOME")) + "/conf/datamind-tcc.key";
    std::ifstream key_file(file_path, std::ios::binary);
    if (!key_file) {
        return Status::NotFound("Failed to open datamind-tcc.key: {}", file_path);
    }
    std::string content((std::istreambuf_iterator<char>(key_file)),
                        std::istreambuf_iterator<char>());
    key_file.close();
    const size_t ak_length = 32;
    if (content.size() < ak_length) {
        return Status::EndOfFile("datamind-tcc.key is too short to contain AK : {}", file_path);
    }
    tcc_api_key = content.substr(0, ak_length);
    return Status::OK();
}


Status ModelConfig::parse_from_parameter(const std::string& parameter) {
    RETURN_IF_ERROR(parse_parameter_2_model_config(parameter, api_key, null_on_failure, retry_times,
                                                   timeout_ms));
    return Status::OK();
}

Status ModelConfig::get_api_key(std::string& api_key) const {
    api_key = this->api_key;
    return Status::OK();
}
Status ModelConfig::is_null_on_failure(bool& is_null_on_failure) const {
    is_null_on_failure = this->null_on_failure;
    return Status::OK();
}
Status ModelConfig::get_retry_times(int& retry_times) const {
    retry_times = this->retry_times;
    return Status::OK();
}
Status ModelConfig::get_timeout_ms(int& timeout_ms) const {
    timeout_ms = this->timeout_ms;
    return Status::OK();
}

} // namespace doris::ai