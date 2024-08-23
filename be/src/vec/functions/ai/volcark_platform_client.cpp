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

#include "volcark_platform_client.h"

#include "json_utils.h"

namespace doris::ai {

Status LanguageModelClient::setup() {
    if (test_flag) {
        return Status::OK();
    }
    std::string endpoint;
    RETURN_IF_ERROR(BuiltInModelInfo::getInstance().get_language_model_url(endpoint));
    RETURN_IF_ERROR(client.init(endpoint));
    client.set_method(POST);
    return Status::OK();
}
Status LanguageModelClient::get_answer(const std::string& prompt, const std::string& model_name,
                                       ModelConfig& model_config, std::string& answer) {
    // Only for test purpose.
    if (test_flag) {
        answer = "Success";
        return Status::OK();
    }
    // 1. Locate the specific model.
    std::string model_id;
    std::string api_key;
    std::pair<std::string, std::string> model_info;
    RETURN_IF_ERROR(BuiltInModelInfo::getInstance().get_language_model_info(model_name, model_info));
    if (model_info.first.empty()) {
        model_id = model_name;
        RETURN_IF_ERROR(model_config.get_api_key(api_key));
        if (api_key.empty()) {
            return Status::InvalidArgument(
                    "{} is not a built-in model, if you specified a user-defined model, the "
                    "`api_key` "
                    "should be set in `config` argument.",
                    model_name);
        }
    } else {
        model_id = model_info.first;
        api_key = model_info.second;
    }
    // 2. Update the http headers.
    client.clear_header();
    client.set_header("Content-Type", "application/json");

    const string authorization_header = "Bearer " + api_key;
    client.set_header("Authorization", authorization_header.c_str());
    // 3. Build the request.
    string request;
    int timeout_ms = 10000;
    RETURN_IF_ERROR(model_config.get_timeout_ms(timeout_ms));
    client.set_timeout_ms(timeout_ms);
    RETURN_IF_ERROR(generate_language_model_request(model_id, prompt, request));
    int retry_times = 3;
    RETURN_IF_ERROR(model_config.get_retry_times(retry_times));
    string raw_response;
    auto status = client.execute_post_request_with_retry(request, &raw_response, retry_times);
    if (!status.ok()) {
        return Status::HttpError("Failed to query language model: {}", status.msg());
    }
    status = parse_language_model_response(raw_response, answer);
    if (!status.ok() || answer.empty()) {
        return Status::InternalError(
                "Failed to parse language model response, status: {}, "
                "answer: {}",
                status.msg(), answer);
    }
    return Status::OK();
}

Status LanguageModelClient::get_embedding(const std::string& text, const std::string& model_name,
                                          ModelConfig& model_config,
                                          std::vector<float>& embedding) {
    return Status::Uninitialized("Language model does not support embedding.");
}

Status EmbeddingModelClient::setup() {
    if (test_flag) {
        return Status::OK();
    }
    std::string endpoint;
    RETURN_IF_ERROR(BuiltInModelInfo::getInstance().get_embedding_model_url(endpoint));
    RETURN_IF_ERROR(client.init(endpoint));
    client.set_method(POST);
    return Status::OK();
}
Status EmbeddingModelClient::get_answer(const std::string& prompt, const std::string& model_name,
                                        ModelConfig& model_config, std::string& answer) {
    return Status::Uninitialized("Embedding model does not support generate contents.");
}

Status EmbeddingModelClient::get_embedding(const std::string& text, const std::string& model_name,
                                           ModelConfig& model_config,
                                           std::vector<float>& embedding) {
    // Only for test purpose.
    if (test_flag) {
        embedding.emplace_back(0.1);
        return Status::OK();
    }
    // 1. Locate the specific model.
    std::string model_id;
    std::string api_key;
    std::pair<std::string, std::string> model_info;
    RETURN_IF_ERROR(BuiltInModelInfo::getInstance().get_embedding_model_info(model_name, model_info));
    if (model_info.first.empty()) {
        model_id = model_name;
        RETURN_IF_ERROR(model_config.get_api_key(api_key));
        if (api_key.empty()) {
            return Status::InvalidArgument(
                    "{} is not a built-in model, if you specified a user-defined model, the "
                    "`api_key` "
                    "should be set in `config` argument.",
                    model_name);
        }
    } else {
        model_id = model_info.first;
        api_key = model_info.second;
    }
    // 2. Update the http headers.
    client.clear_header();
    client.set_header("Content-Type", "application/json");

    const string authorization_header = "Bearer " + api_key;
    client.set_header("Authorization", authorization_header.c_str());
    // 3. Build the request.
    string request;
    int timeout_ms = 10000;
    RETURN_IF_ERROR(model_config.get_timeout_ms(timeout_ms));
    client.set_timeout_ms(timeout_ms);
    RETURN_IF_ERROR(generate_embedding_model_request(model_id, text, request));
    int retry_times = 3;
    RETURN_IF_ERROR(model_config.get_retry_times(retry_times));
    string raw_response;
    auto status = client.execute_post_request_with_retry(request, &raw_response, retry_times);
    if (!status.ok()) {
        return Status::HttpError("Failed to query embedding model: {}", status.msg());
    }
    status = parse_embedding_model_response(raw_response, embedding);
    if (!status.ok() || embedding.empty()) {
        return Status::InternalError("Failed to parse embedding response, status: {}",
                                     status.msg());
    }
    return Status::OK();
}

bool VolcarkPlatformClient::test_flag = false;

void VolcarkPlatformClient::set_test_flag() {
    test_flag = true;
}

} // namespace doris::ai
