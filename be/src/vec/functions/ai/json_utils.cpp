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

#include "json_utils.h"

#include <rapidjson/document.h>

namespace doris::ai {

Status parse_tcc_response_2_builtin_model_info(
        const std::string& json_response,
        std::unordered_map<std::string, std::pair<std::string, std::string>>& language_model_infos,
        std::unordered_map<std::string, std::pair<std::string, std::string>>& embedding_model_infos,
        std::string& language_model_url, std::string& embedding_model_url) {
    rapidjson::Document raw_data;
    raw_data.Parse(json_response.c_str());
    if (raw_data.HasParseError()) {
        return Status::InternalError("The format of tcc model config is illegal, {}",
                                     json_response);
    }
    const rapidjson::Value& data = raw_data["data"]["version_data"]["data"];
    rapidjson::Document model_config;
    model_config.Parse(data.GetString());
    if (model_config.HasParseError()) {
        return Status::InternalError("The format of tcc model config is illegal, {}",
                                     data.GetString());
    }
    // Inititialize the configs.
    // 1. Language model config.
    const rapidjson::Value& languageModelConfig = model_config["languange_model"];
    for (rapidjson::Document::ConstMemberIterator itr = languageModelConfig.MemberBegin();
         itr != languageModelConfig.MemberEnd(); ++itr) {
        language_model_infos[string(itr->name.GetString())] =
                make_pair(itr->value[0].GetString(), itr->value[1].GetString());
    }
    // 2. Embedding model config.
    const rapidjson::Value& embeddingModelConfig = model_config["embedding_model"];
    for (rapidjson::Document::ConstMemberIterator itr = embeddingModelConfig.MemberBegin();
         itr != embeddingModelConfig.MemberEnd(); ++itr) {
        embedding_model_infos[string(itr->name.GetString())] =
                make_pair(itr->value[0].GetString(), itr->value[1].GetString());
    }
    // 3. Language model url.
    language_model_url = model_config["language_model_url"].GetString();
    // 4. Embedding model url.
    embedding_model_url = model_config["embedding_model_url"].GetString();
    return Status::OK();
}

Status generate_language_model_request(const std::string& model_id, const std::string& prompt,
                                       std::string& json_request) {
    rapidjson::Document document;
    document.SetObject();
    rapidjson::Document::AllocatorType& allocator = document.GetAllocator();
    document.AddMember("model", rapidjson::Value(model_id.c_str(), allocator), allocator);
    rapidjson::Value messages(rapidjson::kArrayType);
    rapidjson::Value message(rapidjson::kObjectType);
    message.AddMember("role", rapidjson::Value("user", allocator), allocator);
    message.AddMember("content", rapidjson::Value(prompt.c_str(), allocator), allocator);
    messages.PushBack(message, allocator);
    document.AddMember("messages", messages, allocator);
    rapidjson::StringBuffer buffer;
    rapidjson::Writer writer(buffer);
    if (!document.Accept(writer)) {
        return Status::InternalError("Failed to write json request.");
    }

    json_request = buffer.GetString();
    return Status::OK();
}

Status generate_embedding_model_request(const std::string& model_id, const std::string& text,
                                        std::string& json_request) {
    rapidjson::Document document;
    document.SetObject();
    rapidjson::Document::AllocatorType& allocator = document.GetAllocator();
    document.AddMember("model", rapidjson::Value(model_id.c_str(), allocator), allocator);
    document.AddMember("input", rapidjson::Value(text.c_str(), allocator), allocator);
    rapidjson::StringBuffer buffer;
    rapidjson::Writer writer(buffer);
    if (!document.Accept(writer)) {
        return Status::InternalError("Failed to write json request.");
    }
    json_request = buffer.GetString();
    return Status::OK();
}

Status parse_language_model_response(const std::string& json_response, std::string& answer) {
    rapidjson::Document document;
    document.Parse(json_response.c_str());
    if (document.HasParseError() || !document.HasMember("choices")) {
        return Status::InternalError("Response doesn't contain 'choices' member.");
    }
    const rapidjson::Value& choice = document["choices"][0];
    if (!choice.HasMember("message")) {
        return Status::InternalError("Response doesn't contain 'message' member.");
    }
    const rapidjson::Value& message = choice["message"];
    if (!message.HasMember("content")) {
        return Status::InternalError("Response doesn't contain 'content' member.");
    }
    const rapidjson::Value& content = message["content"];
    answer = content.GetString();
    return Status::OK();
}

Status parse_embedding_model_response(const std::string& json_response,
                                      std::vector<float>& embedding) {
    rapidjson::Document document;
    document.Parse(json_response.c_str());
    if (document.HasParseError()) {
        return Status::InternalError("Embedding response is illegal.");
    }
    const rapidjson::Value& embeddingResult = document["data"][0]["embedding"];
    if (!embeddingResult.IsArray()) {
        return Status::InternalError("Embeddings member is not an array.");
    }
    for (const auto& value : embeddingResult.GetArray()) {
        if (!value.IsNumber()) {
            return Status::InternalError("Embedding values must be numbers.");
        }
        embedding.push_back(static_cast<float>(value.GetDouble()));
    }
    return Status::OK();
}

Status parse_parameter_2_model_config(const std::string& parameter, std::string& api_key,
                                      bool& null_on_failure, int& retry_times, int& timeout_ms) {
    rapidjson::Document document;
    if (document.Parse(parameter.c_str()).HasParseError()) {
        return Status::InvalidArgument("`parameter` argument is not a valid json object.");
    }
    if (!document.IsObject()) {
        return Status::InvalidArgument("`parameter` argument is not a valid json object.");
    }
    for (rapidjson::Document::ConstMemberIterator itr = document.MemberBegin();
         itr != document.MemberEnd(); ++itr) {
        string key = itr->name.GetString();
        if (key == "api_key") {
            api_key = string(itr->value.GetString());
        }
        if (key == "null_on_failure") {
            null_on_failure = itr->value.GetBool();
        }
        if (key == "retry") {
            retry_times = itr->value.GetInt();
        }
        if (key == "timeout_ms") {
            timeout_ms = itr->value.GetInt();
        }
    }
    return Status::OK();
}

} // namespace doris::ai
