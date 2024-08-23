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
#include "../../../common/status.h"
/**
 * All json parse utils.
 */
namespace doris::ai {

Status parse_tcc_response_2_builtin_model_info(
        const std::string& json_response,
        std::unordered_map<std::string, std::pair<std::string, std::string>>& language_model_infos,
        std::unordered_map<std::string, std::pair<std::string, std::string>>& embedding_model_infos,
        std::string& language_model_url, std::string& embedding_model_url);

Status generate_language_model_request(const std::string& model_id, const std::string& prompt,
                                       std::string& json_request);

Status generate_embedding_model_request(const std::string& model_id, const std::string& text,
                                        std::string& json_request);

Status parse_embedding_model_response(const std::string& json_response,
                                      std::vector<float>& embedding);

Status parse_language_model_response(const std::string& json_response, std::string& answer);

Status parse_parameter_2_model_config(const std::string& parameter, std::string& api_key,
                                      bool& null_on_failure, int& retry_times, int& timeout_ms);

} // namespace doris::ai