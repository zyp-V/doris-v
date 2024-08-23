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

#include <string>

#include "../../../common/status.h"
#include "../../../http/http_client.h"
#include "config.h"

namespace doris::ai {
/**
 * {@link VolcarkPlatformClient} is a client for accessing the Volcark Platform and invoking model services.
 */
class VolcarkPlatformClient {
public:

    virtual Status setup() = 0;

    virtual Status get_answer(const std::string& prompt, const std::string& model_name,
                              ModelConfig& model_config, std::string& answer) = 0;

    virtual Status get_embedding(const std::string& text, const std::string& model_name,
                                 ModelConfig& model_config, std::vector<float>& embedding) = 0;

    virtual ~VolcarkPlatformClient() = default;

    // Only for testing purpose.
    static void set_test_flag();

protected:
    HttpClient client;
    static bool test_flag;
};

/**
 * {@link LanguageModelClient} is used for language models.
 */
class LanguageModelClient final : public VolcarkPlatformClient {
public:
    LanguageModelClient() = default;
    Status setup() override;
    Status get_answer(const std::string& prompt, const std::string& model_name,
                      ModelConfig& model_config, std::string& answer) override;
    Status get_embedding(const std::string& text, const std::string& model_name,
                         ModelConfig& model_config, std::vector<float>& embedding) override;

    ~LanguageModelClient() override = default;
};

/**
 * {@link EmbeddingModelClient} is used for embedding models.
 */
class EmbeddingModelClient final : public VolcarkPlatformClient {
public:
    EmbeddingModelClient() = default;
    Status setup() override;
    Status get_answer(const std::string& prompt, const std::string& model_name,
                      ModelConfig& model_config, std::string& answer) override;
    Status get_embedding(const std::string& text, const std::string& model_name,
                         ModelConfig& model_config, std::vector<float>& embedding) override;

    ~EmbeddingModelClient() override = default;
};

} // namespace doris::ai
