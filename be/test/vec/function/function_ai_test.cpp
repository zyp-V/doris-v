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

#include <gmock/gmock.h>
#include <gtest/gtest.h>
#include <vec/data_types/data_type_array.h>

#include <cstdint>
#include <cstdlib>
#include <cstring>
#include <memory>
#include <string>
#include <vector>

#include "common/status.h"
#include "function_test_util.h"
#include "testutil/any_type.h"
#include "vec/core/types.h"
#include "vec/data_types/data_type_date_time.h"
#include "vec/data_types/data_type_string.h"
#include "vec/functions/ai/volcark_platform_client.h"

namespace doris::vectorized {

TEST(function_ai_test, function_ai_query_case1) {
    std::string func_name = "ai_query";
    ai::VolcarkPlatformClient::set_test_flag();
    InputTypeSet input_types = {TypeIndex::String, TypeIndex::String};
    DataSet data_set = {{{std::string("volcark/Doubao-pro-128k"), std::string("Hello")},
                         std::string("Success")}};
    static_cast<void>(check_function<DataTypeString, true>(func_name, input_types, data_set));
}

TEST(function_ai_test, function_ai_query_case2) {
    std::string func_name = "ai_query";
    InputTypeSet input_types = {TypeIndex::String, TypeIndex::String, TypeIndex::String};
    DataSet data_set = {{{std::string("ep-20240806174835-8kt7k"), std::string("Hello"),
                          std::string(
                                  R"({"api_key":"46b719ab-e168-4",
                            "null_on_failure":true,
                            "retry":10,
                            "timeout_ms":20000})")},
                         Null()}};
    static_cast<void>(check_function<DataTypeString, true>(func_name, input_types, data_set));
}

TEST(function_ai_test, function_text_embedding_case1) {
    std::string func_name = "text_embedding";
    ai::VolcarkPlatformClient::set_test_flag();
    InputTypeSet input_types = {TypeIndex::String, TypeIndex::String};
    Array vec = {Float32(0.1)};
    DataSet data_set = {{{std::string("volcark/Doubao-embedding"), std::string("Text")}, vec}};
    static_cast<void>(check_array_function(
            func_name, input_types, data_set,
            std::make_shared<DataTypeArray>(make_nullable(std::make_shared<DataTypeFloat32>()))));
}

TEST(function_ai_test, function_text_embedding_case2) {
    std::string func_name = "text_embedding";
    InputTypeSet input_types = {TypeIndex::String, TypeIndex::String, TypeIndex::String};
    Array vec = {};
    DataSet data_set = {{{std::string("ep-20240820142732-wmx42"), std::string("Text"),
                          std::string(R"({"api_key":"123456",
                            "null_on_failure":true,
                            "retry":10,
                            "timeout_ms":20000})")},
                         vec}};
    static_cast<void>(check_array_function(
            func_name, input_types, data_set,
            std::make_shared<DataTypeArray>(make_nullable(std::make_shared<DataTypeFloat32>()))));
}

} // namespace doris::vectorized