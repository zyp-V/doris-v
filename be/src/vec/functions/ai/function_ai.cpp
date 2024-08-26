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

#include <vec/data_types/data_type_string.h>
#include <vec/functions/function_string.h>

#include "../simple_function_factory.h"
#include "config.h"
#include "vec/columns/column_array.h"
#include "vec/columns/column_map.h"
#include "vec/data_types/data_type_array.h"
#include "vec/data_types/data_type_number.h"
#include "vec/functions/function.h"
#include "volcark_platform_client.h"

namespace doris::vectorized {

class FunctionAIQuery final : public IFunction {
public:
    static constexpr auto name = "ai_query";

    static FunctionPtr create() { return std::make_shared<FunctionAIQuery>(); }

    String get_name() const override { return name; }

    size_t get_number_of_arguments() const override { return 0; }

    bool is_variadic() const override { return true; }

    DataTypePtr get_return_type_impl(const DataTypes& arguments) const override {
        return make_nullable(std::make_shared<DataTypeString>());
    }

    Status execute_impl(FunctionContext* context, Block& block, const ColumnNumbers& arguments,
                        size_t result, size_t input_rows_count) const override {
        // The number of arguments should be less than 3.
        DCHECK_LE(arguments.size(), 3);
        // 1. Create the result column.
        auto res = ColumnString::create();
        auto& res_offsets = res->get_offsets();
        auto& res_chars = res->get_chars();
        res_offsets.resize(input_rows_count);
        // 2. Parse the input arguments.
        const ColumnPtr argument_column1 =
                block.get_by_position(arguments[0]).column->convert_to_full_column_if_const();
        const ColumnPtr argument_column2 =
                block.get_by_position(arguments[1]).column->convert_to_full_column_if_const();
        const ColumnPtr argument_column3 =
                arguments.size() == 3 ? block.get_by_position(arguments[2])
                                                .column->convert_to_full_column_if_const()
                                      : nullptr;
        const auto* model_name_column = check_and_get_column<ColumnString>(argument_column1.get());
        const auto* prompt_column = check_and_get_column<ColumnString>(argument_column2.get());
        const auto* config_column =
                argument_column3 ? check_and_get_column<ColumnString>(argument_column3.get())
                                 : nullptr;
        if (!model_name_column) {
            return Status::InternalError("`model` argument should not be null");
        }
        if (!prompt_column) {
            return Status::InternalError("`prompt` argument should not be null");
        }
        // 3. Execute the function.
        // the null map is used to record which row is null.
        ai::LanguageModelClient language_model_client;
        RETURN_IF_ERROR(language_model_client.setup());
        auto null_map = ColumnUInt8::create(input_rows_count, 0);
        for (size_t i = 0; i < input_rows_count; ++i) {
            auto prompt = prompt_column->get_data_at(i);
            auto model_name = model_name_column->get_data_at(i);
            ai::ModelConfig config;
            if (config_column) {
                auto parameter_string = config_column->get_data_at(i);
                RETURN_IF_ERROR(config.parse_from_parameter(parameter_string.to_string()));
            }
            std::string answer;
            bool null_on_failure = false;
            RETURN_IF_ERROR(config.is_null_on_failure(null_on_failure));
            if (const auto status = language_model_client.get_answer(
                        prompt.to_string(), model_name.to_string(), config, answer);
                !status.ok()) {
                // Skip the error when null_on_failure is true.
                if (null_on_failure) {
                    null_map->get_data()[i] = 1;
                    continue;
                }
                return status;
            }
            StringOP::push_value_string(answer, i, res_chars, res_offsets);
        }
        // Create the nullable column
        block.get_by_position(result).column =
                ColumnNullable::create(std::move(res), std::move(null_map));
        return Status::OK();
    }
};

class FunctionTextEmbedding final : public IFunction {
public:
    static constexpr auto name = "text_embedding";
    static FunctionPtr create() { return std::make_shared<FunctionTextEmbedding>(); }

    String get_name() const override { return name; }

    bool is_variadic() const override { return true; }

    size_t get_number_of_arguments() const override { return 0; }

    DataTypePtr get_return_type_impl(const DataTypes& arguments) const override {
        return make_nullable(std::make_shared<DataTypeArray>(
                make_nullable(std::make_shared<DataTypeFloat32>())));
    }

    Status execute_impl(FunctionContext* context, Block& block, const ColumnNumbers& arguments,
                        size_t result, size_t input_rows_count) const override {
        // 1. Create the result column.
        auto offsets_col = ColumnVector<ColumnArray::Offset64>::create();
        ColumnArray::Offsets64& offsets = offsets_col->get_data();
        offsets.reserve(input_rows_count);
        // 2. Parse the input column.
        const ColumnPtr argument_column1 =
                block.get_by_position(arguments[0]).column->convert_to_full_column_if_const();
        const ColumnPtr argument_column2 =
                block.get_by_position(arguments[1]).column->convert_to_full_column_if_const();
        const ColumnPtr argument_column3 =
                arguments.size() == 3 ? block.get_by_position(arguments[2])
                                                .column->convert_to_full_column_if_const()
                                      : nullptr;
        const auto* model_name_column = check_and_get_column<ColumnString>(argument_column1.get());
        const auto* text_column = check_and_get_column<ColumnString>(argument_column2.get());
        const auto* config_column =
                argument_column3 ? check_and_get_column<ColumnString>(argument_column3.get())
                                 : nullptr;
        if (!model_name_column) {
            return Status::InternalError("Not supported `model` argument type");
        }
        if (!text_column) {
            return Status::InternalError("Not supported `text` argument type");
        }
        std::vector<float> embeddings;
        // 3. Execute the function.
        // the null map is used to record which row is null.
        ai::EmbeddingModelClient embedding_model_client;
        RETURN_IF_ERROR(embedding_model_client.setup());
        auto null_map = ColumnUInt8::create(input_rows_count, 0);
        for (size_t i = 0; i < input_rows_count; ++i) {
            const auto& text = text_column->get_data_at(i).to_string();
            const auto& model_name = model_name_column->get_data_at(i).to_string();
            ai::ModelConfig config;
            if (config_column) {
                auto parameter_string = config_column->get_data_at(i);
                RETURN_IF_ERROR(config.parse_from_parameter(parameter_string.to_string()));
            }
            bool null_on_failure = false;
            RETURN_IF_ERROR(config.is_null_on_failure(null_on_failure));
            auto status =
                    embedding_model_client.get_embedding(text, model_name, config, embeddings);
            offsets.push_back(static_cast<ColumnArray::Offset64>(embeddings.size()));
            if (!status.ok()) {
                // Skip the error when null_on_failure is true.
                if (null_on_failure) {
                    null_map->get_data()[i] = 1;
                    continue;
                }
                return status;
            }
        }
        auto float_col = ColumnFloat32::create(embeddings.size());
        std::memcpy(float_col->get_data().data(), embeddings.data(),
                    embeddings.size() * sizeof(float));
        auto nullable_float_col = ColumnNullable::create(std::move(float_col),
                                                         ColumnUInt8::create(float_col->size(), 0));
        auto array_col = ColumnArray::create(std::move(nullable_float_col), std::move(offsets_col));
        auto nullable_array_col = ColumnNullable::create(std::move(array_col), std::move(null_map));
        block.replace_by_position(result, std::move(nullable_array_col));
        return Status::OK();
    }
};

void register_function_size(SimpleFunctionFactory& factory) {
    factory.register_function<FunctionAIQuery>();
    factory.register_function<FunctionTextEmbedding>();
}
} // namespace doris::vectorized