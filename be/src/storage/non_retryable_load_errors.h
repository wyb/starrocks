// Copyright 2021-present StarRocks, Inc. All rights reserved.
//
// Licensed under the Apache License, Version 2.0 (the "License");
// you may not use this file except in compliance with the License.
// You may obtain a copy of the License at
//
//     https://www.apache.org/licenses/LICENSE-2.0
//
// Unless required by applicable law or agreed to in writing, software
// distributed under the License is distributed on an "AS IS" BASIS,
// WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
// See the License for the specific language governing permissions and
// limitations under the License.

#pragma once

#include <string_view>
#include <vector>

namespace starrocks {

// Centralized registry of non-retryable load error messages.
// When a routine load task encounters one of these errors, it will be paused
// instead of retried, because retrying would produce the same failure.
//
// To add a new non-retryable error:
//   1. Add a constant below (kXxxError)
//   2. Add it to the vector in is_non_retryable_load_error()
//   3. Use the constant at the error source site

inline constexpr char kPrimaryKeySizeExceedError[] = "primary key size exceed the limit";

inline bool is_non_retryable_load_error(std::string_view msg) {
    static const std::vector<std::string_view> non_retryable_errors = {
            kPrimaryKeySizeExceedError,
    };
    for (const auto& pattern : non_retryable_errors) {
        if (msg.find(pattern) != std::string_view::npos) {
            return true;
        }
    }
    return false;
}

} // namespace starrocks
