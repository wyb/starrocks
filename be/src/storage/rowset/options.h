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

// This file is based on code available under the Apache license here:
//   https://github.com/apache/incubator-doris/blob/master/be/src/olap/rowset/segment_v2/options.h

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

#include <cstddef>

#include "io/seekable_input_stream.h"
#include "storage/rowset/page_handle.h"
namespace starrocks {

class FileSystem;
class RandomAccessFile;

static const uint32_t DEFAULT_PAGE_SIZE = 1024 * 1024; // default size: 1M

class PageBuilderOptions {
public:
    uint32_t data_page_size = DEFAULT_PAGE_SIZE;

    uint32_t dict_page_size = config::dictionary_page_size;
};

class IndexReadOptions {
public:
    bool use_page_cache = false;
    // for lake tablet
    LakeIOOptions lake_io_opts{.fill_data_cache = true};

    //RandomAccessFile* read_file = nullptr;
    io::SeekableInputStream* read_file = nullptr;
    OlapReaderStatistics* stats = nullptr;
};

} // namespace starrocks
