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
//
// This file is based on code available under the Apache license here:
//   https://github.com/apache/incubator-doris/blob/master/gensrc/thrift/Status.thrift

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

namespace cpp starrocks
namespace java com.starrocks.thrift

// NOTE: Each item of StatusCode is explicitly assigned a constant value.
// The TStatusCode struct is used in all FEs and BEs. In order to be able to
// avoid errors when identifying status_codes in RPC during upgrading StarRocks
// (update and restart the servers one by one), we must ensure that each element
// always a fixed value.
//
// If each element is not explicitly assigned a constant, then the value of
// each element will be assigned from 0 in turn, which will need us to be very
// careful when adding and removing elements, to avoid the same element on
// different machines to be recognized as a different value. i.e., new elements
// can only be added to the end, and only elements at the end can be deleted.
// Unfortunately, this implicit constraint is likely to be ignored by
// programmers when coding, especially those who are new to StarRocks.
//
// NOTE: We use one byte in starrocks::Status, so the max value is 255.
enum TStatusCode {
    OK                              = 0,
    CANCELLED                       = 1,
    ANALYSIS_ERROR                  = 2,
    NOT_IMPLEMENTED_ERROR           = 3,
    RUNTIME_ERROR                   = 4,
    MEM_LIMIT_EXCEEDED              = 5,
    INTERNAL_ERROR                  = 6,
    THRIFT_RPC_ERROR                = 7,
    TIMEOUT                         = 8,
    KUDU_NOT_ENABLED                = 9,  // Deprecated
    KUDU_NOT_SUPPORTED_ON_OS        = 10, // Deprecated
    MEM_ALLOC_FAILED                = 11,
    BUFFER_ALLOCATION_FAILED        = 12,
    MINIMUM_RESERVATION_UNAVAILABLE = 13,
    PUBLISH_TIMEOUT                 = 14,
    LABEL_ALREADY_EXISTS            = 15,
    TOO_MANY_TASKS                  = 16,
    ES_INTERNAL_ERROR               = 17,
    ES_INDEX_NOT_FOUND              = 18,
    ES_SHARD_NOT_FOUND              = 19,
    ES_INVALID_CONTEXTID            = 20,
    ES_INVALID_OFFSET               = 21,
    ES_REQUEST_ERROR                = 22,

    END_OF_FILE         = 30,
    NOT_FOUND           = 31,
    CORRUPTION          = 32,
    INVALID_ARGUMENT    = 33,
    IO_ERROR            = 34,
    ALREADY_EXIST       = 35,
    NETWORK_ERROR       = 36,
    ILLEGAL_STATE       = 37,
    NOT_AUTHORIZED      = 38,
    ABORTED             = 39,
    REMOTE_ERROR        = 40,
    SERVICE_UNAVAILABLE = 41,
    UNINITIALIZED       = 42,
    CONFIGURATION_ERROR = 43,
    INCOMPLETE          = 44,
    OLAP_ERR_VERSION_ALREADY_MERGED = 45,
    DATA_QUALITY_ERROR  = 46,
    DUPLICATE_RPC_INVOCATION        = 47,
    GLOBAL_DICT_ERROR = 48,

    UNKNOWN = 50,

    TXN_NOT_EXISTS = 51,
    TXN_IN_PROCESSING = 52,

    RESOURCE_BUSY = 53,

    SR_EAGAIN = 54,

    REMOTE_FILE_NOT_FOUND = 55, // for hive external table
    YIELD = 56,
    
    JIT_COMPILE_ERROR = 57,

    CAPACITY_LIMIT_EXCEED = 58,

    SHUTDOWN = 59, // the service is shutting down

    BIG_QUERY_CPU_SECOND_LIMIT_EXCEEDED = 60,
    BIG_QUERY_SCAN_ROWS_LIMIT_EXCEEDED = 61,
    GLOBAL_DICT_NOT_MATCH = 62,
    LEADER_TRANSFERRED = 63,
}

