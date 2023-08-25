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

#include "exec/pipeline/scan/connector_scan_context.h"

namespace starrocks::pipeline {

Status ConnectorScanContext::prepare(RuntimeState* state, pipeline::MorselQueue* morsel_queue) {
    return _data_source_context->prepare(morsel_queue);
}

Status ConnectorScanContext::finish(RuntimeState* state, const std::vector<ExprContext*>& runtime_in_filters,
                                    RuntimeFilterProbeCollector* runtime_bloom_filters,
                                    pipeline::MorselQueue* morsel_queue) {
    return _data_source_context->finish(state, runtime_in_filters, runtime_bloom_filters, morsel_queue);
}

ConnectorScanContextPtr ConnectorScanContextFactory::get_or_create(int32_t driver_sequence) {
    DCHECK_LT(driver_sequence, _dop);
    // ScanOperators sharing one morsel use the same context.
    int32_t idx = _shared_morsel_queue ? 0 : driver_sequence;
    DCHECK_LT(idx, _contexts.size());

    if (_contexts[idx] == nullptr) {
        _contexts[idx] = std::make_shared<ConnectorScanContext>(_scan_node, _dop, _shared_scan);
    }
    return _contexts[idx];
}

} // namespace starrocks::pipeline
