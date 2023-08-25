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

#include "connector/connector.h"
#include "exec/connector_scan_node.h"
#include "exec/pipeline/context_with_dependency.h"

namespace starrocks {

class RuntimeFilterProbeCollector;
class ScanNode;

namespace pipeline {

class ConnectorScanContext;
using ConnectorScanContextPtr = std::shared_ptr<ConnectorScanContext>;
class ConnectorScanContextFactory;
using ConnectorScanContextFactoryPtr = std::shared_ptr<ConnectorScanContextFactory>;

class ConnectorScanContext final : public ContextWithDependency {
public:
    explicit ConnectorScanContext(ConnectorScanNode* scan_node, int32_t dop, bool shared_scan)
            : _scan_node(scan_node),
              _shared_scan(shared_scan),
              _data_source_context(_scan_node->data_source_provider()->create_data_source_context()) {}
    ~ConnectorScanContext() override = default;

    Status prepare(RuntimeState* state, pipeline::MorselQueue* morsel_queue);
    Status finish(RuntimeState* state, const std::vector<ExprContext*>& runtime_in_filters,
                  RuntimeFilterProbeCollector* runtime_bloom_filters, pipeline::MorselQueue* morsel_queue);
    void close(RuntimeState* state) override {}

    void set_prepare_finished() { _is_prepare_finished.store(true, std::memory_order_release); }
    bool is_prepare_finished() const { return _is_prepare_finished.load(std::memory_order_acquire); }

    ConnectorScanNode* scan_node() const { return _scan_node; }

    // Shared scan states
    bool is_shared_scan() const { return _shared_scan; }

    connector::DataSourceContextPtr data_source_context() { return _data_source_context; }

private:
    ConnectorScanNode* _scan_node;
    bool _shared_scan;
    std::atomic<bool> _is_prepare_finished{false};
    connector::DataSourceContextPtr _data_source_context;
};

// ConnectorScanContextFactory creates different contexts for each scan operator, if _shared_scan is false.
// Otherwise, it outputs the same context for each scan operator.
class ConnectorScanContextFactory {
public:
    ConnectorScanContextFactory(ConnectorScanNode* const scan_node, int32_t dop, bool shared_morsel_queue,
                                bool shared_scan)
            : _scan_node(scan_node),
              _dop(dop),
              _shared_morsel_queue(shared_morsel_queue),
              _shared_scan(shared_scan),
              _contexts(shared_morsel_queue ? 1 : dop) {}

    ConnectorScanContextPtr get_or_create(int32_t driver_sequence);

private:
    ConnectorScanNode* const _scan_node;
    const int32_t _dop;
    const bool _shared_morsel_queue;   // Whether the scan operators share a morsel queue.
    const bool _shared_scan;           // Whether the scan operators share a chunk buffer.
    std::vector<ConnectorScanContextPtr> _contexts;
};

} // namespace pipeline
} // namespace starrocks
