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

#include "exec/pipeline/scan/connector_scan_prepare_operator.h"

#include "exec/connector_scan_node.h"
#include "storage/storage_engine.h"

namespace starrocks::pipeline {

/// ConnectorScanPrepareOperator
ConnectorScanPrepareOperator::ConnectorScanPrepareOperator(OperatorFactory* factory, int32_t id, const string& name,
                                                           int32_t plan_node_id, int32_t driver_sequence,
                                                           ConnectorScanContextPtr ctx)
        : SourceOperator(factory, id, name, plan_node_id, true, driver_sequence), _ctx(std::move(ctx)) {
    _ctx->ref();
}

ConnectorScanPrepareOperator::~ConnectorScanPrepareOperator() {
    auto* state = runtime_state();
    if (state == nullptr) {
        return;
    }

    _ctx->unref(state);
}

Status ConnectorScanPrepareOperator::prepare(RuntimeState* state) {
    RETURN_IF_ERROR(SourceOperator::prepare(state));
    RETURN_IF_ERROR(_ctx->prepare(state, _morsel_queue));
    return Status::OK();
}

void ConnectorScanPrepareOperator::close(RuntimeState* state) {
    SourceOperator::close(state);
}

bool ConnectorScanPrepareOperator::has_output() const {
    return !is_finished();
}

bool ConnectorScanPrepareOperator::is_finished() const {
    return _ctx->is_prepare_finished() || _ctx->is_finished();
}

StatusOr<ChunkPtr> ConnectorScanPrepareOperator::pull_chunk(RuntimeState* state) {
    auto status = _ctx->finish(state, runtime_in_filters(), runtime_bloom_filters(), _morsel_queue);
    _ctx->set_prepare_finished();
    if (!status.ok()) {
        _ctx->set_finished();
        return status;
    }
    return nullptr;
}

/// ConnectorScanPrepareOperatorFactory
ConnectorScanPrepareOperatorFactory::ConnectorScanPrepareOperatorFactory(int32_t id, int32_t plan_node_id,
                                                                         ConnectorScanNode* const scan_node,
                                                                         ConnectorScanContextFactoryPtr ctx_factory)
        : SourceOperatorFactory(id, "connector_scan_prepare", plan_node_id),
          _scan_node(scan_node),
          _ctx_factory(std::move(ctx_factory)) {}

Status ConnectorScanPrepareOperatorFactory::prepare(RuntimeState* state) {
    RETURN_IF_ERROR(SourceOperatorFactory::prepare(state));

    RETURN_IF_ERROR(_scan_node->data_source_provider()->prepare_scan(state));

    const auto& conjunct_ctxs = _scan_node->conjunct_ctxs();
    RETURN_IF_ERROR(Expr::prepare(conjunct_ctxs, state));
    RETURN_IF_ERROR(Expr::open(conjunct_ctxs, state));

    return Status::OK();
}

void ConnectorScanPrepareOperatorFactory::close(RuntimeState* state) {
    const auto& conjunct_ctxs = _scan_node->conjunct_ctxs();
    Expr::close(conjunct_ctxs, state);

    SourceOperatorFactory::close(state);
}

OperatorPtr ConnectorScanPrepareOperatorFactory::create(int32_t degree_of_parallelism, int32_t driver_sequence) {
    return std::make_shared<ConnectorScanPrepareOperator>(this, _id, _name, _plan_node_id, driver_sequence,
                                                          _ctx_factory->get_or_create(driver_sequence));
}

} // namespace starrocks::pipeline
