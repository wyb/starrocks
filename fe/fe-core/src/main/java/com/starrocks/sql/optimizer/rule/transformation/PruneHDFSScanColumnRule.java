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

package com.starrocks.sql.optimizer.rule.transformation;

import com.google.common.base.Preconditions;
import com.google.common.collect.Lists;
import com.starrocks.catalog.Column;
import com.starrocks.catalog.PrimitiveType;
import com.starrocks.catalog.Table;
import com.starrocks.catalog.Type;
import com.starrocks.sql.common.ErrorType;
import com.starrocks.sql.common.StarRocksPlannerException;
import com.starrocks.sql.optimizer.OptExpression;
import com.starrocks.sql.optimizer.OptimizerContext;
import com.starrocks.sql.optimizer.Utils;
import com.starrocks.sql.optimizer.base.ColumnRefSet;
import com.starrocks.sql.optimizer.operator.OperatorType;
import com.starrocks.sql.optimizer.operator.logical.LogicalIcebergScanOperator;
import com.starrocks.sql.optimizer.operator.logical.LogicalScanOperator;
import com.starrocks.sql.optimizer.operator.pattern.MultiOpPattern;
import com.starrocks.sql.optimizer.operator.scalar.ColumnRefOperator;
import com.starrocks.sql.optimizer.operator.scalar.ScalarOperator;
import com.starrocks.sql.optimizer.rule.RuleType;

import java.util.ArrayList;
import java.util.Collections;
import java.util.List;
import java.util.Map;
import java.util.Objects;
import java.util.Set;
import java.util.stream.Collectors;

import static java.util.function.UnaryOperator.identity;

public class PruneHDFSScanColumnRule extends TransformationRule {
    private static final Set<OperatorType> SUPPORTED = Set.of(
            OperatorType.LOGICAL_HIVE_SCAN,
            OperatorType.LOGICAL_ICEBERG_SCAN,
            OperatorType.LOGICAL_HUDI_SCAN,
            OperatorType.LOGICAL_DELTALAKE_SCAN,
            OperatorType.LOGICAL_FILE_SCAN,
            OperatorType.LOGICAL_PAIMON_SCAN,
            OperatorType.LOGICAL_ODPS_SCAN,
            OperatorType.LOGICAL_TABLE_FUNCTION_TABLE_SCAN,
            OperatorType.LOGICAL_ICEBERG_METADATA_SCAN
    );

    public PruneHDFSScanColumnRule() {
        super(RuleType.TF_PRUNE_OLAP_SCAN_COLUMNS, MultiOpPattern.of(SUPPORTED));
    }

    @Override
    public List<OptExpression> transform(OptExpression input, OptimizerContext context) {
        LogicalScanOperator scanOperator = (LogicalScanOperator) input.getOp();
        ColumnRefSet requiredOutputColumns = context.getTaskContext().getRequiredColumns();

        Set<ColumnRefOperator> scanColumns =
                scanOperator.getColRefToColumnMetaMap().keySet().stream().filter(requiredOutputColumns::contains)
                        .collect(Collectors.toSet());
        scanColumns.addAll(Utils.extractColumnRef(scanOperator.getPredicate()));

        checkPartitionColumnType(scanOperator, scanColumns, context);

        // make sure there is at least one materialized column in new output columns.
        // if not, we have to choose one materialized column from scan operator output columns
        // with the minimal cost.
        boolean canUseAnyColumn = false;

        // if this scan operator columns are all partitions columns(like iceberg table)
        // we have to take partition columns are materialized columns and read them from files.
        // And we can not use `canUseAnyColumn` optimization either.
        boolean allPartitionColumns =
                scanOperator.getPartitionColumns()
                        .containsAll(scanOperator.getColRefToColumnMetaMap().values().stream().map(x -> x.getName()).collect(
                                Collectors.toList()));

        if (!containsMaterializedColumn(scanOperator, scanColumns)) {
            List<ColumnRefOperator> preOutputColumns =
                    new ArrayList<>(scanOperator.getColRefToColumnMetaMap().keySet());
            List<ColumnRefOperator> outputColumns = preOutputColumns.stream()
                    .filter(column -> !column.getType().getPrimitiveType().equals(PrimitiveType.UNKNOWN_TYPE))
                    .collect(Collectors.toList());

            int smallestIndex = -1;
            int smallestColumnLength = Integer.MAX_VALUE;
            for (int index = 0; index < outputColumns.size(); ++index) {
                if (!allPartitionColumns && isPartitionColumn(scanOperator, outputColumns.get(index).getName())) {
                    continue;
                }

                if (smallestIndex == -1) {
                    smallestIndex = index;
                }
                Type columnType = outputColumns.get(index).getType();
                if (columnType.isScalarType() && columnType.isSupported()) {
                    int columnLength = columnType.getTypeSize();
                    if (columnLength < smallestColumnLength) {
                        smallestIndex = index;
                        smallestColumnLength = columnLength;
                    }
                }
            }
            Preconditions.checkArgument(smallestIndex != -1);
            scanColumns.add(outputColumns.get(smallestIndex));
            canUseAnyColumn = true;
        }

        if (allPartitionColumns || !context.getSessionVariable().isEnableCountStarOptimization()) {
            canUseAnyColumn = false;
        }

        if (scanOperator.getOutputColumns().equals(new ArrayList<>(scanColumns))) {
            scanOperator.getScanOptimizeOption().setCanUseAnyColumn(canUseAnyColumn);
            return Collections.emptyList();
        } else {
            try {
                Class<? extends LogicalScanOperator> classType = scanOperator.getClass();
                Map<ColumnRefOperator, Column> newColumnRefMap = scanColumns.stream()
                        .collect(Collectors.toMap(identity(), scanOperator.getColRefToColumnMetaMap()::get));
                LogicalScanOperator newScanOperator =
                        classType.getConstructor(Table.class, Map.class, Map.class, long.class,
                                ScalarOperator.class).newInstance(
                                scanOperator.getTable(),
                                newColumnRefMap,
                                scanOperator.getColumnMetaToColRefMap(),
                                scanOperator.getLimit(),
                                scanOperator.getPredicate());
                newScanOperator.getScanOptimizeOption().setCanUseAnyColumn(canUseAnyColumn);
                newScanOperator.setScanOperatorPredicates(scanOperator.getScanOperatorPredicates());
                newScanOperator.setTableVersionRange(scanOperator.getTableVersionRange());

                if (newScanOperator.getOpType() == OperatorType.LOGICAL_ICEBERG_SCAN) {
                    LogicalIcebergScanOperator newIcebergScanOp = (LogicalIcebergScanOperator) newScanOperator;
                    newIcebergScanOp.setMORParam(((LogicalIcebergScanOperator) scanOperator).getMORParam());
                    newIcebergScanOp.setTableFullMORParams(((LogicalIcebergScanOperator) scanOperator).
                            getTableFullMORParams());
                }

                return Lists.newArrayList(new OptExpression(newScanOperator));
            } catch (Exception e) {
                throw new StarRocksPlannerException(e.getMessage(), ErrorType.INTERNAL_ERROR);
            }
        }
    }

    private void checkPartitionColumnType(LogicalScanOperator scanOperator,
                                          Set<ColumnRefOperator> scanColumnRefOperators,
                                          OptimizerContext context) {
        Table table = scanOperator.getTable();
        List<Column> partitionColumns = table.getPartitionColumnNames().stream().filter(Objects::nonNull)
                .map(table::getColumn).collect(Collectors.toList());
        List<Column> scanColumns =
                scanColumnRefOperators.stream().map(col -> context.getColumnRefFactory().getColumn(col)).
                        collect(Collectors.toList());
        partitionColumns.retainAll(scanColumns);
        if (table.isIcebergTable()) {
            if (partitionColumns.stream().map(Column::getType).anyMatch(this::icebergNotSupportedPartitionColumnType)) {
                throw new StarRocksPlannerException("Iceberg table partition by float/double/decimalV2 datatype is not supported",
                        ErrorType.UNSUPPORTED);
            }
        } else if (partitionColumns.stream().map(Column::getType).anyMatch(this::notSupportedPartitionColumnType)) {
            throw new StarRocksPlannerException("Table partition by float/double/decimal datatype is not supported",
                    ErrorType.UNSUPPORTED);
        }
    }

    private boolean notSupportedPartitionColumnType(Type type) {
        return type.isFloat() || type.isDouble() || type.isDecimalOfAnyVersion();
    }

    private boolean icebergNotSupportedPartitionColumnType(Type type) {
        return type.isFloat() || type.isDouble() || type.isDecimalV2();
    }

    private boolean containsMaterializedColumn(LogicalScanOperator scanOperator, Set<ColumnRefOperator> scanColumns) {
        return scanColumns.size() != 0 && !scanOperator.getPartitionColumns().containsAll(
                scanColumns.stream().map(ColumnRefOperator::getName).collect(Collectors.toList()));
    }

    private boolean isPartitionColumn(LogicalScanOperator scanOperator, String columnName) {
        // Hive/Hudi partition columns is not materialized column, so except partition columns
        return scanOperator.getPartitionColumns().contains(columnName);
    }
}
