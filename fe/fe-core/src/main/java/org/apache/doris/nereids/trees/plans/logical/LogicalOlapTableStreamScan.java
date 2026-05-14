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

package org.apache.doris.nereids.trees.plans.logical;

import org.apache.doris.catalog.Column;
import org.apache.doris.catalog.OlapTable;
import org.apache.doris.catalog.Table;
import org.apache.doris.nereids.memo.GroupExpression;
import org.apache.doris.nereids.properties.LogicalProperties;
import org.apache.doris.nereids.trees.TableSample;
import org.apache.doris.nereids.trees.expressions.Alias;
import org.apache.doris.nereids.trees.expressions.Slot;
import org.apache.doris.nereids.trees.expressions.SlotReference;
import org.apache.doris.nereids.trees.expressions.literal.BigIntLiteral;
import org.apache.doris.nereids.trees.expressions.literal.VarcharLiteral;
import org.apache.doris.nereids.trees.plans.Plan;
import org.apache.doris.nereids.trees.plans.PreAggStatus;
import org.apache.doris.nereids.trees.plans.RelationId;
import org.apache.doris.nereids.trees.plans.visitor.PlanVisitor;

import com.google.common.collect.ImmutableList;
import com.google.common.collect.Maps;
import org.apache.commons.lang3.tuple.Pair;

import java.util.Collection;
import java.util.List;
import java.util.Map;
import java.util.Objects;
import java.util.Optional;
import java.util.Set;

/**
 * Logical OlapTableStreamScan.
 */
public class LogicalOlapTableStreamScan extends LogicalOlapScan {
    private final boolean isIncrementalScan;
    private final Optional<List<Slot>> cachedOutput;

    /**
     * LogicalOlapTableStreamScan construct method.
     */
    public LogicalOlapTableStreamScan(RelationId id, OlapTable table, List<String> qualifier, List<Long> tabletIds,
            List<String> hints, Optional<TableSample> tableSample, Collection<Slot> operativeSlots) {
        super(id, table, qualifier, tabletIds, hints, tableSample);
        this.isIncrementalScan = false;
        this.cachedOutput = Optional.empty();
    }

    /**
     * LogicalOlapTableStreamScan construct method.
     */
    public LogicalOlapTableStreamScan(RelationId id, OlapTable table, List<String> qualifier,
            List<Long> specifiedPartitions, List<Long> tabletIds, List<String> hints,
            Optional<TableSample> tableSample, Collection<Slot> operativeSlots) {
        super(id, table, qualifier, specifiedPartitions, tabletIds, hints, tableSample);
        this.isIncrementalScan = false;
        this.cachedOutput = Optional.empty();
    }

    private LogicalOlapTableStreamScan(RelationId id, Table table, List<String> qualifier,
            Optional<GroupExpression> groupExpression, Optional<LogicalProperties> logicalProperties,
            List<Long> selectedPartitionIds, boolean partitionPruned,
            List<Long> selectedTabletIds, long selectedIndexId, boolean indexSelected,
            PreAggStatus preAggStatus, List<Long> specifiedPartitions,
            List<String> hints, Map<Pair<Long, String>, Slot> cacheSlotWithSlotName,
            Optional<TableSample> tableSample, boolean directMvScan,
            Map<String, Set<List<String>>> colToSubPathsMap, List<Long> specifiedTabletIds,
            Optional<List<Slot>> cachedOutput, boolean isIncrementalScan) {
        super(id, table, qualifier, groupExpression, logicalProperties,
                selectedPartitionIds, partitionPruned, selectedTabletIds,
                selectedIndexId, indexSelected, preAggStatus, specifiedPartitions,
                hints, cacheSlotWithSlotName, tableSample, directMvScan,
                colToSubPathsMap, specifiedTabletIds);
        this.isIncrementalScan = isIncrementalScan;
        this.cachedOutput = Objects.requireNonNull(cachedOutput, "cachedOutput can not be null");
    }

    @Override
    public List<Slot> computeOutput() {
        if (cachedOutput.isPresent()) {
            return cachedOutput.get();
        }

        // We need to create slots vectorized for stream scan, no need for invisible columns.
        // TODO: support compute binlog-based schema.
        List<Column> baseSchema = table.getBaseSchema(false);
        List<SlotReference> slotFromColumn = createSlotsVectorized(baseSchema);

        ImmutableList.Builder<Slot> slots = ImmutableList.builder();
        for (int i = 0; i < baseSchema.size(); i++) {
            final int index = i;
            Column col = baseSchema.get(i);
            Pair<Long, String> key = Pair.of(selectedIndexId, col.getName());
            Slot slot = cacheSlotWithSlotName.computeIfAbsent(key, k -> slotFromColumn.get(index));
            slots.add(slot);
            if (colToSubPathsMap.containsKey(key.getValue())) {
                for (List<String> subPath : colToSubPathsMap.get(key.getValue())) {
                    if (!subPath.isEmpty()) {
                        SlotReference slotReference = SlotReference.fromColumn(table, col, qualified())
                                .withSubPath(subPath);
                        slots.add(slotReference);
                        subPathToSlotMap.computeIfAbsent(slot, k -> Maps.newHashMap())
                                .put(subPath, slotReference);
                    }
                }
            }
        }

        if (!isIncrementalScan) {
            // Inject virtual stream hidden columns.
            SlotReference seqColRef = (SlotReference) new Alias(new BigIntLiteral(-1L), Column.STREAM_SEQ_COL).toSlot();
            slots.add(seqColRef.withColumn(Column.STREAM_SEQ_VIRTUAL_COLUMN));

            SlotReference changeTypeColRef = (SlotReference) new Alias(new VarcharLiteral("APPEND"),
                    Column.STREAM_CHANGE_TYPE_COL).toSlot();
            slots.add(changeTypeColRef.withColumn(Column.STREAM_CHANGE_TYPE_VIRTUAL_COLUMN));
        }

        return slots.build();
    }

    public boolean isIncrementalScan() {
        return isIncrementalScan;
    }

    @Override
    public LogicalOlapTableStreamScan withManuallySpecifiedTabletIds(List<Long> manuallySpecifiedTabletIds) {
        return new LogicalOlapTableStreamScan(relationId, (Table) table, qualifier,
                Optional.empty(), Optional.of(getLogicalProperties()),
                selectedPartitionIds, partitionPruned, selectedTabletIds,
                selectedIndexId, indexSelected, preAggStatus, manuallySpecifiedPartitions,
                hints, cacheSlotWithSlotName, tableSample, directMvScan, colToSubPathsMap,
                manuallySpecifiedTabletIds, cachedOutput, isIncrementalScan);
    }

    /**
     * Return a new scan with specified selected tablet ids.
     */
    public LogicalOlapTableStreamScan withSelectedTabletIds(List<Long> selectedTabletIds) {
        return new LogicalOlapTableStreamScan(relationId, (Table) table, qualifier,
                Optional.empty(), Optional.of(getLogicalProperties()),
                selectedPartitionIds, partitionPruned, selectedTabletIds,
                selectedIndexId, indexSelected, preAggStatus, manuallySpecifiedPartitions,
                hints, cacheSlotWithSlotName, tableSample, directMvScan, colToSubPathsMap,
                manuallySpecifiedTabletIds, cachedOutput, isIncrementalScan);
    }

    /**
     * Return a new scan with cached output slots.
     */
    public LogicalOlapTableStreamScan withCachedOutput(List<Slot> outputSlots) {
        return new LogicalOlapTableStreamScan(relationId, (Table) table, qualifier,
                Optional.empty(), Optional.empty(),
                selectedPartitionIds, partitionPruned, selectedTabletIds,
                selectedIndexId, indexSelected, preAggStatus, manuallySpecifiedPartitions,
                hints, cacheSlotWithSlotName, tableSample, directMvScan, colToSubPathsMap,
                manuallySpecifiedTabletIds, Optional.of(outputSlots), isIncrementalScan);
    }

    /**
     * Return a new scan with specified pre-aggregation status.
     */
    public LogicalOlapTableStreamScan withPreAggStatus(PreAggStatus preAggStatus) {
        return new LogicalOlapTableStreamScan(relationId, (Table) table, qualifier,
                Optional.empty(), Optional.of(getLogicalProperties()),
                selectedPartitionIds, partitionPruned, selectedTabletIds,
                selectedIndexId, indexSelected, preAggStatus, manuallySpecifiedPartitions,
                hints, cacheSlotWithSlotName, tableSample, directMvScan, colToSubPathsMap,
                manuallySpecifiedTabletIds, cachedOutput, isIncrementalScan);
    }

    @Override
    public LogicalOlapTableStreamScan withGroupExpression(Optional<GroupExpression> groupExpression) {
        return new LogicalOlapTableStreamScan(relationId, (Table) table, qualifier,
                groupExpression, Optional.of(getLogicalProperties()),
                selectedPartitionIds, partitionPruned, selectedTabletIds,
                selectedIndexId, indexSelected, preAggStatus, manuallySpecifiedPartitions,
                hints, cacheSlotWithSlotName, tableSample, directMvScan, colToSubPathsMap,
                manuallySpecifiedTabletIds, cachedOutput, isIncrementalScan);
    }

    @Override
    public Plan withGroupExprLogicalPropChildren(Optional<GroupExpression> groupExpression,
            Optional<LogicalProperties> logicalProperties, List<Plan> children) {
        return new LogicalOlapTableStreamScan(relationId, (Table) table, qualifier,
                groupExpression, logicalProperties,
                selectedPartitionIds, partitionPruned, selectedTabletIds,
                selectedIndexId, indexSelected, preAggStatus, manuallySpecifiedPartitions,
                hints, cacheSlotWithSlotName, tableSample, directMvScan, colToSubPathsMap,
                manuallySpecifiedTabletIds, cachedOutput, isIncrementalScan);
    }

    @Override
    public LogicalOlapTableStreamScan withRelationId(RelationId relationId) {
        return new LogicalOlapTableStreamScan(relationId, (Table) table, qualifier,
                Optional.empty(), Optional.empty(),
                selectedPartitionIds, false, selectedTabletIds,
                selectedIndexId, indexSelected, preAggStatus, manuallySpecifiedPartitions,
                hints, Maps.newHashMap(), tableSample, directMvScan, colToSubPathsMap,
                selectedTabletIds, cachedOutput, isIncrementalScan);
    }

    @Override
    public <R, C> R accept(PlanVisitor<R, C> visitor, C context) {
        return visitor.visitLogicalOlapTableStreamScan(this, context);
    }

    @Override
    public boolean equals(Object o) {
        if (this == o) {
            return true;
        }
        if (o == null || getClass() != o.getClass()) {
            return false;
        }
        if (!super.equals(o)) {
            return false;
        }
        LogicalOlapTableStreamScan that = (LogicalOlapTableStreamScan) o;
        return Objects.equals(isIncrementalScan, that.isIncrementalScan)
                && Objects.equals(cachedOutput, that.cachedOutput);
    }

    @Override
    public int hashCode() {
        return Objects.hash(super.hashCode(), isIncrementalScan, cachedOutput);
    }
}
