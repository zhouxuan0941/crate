/*
 * Licensed to Crate.io Inc. (Crate) under one or more contributor license
 * agreements.  See the NOTICE file distributed with this work for additional
 * information regarding copyright ownership.  Crate licenses this file to
 * you under the Apache License, Version 2.0 (the "License");  you may not
 * use this file except in compliance with the License.  You may obtain a
 * copy of the License at
 *
 *   http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS, WITHOUT
 * WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.  See the
 * License for the specific language governing permissions and limitations
 * under the License.
 *
 * However, to use any modules in this file marked as "Enterprise Features",
 * Crate must have given you permission to enable and use such Enterprise
 * Features and you must have a valid Enterprise or Subscription Agreement
 * with Crate.  If you enable or use the Enterprise Features, you represent
 * and warrant that you have a valid Enterprise or Subscription Agreement
 * with Crate.  Your use of the Enterprise Features if governed by the terms
 * and conditions of your Enterprise or Subscription Agreement with Crate.
 */

package io.crate.operation.reference.sys.shard.unassigned;

import com.google.common.collect.ImmutableMap;
import io.crate.metadata.ColumnIdent;
import io.crate.metadata.ReferenceImplementation;
import io.crate.metadata.RowCollectExpression;
import io.crate.metadata.RowContextCollectorExpression;
import io.crate.metadata.expressions.RowCollectExpressionFactory;
import io.crate.metadata.shard.unassigned.UnassignedShard;
import io.crate.metadata.sys.SysNodesTableInfo;
import io.crate.metadata.sys.SysShardsTableInfo;
import org.apache.lucene.util.BytesRef;

import java.util.Map;

public class UnassignedShardsExpressionFactories {

    private UnassignedShardsExpressionFactories() {
    }

    public static Map<ColumnIdent, RowCollectExpressionFactory> getSysShardsTableInfoFactories() {
        return ImmutableMap.<ColumnIdent, RowCollectExpressionFactory>builder()
            .put(SysShardsTableInfo.Columns.SCHEMA_NAME, new RowCollectExpressionFactory() {

                @Override
                public RowContextCollectorExpression create() {
                    return new RowContextCollectorExpression<UnassignedShard, BytesRef>() {
                        @Override
                        public BytesRef value() {
                            return new BytesRef(this.row.schemaName());
                        }
                    };
                }
            })
            .put(SysShardsTableInfo.Columns.TABLE_NAME, new RowCollectExpressionFactory() {
                @Override
                public RowContextCollectorExpression create() {
                    return new RowContextCollectorExpression<UnassignedShard, BytesRef>() {
                        @Override
                        public BytesRef value() {
                            return new BytesRef(this.row.tableName());
                        }
                    };
                }
            })
            .put(SysShardsTableInfo.Columns.PARTITION_IDENT, new RowCollectExpressionFactory() {
                @Override
                public RowContextCollectorExpression create() {
                    return new RowContextCollectorExpression<UnassignedShard, BytesRef>() {
                        @Override
                        public BytesRef value() {
                            return new BytesRef(this.row.partitionIdent());
                        }
                    };
                }
            })
            .put(SysShardsTableInfo.Columns.ID, new RowCollectExpressionFactory() {
                @Override
                public RowContextCollectorExpression create() {
                    return new RowContextCollectorExpression<UnassignedShard, Integer>() {
                        @Override
                        public Integer value() {
                            return this.row.id();
                        }
                    };
                }
            })
            .put(SysShardsTableInfo.Columns.NUM_DOCS, new RowCollectExpressionFactory() {
                @Override
                public RowContextCollectorExpression create() {
                    return new RowContextCollectorExpression<UnassignedShard, Long>() {
                        @Override
                        public Long value() {
                            return 0L;
                        }
                    };
                }
            })
            .put(SysShardsTableInfo.Columns.PRIMARY, new RowCollectExpressionFactory() {
                @Override
                public RowContextCollectorExpression create() {
                    return new RowContextCollectorExpression<UnassignedShard, Boolean>() {
                        @Override
                        public Boolean value() {
                            return row.primary();
                        }
                    };
                }
            })
            .put(SysShardsTableInfo.Columns.RELOCATING_NODE, new RowCollectExpressionFactory() {
                @Override
                public RowContextCollectorExpression create() {
                    return new RowContextCollectorExpression<UnassignedShard, BytesRef>() {
                        @Override
                        public BytesRef value() {
                            return null;
                        }
                    };
                }
            })
            .put(SysShardsTableInfo.Columns.SIZE, new RowCollectExpressionFactory() {
                @Override
                public RowContextCollectorExpression create() {
                    return new RowContextCollectorExpression<UnassignedShard, Long>() {
                        @Override
                        public Long value() {
                            return 0L;
                        }
                    };
                }
            })
            .put(SysShardsTableInfo.Columns.STATE, new RowCollectExpressionFactory() {
                @Override
                public RowContextCollectorExpression create() {
                    return new RowContextCollectorExpression<UnassignedShard, BytesRef>() {
                        @Override
                        public BytesRef value() {
                            return row.state();
                        }
                    };
                }
            })
            .put(SysShardsTableInfo.Columns.ROUTING_STATE, new RowCollectExpressionFactory() {
                @Override
                public RowContextCollectorExpression create() {
                    return new RowContextCollectorExpression<UnassignedShard, BytesRef>() {
                        @Override
                        public BytesRef value() {
                            return row.state();
                        }
                    };
                }
            })
            .put(SysShardsTableInfo.Columns.ORPHAN_PARTITION, new RowCollectExpressionFactory() {
                @Override
                public RowContextCollectorExpression create() {
                    return new RowContextCollectorExpression<UnassignedShard, Boolean>() {
                        @Override
                        public Boolean value() {
                            return this.row.orphanedPartition();
                        }
                    };
                }
            })
            .put(SysShardsTableInfo.Columns.RECOVERY, new RowCollectExpressionFactory() {
                @Override
                public RowContextCollectorExpression create() {
                    return new RowContextCollectorExpression() {
                        @Override
                        public Object value() {
                            return null;
                        }

                        @Override
                        public ReferenceImplementation getChildImplementation(String name) {
                            return this;
                        }
                    };
                }
            })
            .put(SysNodesTableInfo.SYS_COL_IDENT, new RowCollectExpressionFactory() {
                @Override
                public RowContextCollectorExpression create() {
                    return new RowContextCollectorExpression() {
                        @Override
                        public Object value() {
                            return null;
                        }

                        @Override
                        public ReferenceImplementation getChildImplementation(String name) {
                            return this;
                        }
                    };
                }
            })
            .put(SysShardsTableInfo.Columns.PATH, new RowCollectExpressionFactory() {
                @Override
                public RowCollectExpression create() {
                    return new RowContextCollectorExpression() {
                        @Override
                        public Object value() {
                            return null;
                        }
                    };
                }
            })
            .put(SysShardsTableInfo.Columns.BLOB_PATH, new RowCollectExpressionFactory() {
                @Override
                public RowCollectExpression create() {
                    return new RowContextCollectorExpression() {
                        @Override
                        public Object value() {
                            return null;
                        }
                    };
                }
            })
            .build();
    }
}
