/* 
 * Licensed to the Apache Software Foundation (ASF) under one
 * or more contributor license agreements.  See the NOTICE file
 * distributed with this work for additional information
 * regarding copyright ownership.  The ASF licenses this file
 * to you under the Apache License, Version 2.0 (the
 * "License"); you may not use this file except in compliance
 * with the License.  You may obtain a copy of the License at
 * 
 *   http://www.apache.org/licenses/LICENSE-2.0
 * 
 * Unless required by applicable law or agreed to in writing,
 * software distributed under the License is distributed on an
 * "AS IS" BASIS, WITHOUT WARRANTIES OR CONDITIONS OF ANY
 * KIND, either express or implied.  See the License for the
 * specific language governing permissions and limitations
 * under the License.
 */
package parquet.hadoop.iotas;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.Path;
import parquet.column.ColumnDescriptor;
import parquet.column.iotas.EmbeddedTablePageReadStore;
import parquet.column.page.PageReadStore;
import parquet.common.schema.ColumnPath;
import parquet.format.converter.ParquetMetadataConverter;
import parquet.hadoop.ParquetFileReader;
import parquet.hadoop.metadata.BlockMetaData;
import parquet.hadoop.metadata.ParquetMetadata;

import java.io.IOException;
import java.util.ArrayList;
import java.util.HashMap;
import java.util.List;
import java.util.Map;

/**
 * Created by abennett on 8/7/15.
 */
public class ParquetEmbeddedTableFileReader extends ParquetFileReader{

    private final List<String> embeddedSchemas;
    private final Map<String, List<BlockMetaData>> embeddedBlocks;
    private final Map<String, Map<ColumnPath, ColumnDescriptor>> embeddedColumnsMap;
    private EmbeddedTablePageReadStore currentPageReadStore;

    public ParquetEmbeddedTableFileReader(Configuration configuration, Path filePath, List<BlockMetaData> blocks, List<ColumnDescriptor> columns) throws IOException {
        super(configuration, filePath, blocks, columns);
        this.embeddedSchemas = new ArrayList<String>();
        this.embeddedBlocks = new HashMap<String, List<BlockMetaData>>();
        this.embeddedColumnsMap = new HashMap<String, Map<ColumnPath, ColumnDescriptor>>();
    }

    public void addEmbeddedSchema(String schemaName, List<BlockMetaData> blocks, List<ColumnDescriptor> columns) {
        Map<ColumnPath, ColumnDescriptor> paths = new HashMap<ColumnPath, ColumnDescriptor>();
        for (ColumnDescriptor col : columns) {
            paths.put(ColumnPath.get(col.getPath()), col);
        }
        this.embeddedColumnsMap.put(schemaName, paths);
        this.embeddedBlocks.put(schemaName, blocks);
        this.embeddedSchemas.add(schemaName);
    }

    @Override
    public EmbeddedTablePageReadStore readNextRowGroup() throws IOException {
        if (currentBlock == blocks.size()) {
            return null;
        }
        BlockMetaData block = blocks.get(currentBlock);
        PageReadStore mainPageReadStore = readRowGroupFromBlock(block, paths);
        EmbeddedTablePageReadStore pageReadStore = new EmbeddedTablePageReadStore(mainPageReadStore);
        for (String schema: this.embeddedSchemas) {
            PageReadStore embeddedPageReadStore = readRowGroupFromBlock(
                    this.embeddedBlocks.get(schema).get(currentBlock),
                    this.embeddedColumnsMap.get(schema));
            pageReadStore.addPageReadStore(schema, embeddedPageReadStore);
        }
        ++currentBlock;
        this.currentPageReadStore = pageReadStore;
        return pageReadStore;
    }

    public EmbeddedTablePageReadStore getCurrentPageReadStore() {
        return this.currentPageReadStore;
    }

    public ParquetMetadata readEmbeddedFooter(long footerPosition, ParquetMetadataConverter.MetadataFilter filter) throws IOException {
        f.seek(footerPosition);
        return converter.readParquetMetadata(f, filter);
    }


}
