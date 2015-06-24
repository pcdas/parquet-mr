package parquet.hadoop.iotas;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.Path;
import parquet.column.ColumnDescriptor;
import parquet.column.iotas.MultiSchemaPageReadStore;
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
public class MultiSchemaParquetFileReader extends ParquetFileReader{

    private final List<String> embeddedSchemas;
    private final Map<String, List<BlockMetaData>> embeddedBlocks;
    private final Map<String, Map<ColumnPath, ColumnDescriptor>> embeddedColumnsMap;
    private MultiSchemaPageReadStore currentPageReadStore;

    public MultiSchemaParquetFileReader(Configuration configuration, Path filePath, List<BlockMetaData> blocks, List<ColumnDescriptor> columns) throws IOException {
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
    public MultiSchemaPageReadStore readNextRowGroup() throws IOException {
        if (currentBlock == blocks.size()) {
            return null;
        }
        BlockMetaData block = blocks.get(currentBlock);
        PageReadStore mainPageReadStore = readRowGroupFromBlock(block, paths);
        MultiSchemaPageReadStore pageReadStore = new MultiSchemaPageReadStore(mainPageReadStore);
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

    public MultiSchemaPageReadStore getCurrentPageReadStore() {
        return this.currentPageReadStore;
    }

    public ParquetMetadata readEmbeddedFooter(long footerPosition, ParquetMetadataConverter.MetadataFilter filter) throws IOException {
        f.seek(footerPosition);
        return converter.readParquetMetadata(f, filter);
    }


}
