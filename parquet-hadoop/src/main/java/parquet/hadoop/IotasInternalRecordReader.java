package parquet.hadoop;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.Path;
import parquet.column.iotas.FilterUtils;
import parquet.column.iotas.EmbeddedTableColumnReaderFactory;
import parquet.filter.UnboundRecordFilter;
import parquet.filter2.compat.FilterCompat;
import parquet.filter2.predicate.FilterPredicate;
import parquet.filter2.predicate.Operators;
import parquet.filter2.predicate.UserDefinedPredicate;
import parquet.filter2.predicate.userdefined.EmbeddedTableDataConsumer;
import parquet.format.converter.ParquetMetadataConverter;
import parquet.hadoop.api.ReadSupport;
import parquet.hadoop.metadata.BlockMetaData;
import parquet.hadoop.metadata.ParquetMetadata;
import parquet.schema.MessageType;

import java.io.IOException;
import java.util.List;
import java.util.Map;

import static parquet.Preconditions.checkNotNull;

/**
 * This class adds the text search platform related functionality to the
 * InternalParquetRecordReader. This added functionality includes:
 *  1. Reading of embedded schema in the initialization phase
 *  2. Initializing the filters with a handle to column readers from the embedded schema
 *
 * Created by abennett on 8/7/15.
 */
class IotasInternalRecordReader<T> extends InternalParquetRecordReader<T>{

    public IotasInternalRecordReader(ReadSupport<T> readSupport, FilterCompat.Filter filter) {
        super(readSupport, filter);
    }

    public IotasInternalRecordReader(ReadSupport<T> readSupport) {
        super(readSupport);
    }

    @Deprecated
    public IotasInternalRecordReader(ReadSupport<T> readSupport, UnboundRecordFilter filter) {
        super(readSupport, filter);
    }

    @Override
    public void initialize(MessageType fileSchema,
                           Map<String, String> fileMetadata,
                           Path file, List<BlockMetaData> blocks, Configuration configuration)
            throws IOException {
        super.initialize(fileSchema, fileMetadata, file, blocks, configuration);
        checkNotNull(readContext, "readContext");
        //set batch read flag
        super.useBatchedRead = readContext.useBatchedRead();
        initializeEmbeddedSchema(readContext, fileMetadata);
    }

    @Override
    protected boolean checkRead() throws IOException {
        if (super.checkRead()) {
            initializeFilters();
            return true;
        }
        return false;
    }

    /**
     * Triggers reading of embedded schema by MultiSchemaParquetFileReader
     * @param readContext
     * @throws IOException
     */
    private void initializeEmbeddedSchema(ReadSupport.ReadContext readContext, Map<String, String> fileMetadata) throws IOException {
        for(ReadSupport.EmbeddedTableSchema embeddedTableSchema : readContext.getEmbeddedTableSchemaList()) {
            long footerPosition = embeddedTableSchema.getFooterPosition();
            if (footerPosition == 0) {
                String footerPositionKey = embeddedTableSchema.getFooterPositionKey();
                footerPosition = Long.parseLong(fileMetadata.get(footerPositionKey));
            }
            ParquetMetadata metadata = reader.readEmbeddedFooter(
                    footerPosition, ParquetMetadataConverter.NO_FILTER);
            MessageType indexSchema = metadata.getFileMetaData().getSchema();
            reader.addEmbeddedSchema(indexSchema.getName(),
                    metadata.getBlocks(),
                    embeddedTableSchema.getRequestedSchema().getColumns());
        }
    }

    /**
     * Initializes the filters with column reader factory
     */
    private void initializeFilters() {
        if (filter instanceof FilterCompat.FilterPredicateCompat) {
            List<FilterPredicate> predicates = FilterUtils.getLeafNodes(
                    ((FilterCompat.FilterPredicateCompat) filter).getFilterPredicate());
            for(FilterPredicate predicate: predicates) {
                if (predicate instanceof Operators.UserDefined) {
                    UserDefinedPredicate udp = ((Operators.UserDefined) predicate)
                            .getUserDefinedPredicate();
                    if (udp instanceof EmbeddedTableDataConsumer) {
                        ((EmbeddedTableDataConsumer) udp).init(new EmbeddedTableColumnReaderFactory(
                                reader.getCurrentPageReadStore(),
                                useBatchedRead));
                    }
                }
            }
        }
    }


}
