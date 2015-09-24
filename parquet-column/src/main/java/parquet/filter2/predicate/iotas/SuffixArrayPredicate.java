package parquet.filter2.predicate.iotas;

import parquet.column.ColumnDescriptor;
import parquet.column.ColumnReader;
import parquet.column.iotas.EmbeddedTableColumnReaderFactory;
import parquet.common.schema.ColumnPath;
import parquet.filter2.predicate.Statistics;
import parquet.filter2.predicate.UserDefinedPredicate;
import parquet.filter2.predicate.iotas.index.SortedIntIterator;
import parquet.filter2.predicate.iotas.index.SortedIntQueue;
import parquet.filter2.predicate.iotas.index.SuffixArrayUtils;
import parquet.schema.MessageType;
import parquet.schema.MessageTypeParser;
import parquet.schema.PrimitiveType;

import java.io.IOException;
import java.io.Serializable;

import static parquet.Preconditions.checkNotNull;

/**
 * Created by abennett on 14/7/15.
 */
public abstract class SuffixArrayPredicate extends UserDefinedPredicate<Integer> implements IndexLookupPredicate, Serializable{

    //TODO: revert it back to suffix-array
    public static final String INDEX_NAME = "suffixArray";
    public static final String TABLE_NAME = "TermIndexTable";
    public static final String TERM_COLUMN = "term";
    public static final String DOC_IDS_COLUMN = "doc_ids";
    public static final String STARTS_WITH_FLAG_COLUMN = "starts_with";

    private final String indexSchema;
    private final String indexTableName;
    private final String columnName;
    private final ColumnPath indexColumn;
    private transient SortedIntQueue docIds;

    public SuffixArrayPredicate(String indexSchema, String indexTableName, String columnName) {
        this.indexSchema = indexSchema;
        this.indexColumn = ColumnPath.fromDotString(columnName);
        this.indexTableName = indexTableName;
        this.columnName = columnName;
    }

    @Override
    public abstract void init(EmbeddedTableColumnReaderFactory columnReaderFactory);

    @Override
    public String getIndexTypeName() {
        return INDEX_NAME;
    }

    @Override
    public MessageType getRequestedSchema() {
        return MessageTypeParser.parseMessageType(indexSchema);
    }

    @Override
    public ColumnPath getIndexedColumn() {
        return indexColumn;
    }

    //TODO: this will go through autoboxing. look for workarounds
    @Override
    public boolean keep(Integer value) {
        return docIds.checkAndPop(value);
    }

    @Override
    public boolean canDrop(Statistics<Integer> statistics) {
        return false;
    }

    @Override
    public boolean inverseCanDrop(Statistics<Integer> statistics) {
        return false;
    }

    protected void setFilteredDocIds(SortedIntQueue docIds) {
        this.docIds = docIds;
    }

    protected ColumnReader getTermColumn(EmbeddedTableColumnReaderFactory columnReaderFactory) {
        String[] colPath = {TERM_COLUMN};
        ColumnDescriptor termColumn = new ColumnDescriptor(colPath, PrimitiveType.PrimitiveTypeName.BINARY, 0, 0);
        ColumnReader termReader = columnReaderFactory.getColumnReader(indexTableName, termColumn);
        return termReader;
    }

    protected ColumnReader getDocIdColumn(EmbeddedTableColumnReaderFactory columnReaderFactory) {
        String[] idPath = {DOC_IDS_COLUMN};
        ColumnDescriptor idColumn = new ColumnDescriptor(idPath, PrimitiveType.PrimitiveTypeName.BINARY, 0, 0);
        ColumnReader idReader = columnReaderFactory.getColumnReader(indexTableName, idColumn);
        return idReader;
    }

    protected ColumnReader getWordStartFlagColumn(EmbeddedTableColumnReaderFactory columnReaderFactory) {
        String[] startsWithFlagPath = {STARTS_WITH_FLAG_COLUMN};
        ColumnDescriptor startsWithFlagColumn = new ColumnDescriptor(startsWithFlagPath, PrimitiveType.PrimitiveTypeName.BINARY, 0, 0);
        ColumnReader startsWithFlagReader = columnReaderFactory.getColumnReader(indexTableName, startsWithFlagColumn);
        return startsWithFlagReader;
    }

    protected void throwCorruptIndexException(Exception e) {
        throw new RuntimeException(String.format("Corrupt %s index on column %s", INDEX_NAME, columnName), e);
    }
}
