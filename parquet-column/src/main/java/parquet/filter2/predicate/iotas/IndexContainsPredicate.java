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
import java.util.List;

import static parquet.Preconditions.checkNotNull;

/**
 * Created by abennett on 14/7/15.
 */
public class IndexContainsPredicate extends UserDefinedPredicate<Integer> implements IndexLookupPredicate, Serializable{

    private final String indexName;
    private final String indexSchema;
    private final ColumnPath indexColumn;
    private final String term;
    private SortedIntQueue docIds;
    private final String indexTableName;

    public IndexContainsPredicate(String columnName, String term) {
        this.term = checkNotNull(term, "term").toLowerCase();
//        indexName = "suffix-array";
        //TODO: revert it back to suffix-array or drive it from metadata
        indexName = "suffixArray";
        indexSchema = "message TermIndexTable {" +
                " required binary term (UTF8);" +
                " required binary doc_ids; }";
        indexColumn = ColumnPath.fromDotString(columnName);
        //TODO: should be set from metadata
        indexTableName = "suffixArrayIndexTable";
    }

    @Override
    public String getIndexTypeName() {
        return indexName;
    }

    @Override
    public MessageType getRequestedSchema() {
        return MessageTypeParser.parseMessageType(indexSchema);
    }

    @Override
    public ColumnPath getIndexedColumn() {
        return indexColumn;
    }

    @Override
    public void init(EmbeddedTableColumnReaderFactory columnReaderFactory) {
        try {
            String[] colPath = {"term"};
            ColumnDescriptor termColumn = new ColumnDescriptor(colPath, PrimitiveType.PrimitiveTypeName.BINARY, 0, 0);
            ColumnReader termReader = columnReaderFactory.getColumnReader(indexTableName, termColumn);
            //TODO: this path involves autoboxing. combine the two methods
            List<Long> pos = SuffixArrayUtils.findTermStartsWithPos(termReader, term);
            String[] idPath = {"doc_ids"};
            ColumnDescriptor idColumn = new ColumnDescriptor(idPath, PrimitiveType.PrimitiveTypeName.BINARY, 0, 0);
            ColumnReader idReader = columnReaderFactory.getColumnReader(indexTableName, idColumn);
            SortedIntIterator docIdIterator = SuffixArrayUtils.getUniqueIdList(idReader, pos);
            docIds = new SortedIntQueue(docIdIterator);
            docIds.init();
        } catch (IOException e) {
            throw new RuntimeException(String.format("Corrupt %s index on column %s", indexName, indexColumn.toString()), e);
        }
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
}
