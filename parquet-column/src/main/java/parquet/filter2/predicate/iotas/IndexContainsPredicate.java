package parquet.filter2.predicate.iotas;

import parquet.column.ColumnReader;
import parquet.column.iotas.EmbeddedTableColumnReaderFactory;
import parquet.filter2.predicate.iotas.index.SortedIntIterator;
import parquet.filter2.predicate.iotas.index.SortedIntQueue;
import parquet.filter2.predicate.iotas.index.SuffixArrayUtils;

import java.io.IOException;

import static parquet.Preconditions.checkNotNull;

/**
 * Created by abennett on 14/7/15.
 */
public class IndexContainsPredicate extends SuffixArrayPredicate {

    private static final String INDEX_SCHEMA = SuffixArrayIndexSchemaBuilder.newBuilder()
            .withTermColumn()
            .withDocIdsColumn().build();

    private final String columnName;
    private final String term;

    public IndexContainsPredicate(String indexTableName, String columnName, String term) {
        super(INDEX_SCHEMA, indexTableName, columnName);
        checkNotNull(indexTableName, "indexTableName");
        this.columnName = checkNotNull(columnName, "columnName");
        this.term = checkNotNull(term, "term").toLowerCase();
    }

    @Override
    public void init(EmbeddedTableColumnReaderFactory columnReaderFactory) {
        try {
            ColumnReader termColumn = getTermColumn(columnReaderFactory);
            ColumnReader docIdColumn = getDocIdColumn(columnReaderFactory);
            long[] pos = SuffixArrayUtils.findTermStartsWithPos(termColumn, term);
            SortedIntIterator docIdIterator = SuffixArrayUtils.getUniqueIdList(docIdColumn, pos);
            SortedIntQueue docIds = new SortedIntQueue(docIdIterator);
            docIds.init();
            setFilteredDocIds(docIds);
        } catch (IOException e) {
            throwCorruptIndexException(e);
        }
    }
}
