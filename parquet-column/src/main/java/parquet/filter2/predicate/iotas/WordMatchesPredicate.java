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
public class WordMatchesPredicate extends SuffixArrayPredicate {

    private static final String INDEX_SCHEMA = SuffixArrayIndexSchemaBuilder.newBuilder()
            .withTermColumn()
            .withDocIdsColumn()
            .withStartsWithColumn().build();

    private final String columnName;
    private final String term;

    public WordMatchesPredicate(String indexTableName, String columnName, String term) {
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
            ColumnReader flagColumn = getWordStartFlagColumn(columnReaderFactory);
            long[] pos = SuffixArrayUtils.findTermMatchPos(termColumn, term);
            SortedIntIterator docIdIterator =
                    SuffixArrayUtils.getFilteredUniqueIdList(docIdColumn, pos, flagColumn);
            SortedIntQueue docIds = new SortedIntQueue(docIdIterator);
            docIds.init();
            setFilteredDocIds(docIds);
        } catch (IOException e) {
            throwCorruptIndexException(e);
        }
    }

}
