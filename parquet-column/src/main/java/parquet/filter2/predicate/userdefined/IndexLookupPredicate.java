package parquet.filter2.predicate.userdefined;

import parquet.common.schema.ColumnPath;
import parquet.schema.MessageType;

/**
 * Interface to be implemented by all UDPs that does index lookup.The UDP should be able to provide
 * the metadata around the index it is going to read. This metadata is used to identify the embedded
 * table (that contains the index) to be used for the evaluation of the predicate.
 *
 * For example, an instance of the IndexLookupPredicate could need the 'suffix-array' type index
 * created against the 'doc_value' column of the main table. Further, it would need the 'term' and
 * 'pos' columns to be read from the index.
 *
 * Created by abennett on 26/6/15.
 */
public interface IndexLookupPredicate extends EmbeddedTableDataConsumer {

    /**
     * @return The type of index that the predicate needs for its functioning. The only index type
     * supported currently is 'suffix-array'.
     */
    public String getIndexTypeName();

    /**
     * @return The projection list to be used while reading the index table.
     */
    public MessageType getRequestedSchema();

    /**
     * @return The column of base table on which the index is created.
     */
    public ColumnPath getIndexedColumn();
}
