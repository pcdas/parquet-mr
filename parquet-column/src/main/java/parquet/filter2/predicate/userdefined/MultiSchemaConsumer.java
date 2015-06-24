package parquet.filter2.predicate.userdefined;

import parquet.column.iotas.MultiSchemaColumnReaderFactory;

/**
 * To be implemented by UDPs or any other component that would need to read
 * columns from the embedded schema
 *
 * Created by abennett on 8/7/15.
 */
public interface MultiSchemaConsumer {

    public void init(MultiSchemaColumnReaderFactory columnReaderFactory);

}
