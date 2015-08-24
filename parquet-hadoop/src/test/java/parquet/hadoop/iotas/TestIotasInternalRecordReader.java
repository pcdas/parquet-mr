package parquet.hadoop.iotas;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.Path;
import org.junit.Test;
import parquet.column.ParquetProperties;
import parquet.column.iotas.EmbeddedTableColumnReaderFactory;
import parquet.example.data.Group;
import parquet.example.data.simple.SimpleGroupFactory;
import parquet.filter2.compat.FilterCompat;
import parquet.filter2.predicate.*;
import parquet.filter2.predicate.userdefined.IndexLookupPredicate;
import parquet.hadoop.ParquetFileReader;
import parquet.hadoop.ParquetReader;
import parquet.hadoop.ParquetWriter;
import parquet.hadoop.example.GroupReadSupport;
import parquet.hadoop.example.GroupWriteSupport;
import parquet.hadoop.metadata.ParquetMetadata;
import parquet.schema.MessageType;

import java.io.File;
import java.io.IOException;
import java.io.Serializable;

import static org.mockito.Mockito.*;
import static parquet.hadoop.metadata.CompressionCodecName.UNCOMPRESSED;
import static parquet.schema.MessageTypeParser.parseMessageType;

/**
 * Created by abennett on 8/7/15.
 */
public class TestIotasInternalRecordReader {

    @Test
    public void testSingleFilterInit() throws IOException {

        Path path = createFile(10);
        Operators.Column<Integer> c1 = FilterApi.intColumn("doc_id");
        TestUDP testUDP = mock(TestUDP.class);
        when(testUDP.canDrop(any(Statistics.class))).thenReturn(false);
        when(testUDP.inverseCanDrop(any(Statistics.class))).thenReturn(false);
        when(testUDP.keep(anyInt())).thenReturn(true);
        FilterPredicate predicate = FilterApi.userDefined(c1, testUDP);

        readFile(path, predicate, 10);
        verify(testUDP).init(any(EmbeddedTableColumnReaderFactory.class));
    }

    @Test
    public void testMultipleFilterInit() throws IOException {

        Path path = createFile(10);
        Operators.Column<Integer> c1 = FilterApi.intColumn("doc_id");

        TestUDP testUDP1 = mock(TestUDP.class);
        when(testUDP1.canDrop(any(Statistics.class))).thenReturn(false);
        when(testUDP1.inverseCanDrop(any(Statistics.class))).thenReturn(false);
        when(testUDP1.keep(anyInt())).thenReturn(true);
        FilterPredicate userDefinedPredicate1 = FilterApi.userDefined(c1, testUDP1);

        TestUDP testUDP2 = mock(TestUDP.class);
        when(testUDP2.canDrop(any(Statistics.class))).thenReturn(false);
        when(testUDP2.inverseCanDrop(any(Statistics.class))).thenReturn(false);
        when(testUDP2.keep(anyInt())).thenReturn(true);
        FilterPredicate userDefinedPredicate2 = FilterApi.userDefined(c1, testUDP2);

        FilterPredicate predicate = FilterApi.and(userDefinedPredicate1, userDefinedPredicate2);

        readFile(path, predicate, 10);
        verify(testUDP1).init(any(EmbeddedTableColumnReaderFactory.class));
        verify(testUDP2).init(any(EmbeddedTableColumnReaderFactory.class));
    }

    @Test
    public void testFilterInitCount() throws IOException {

        Path path = createFile(10000);
        Configuration configuration = new Configuration();
        ParquetMetadata metadata = ParquetFileReader.readFooter(configuration, path);
        int numRowGroups = metadata.getBlocks().size();
        Operators.Column<Integer> c1 = FilterApi.intColumn("doc_id");

        TestUDP testUDP = mock(TestUDP.class);
        when(testUDP.canDrop(any(Statistics.class))).thenReturn(false);
        when(testUDP.inverseCanDrop(any(Statistics.class))).thenReturn(false);
        when(testUDP.keep(anyInt())).thenReturn(true);
        FilterPredicate predicate = FilterApi.userDefined(c1, testUDP);

        readFile(path, predicate, 10000);
        verify(testUDP, times(numRowGroups)).init(any(EmbeddedTableColumnReaderFactory.class));
    }

    private Path createFile(int n) throws IOException {
        File testFile = new File("target/test/MultiSchema/testIIPR.par").getAbsoluteFile();
        testFile.delete();

        Path path = new Path(testFile.toURI());
        Configuration configuration = new Configuration();
        MessageType schema = parseMessageType(
                "message test { "
                        + "required int32 doc_id; "
                        + "required binary data (UTF8); "
                        + "} ");
        GroupWriteSupport.setSchema(schema, configuration);
        SimpleGroupFactory f = new SimpleGroupFactory(schema);
        ParquetWriter<Group> writer = new ParquetWriter<Group>(
                path, new GroupWriteSupport(), UNCOMPRESSED, 10 * 1024, 1024,
                1024, true, false, ParquetProperties.WriterVersion.PARQUET_2_0, configuration);
        for (int i = 0; i < n; i++) {
            writer.write(
                    f.newGroup()
                            .append("doc_id", i)
                            .append("data", "mesg:" + i));
        }
        writer.close();
        return path;
    }

    private void readFile(Path path, FilterPredicate predicate, int n) throws IOException {
        Configuration configuration = new Configuration();
        ParquetReader < Group > reader = ParquetReader.builder(
                new GroupReadSupport(), path).withConf(configuration).withFilter(FilterCompat.get(predicate)).build();
        for (int i = 0; i < n; i++) {
            reader.read();
        }
        reader.close();

    }

    private static abstract class TestUDP extends UserDefinedPredicate<Integer> implements IndexLookupPredicate, Serializable {
    }

}
