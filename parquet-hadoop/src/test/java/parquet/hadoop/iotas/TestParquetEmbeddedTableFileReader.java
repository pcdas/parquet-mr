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
import org.apache.hadoop.fs.FSDataInputStream;
import org.apache.hadoop.fs.FileStatus;
import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.fs.Path;
import org.junit.Test;
import org.mockito.ArgumentCaptor;
import parquet.column.BatchColumnReader;
import parquet.column.ColumnDescriptor;
import parquet.column.ColumnReader;
import parquet.column.ParquetProperties;
import parquet.column.iotas.EmbeddedTableColumnReaderFactory;
import parquet.column.impl.ColumnReaderBatchingWrapper;
import parquet.example.data.Group;
import parquet.example.data.simple.SimpleGroupFactory;
import parquet.filter2.compat.FilterCompat;
import parquet.filter2.predicate.*;
import parquet.filter2.predicate.iotas.IndexLookupPredicate;
import parquet.hadoop.ParquetReader;
import parquet.hadoop.ParquetWriter;
import parquet.hadoop.api.InitContext;
import parquet.hadoop.example.GroupReadSupport;
import parquet.hadoop.example.GroupWriteSupport;
import parquet.schema.MessageType;
import parquet.schema.PrimitiveType;

import java.io.File;
import java.io.IOException;
import java.io.Serializable;
import java.util.ArrayList;
import java.util.List;
import java.util.Map;

import static org.junit.Assert.assertEquals;
import static org.mockito.Matchers.any;
import static org.mockito.Matchers.anyInt;
import static org.mockito.Mockito.*;
import static parquet.bytes.BytesUtils.readIntLittleEndian;
import static parquet.hadoop.ParquetFileWriter.MAGIC;
import static parquet.hadoop.metadata.CompressionCodecName.UNCOMPRESSED;
import static parquet.schema.MessageTypeParser.parseMessageType;

/**
 * Created by abennett on 8/7/15.
 */
public class TestParquetEmbeddedTableFileReader {

    @Test
    public void test() throws IOException {
        Path path = writeFile();
        long footerLoc = getFooterLocation(path);

        Operators.Column<Integer> c1 = FilterApi.intColumn("doc_id");
        TestUDP testUDP = mock(TestUDP.class);
        when(testUDP.canDrop(any(Statistics.class))).thenReturn(false);
        when(testUDP.inverseCanDrop(any(Statistics.class))).thenReturn(false);
        when(testUDP.keep(anyInt())).thenReturn(true);
        FilterPredicate predicate = FilterApi.userDefined(c1, testUDP);

        readFile(path, predicate, footerLoc);

        ArgumentCaptor<EmbeddedTableColumnReaderFactory> factoryCaptor = ArgumentCaptor.forClass(EmbeddedTableColumnReaderFactory.class);
        verify(testUDP).init(factoryCaptor.capture());
        String[] colPath = {"doc_id"};
        ColumnDescriptor c = new ColumnDescriptor(colPath, PrimitiveType.PrimitiveTypeName.INT32, 0, 0);
        ColumnReader columnReader = factoryCaptor.getValue().getColumnReader("test", c);
        assertEquals(columnReader.getTotalValueCount(), 10);
    }

    @Test
    public void testBatchUsage() throws IOException {
        Path path = writeFile();
        long footerLoc = getFooterLocation(path);

        Operators.Column<Integer> c1 = FilterApi.intColumn("doc_id");
        TestUDP testUDP = mock(TestUDP.class);
        when(testUDP.canDrop(any(Statistics.class))).thenReturn(false);
        when(testUDP.inverseCanDrop(any(Statistics.class))).thenReturn(false);
        when(testUDP.keep(anyInt())).thenReturn(true);
        FilterPredicate predicate = FilterApi.userDefined(c1, testUDP);

        batchReadFile(path, predicate, footerLoc);

        ArgumentCaptor<EmbeddedTableColumnReaderFactory> factoryCaptor = ArgumentCaptor.forClass(EmbeddedTableColumnReaderFactory.class);
        verify(testUDP).init(factoryCaptor.capture());
        String[] colPath = {"doc_id"};
        ColumnDescriptor c = new ColumnDescriptor(colPath, PrimitiveType.PrimitiveTypeName.INT32, 0, 0);
        ColumnReader columnReader = factoryCaptor.getValue().getColumnReader("test", c);
        assert(columnReader instanceof ColumnReaderBatchingWrapper);
        assert(((ColumnReaderBatchingWrapper) columnReader).getWrappedBatchColumnReader() instanceof BatchColumnReader);
    }

    private Path writeFile() throws IOException {
        File testFile = new File("target/test/MultiSchema/testMSPFR.par").getAbsoluteFile();
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
        for (int i = 0; i < 10; i++) {
            writer.write(
                    f.newGroup()
                            .append("doc_id", i)
                            .append("data", "mesg:" + i));
        }
        writer.close();
        return path;
    }

    private void readFile(Path path, FilterPredicate predicate, long footerPos) throws IOException {
        Configuration configuration = new Configuration();
        ParquetReader < Group > reader = ParquetReader.builder(
                new TestGroupReadSupport(footerPos), path).
                withConf(configuration).withFilter(FilterCompat.get(predicate)).build();
        for (int i = 0; i < 10; i++) {
            reader.read();
        }
        reader.close();
    }

    private void batchReadFile(Path path, FilterPredicate predicate, long footerPos) throws IOException {
        Configuration configuration = new Configuration();
        ParquetReader < Group > reader = ParquetReader.builder(
                new BatchTestGroupReadSupport(footerPos), path).
                withConf(configuration).withFilter(FilterCompat.get(predicate)).build();
        for (int i = 0; i < 10; i++) {
            reader.read();
        }
        reader.close();
    }

    private long getFooterLocation(Path path) throws IOException {
        Configuration configuration = new Configuration();
        FileSystem fileSystem = FileSystem.get(configuration);
        FileStatus file = fileSystem.getFileStatus(path);
        FSDataInputStream f = fileSystem.open(file.getPath());
        try {
            long l = file.getLen();
            int FOOTER_LENGTH_SIZE = 4;
            long footerLengthIndex = l - FOOTER_LENGTH_SIZE - MAGIC.length;
            f.seek(footerLengthIndex);
            int footerLength = readIntLittleEndian(f);
            long footerIndex = footerLengthIndex - footerLength;
            return footerIndex;
        } finally {
            f.close();
        }

    }

    private static class TestGroupReadSupport extends GroupReadSupport {

        private long footerLocation;

        public TestGroupReadSupport(long footerLocation) {

            this.footerLocation = footerLocation;
        }
        @Override
        public ReadContext init(Configuration configuration, Map<String, String> keyValueMetaData, MessageType fileSchema) {
            return constructReadContext(super.init(configuration, keyValueMetaData, fileSchema));
        }

        @Override
        public ReadContext init(InitContext context) {
            return constructReadContext(super.init(context));
        }

        protected ReadContext constructReadContext(ReadContext rc) {
            EmbeddedTableSchema embeddedTableSchema = new EmbeddedTableSchema(footerLocation, rc.getRequestedSchema());
            List<EmbeddedTableSchema> embeddedTableSchemaList = new ArrayList<EmbeddedTableSchema>();
            embeddedTableSchemaList.add(embeddedTableSchema);
            return new ReadContext(rc.getRequestedSchema(), rc.getReadSupportMetadata(), embeddedTableSchemaList);
        }

    }

    private static class BatchTestGroupReadSupport extends TestGroupReadSupport {

        public BatchTestGroupReadSupport(long footerLocation) {
            super(footerLocation);
        }

        @Override
        protected ReadContext constructReadContext(ReadContext rc) {
            ReadContext parent = super.constructReadContext(rc);
            return new ReadContext(parent.getRequestedSchema(), parent.getReadSupportMetadata(),
                    parent.getEmbeddedTableSchemaList(), true);
        }
    }

    private static abstract class TestUDP extends UserDefinedPredicate<Integer> implements IndexLookupPredicate, Serializable {
    }


}
