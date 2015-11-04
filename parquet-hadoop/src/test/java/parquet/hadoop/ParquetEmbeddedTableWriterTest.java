package parquet.hadoop;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.Path;
import org.junit.Test;
import parquet.example.data.Group;
import parquet.example.data.simple.SimpleGroupFactory;
import parquet.hadoop.api.InitContext;
import parquet.hadoop.example.GroupReadSupport;
import parquet.hadoop.example.GroupWriteSupport;
import parquet.hadoop.metadata.*;
import parquet.io.api.Binary;
import parquet.schema.MessageType;

import java.io.IOException;
import java.util.ArrayList;
import java.util.List;
import java.util.Map;

import static org.junit.Assert.*;
import static parquet.column.ParquetProperties.WriterVersion.PARQUET_2_0;
import static parquet.format.converter.ParquetMetadataConverter.NO_FILTER;
import static parquet.hadoop.TestUtils.enforceEmptyDir;
import static parquet.schema.MessageTypeParser.parseMessageType;

public class ParquetEmbeddedTableWriterTest {

  private static final String EMB_INDEX1 = "suffixArray";
  private static final String EMB_INDEX2 = "fuzzy";
  private static final String INDEXED_COLUMN = "doc_value";
  private static final String EMB_TABLE1_NAME = "embeddedTable1";
  private static final String EMB_TABLE2_NAME = "embeddedTable2";
  private static final int NUM_OF_MAIN_TABLE_ENTRIES = 1000;
  private static final int NUM_OF_EMB_TABLE1_ENTRIES = 500;
  private static final int NUM_OF_EMB_TABLE2_ENTRIES = 200;

  Configuration conf = new Configuration();
  Path root = new Path("target/tests/ParquetEmbeddedTableWriterTest/");
  MessageType schema = parseMessageType(
      "message mainTable { "
          + "required int32 doc_id; "
          + "required binary doc_value (UTF8);"
          + "} ");

  SimpleGroupFactory f = new SimpleGroupFactory(schema);

  private ParquetEmbeddedTableWriter<Group> createEmbeddedTableWriter(Path file) throws IOException {
    ParquetEmbeddedTableWriter<Group> writer = new ParquetEmbeddedTableWriter<Group>(
        file,
        ParquetFileWriter.Mode.CREATE,
        new GroupWriteSupport(),
        CompressionCodecName.UNCOMPRESSED,
        ParquetEmbeddedTableWriter.DEFAULT_BLOCK_SIZE,
        ParquetEmbeddedTableWriter.DEFAULT_PAGE_SIZE,
        ParquetEmbeddedTableWriter.DEFAULT_PAGE_SIZE,
        true,
        false,
        PARQUET_2_0,
        conf
    );

    return writer;
  }

  private void populateMainTableContent(ParquetEmbeddedTableWriter<Group> writer, int numOfValues) throws IOException {
    for(int i = 0; i < numOfValues; i++) {
      writer.write(
          f.newGroup()
              .append("doc_id", i)
              .append("doc_value", Binary.fromString("doc value " + i))
      );
    }

  }

  private void populateEmbeddedTableContent(ParquetEmbeddedTableRecordWriter<Group> embWriter, int numOfValues) throws IOException, InterruptedException {
    for(int i = 0; i < numOfValues; i++) {
      embWriter.write(
          f.newGroup()
              .append("doc_id", i)
              .append("doc_value", Binary.fromString("doc value " + i)));
    }

  }

  private void verifyTableContent(ParquetReader<Group> reader, int numOfValues) throws IOException {

    for(int i = 0; i < numOfValues; i++) {
      Group group = reader.read();

      assertEquals(i, group.getInteger("doc_id", 0));
      assertEquals("doc value " + i, group.getBinary("doc_value", 0).toStringUsingUTF8());
    }

  }

  @Test
  public void testWriteNoEmbeddedTable() throws IOException {
    enforceEmptyDir(conf, root);
    GroupWriteSupport.setSchema(schema, conf);

    Path file = new Path(root, "noEmbeddedTable.par");

    ParquetEmbeddedTableWriter<Group> writer = createEmbeddedTableWriter(file);
    populateMainTableContent(writer, NUM_OF_MAIN_TABLE_ENTRIES);
    writer.close();

    ParquetReader<Group> reader = ParquetReader.builder(new GroupReadSupport(), file).withConf(conf).build();
    verifyTableContent(reader, NUM_OF_MAIN_TABLE_ENTRIES);

  }

  @Test
  public void testWriteWithOneEmbeddedTable() throws IOException, InterruptedException {
    enforceEmptyDir(conf, root);
    GroupWriteSupport.setSchema(schema, conf);

    Path file = new Path(root, "oneEmbeddedTable.par");

    ParquetEmbeddedTableWriter<Group> writer = createEmbeddedTableWriter(file);

    populateMainTableContent(writer, NUM_OF_MAIN_TABLE_ENTRIES);

    EmbeddedTableMetadata embMet = new EmbeddedIndexMetadata(EMB_INDEX1, INDEXED_COLUMN, EMB_TABLE1_NAME);

    ParquetEmbeddedTableRecordWriter<Group> embWriter =  writer.addEmbeddedTableRecordWriter(
        new GroupWriteSupport(), false, false, embMet);

    populateEmbeddedTableContent(embWriter, NUM_OF_EMB_TABLE1_ENTRIES);

    writer.close();

    //verify footer metadata
    ParquetMetadata footer = ParquetFileReader.readFooter(conf, file, NO_FILTER);
    Map<String,String> footerKeyVal = footer.getFileMetaData().getKeyValueMetaData();
    EmbeddedIndexMetadata embIndexMeta = EmbeddedIndexMetadata.getEmbeddedIndexMetadata(
        footerKeyVal, EMB_INDEX1, INDEXED_COLUMN);

    assertNotNull(embIndexMeta);

    long embFooterPos = embIndexMeta.getEmbeddedIndexFooterPos();

    //verify main table content
    ParquetReader<Group> reader = ParquetReader.builder(new GroupReadSupport(), file).withConf(conf).build();
    verifyTableContent(reader, NUM_OF_MAIN_TABLE_ENTRIES);

    //verify embedded table content
    ParquetReader<Group> embReader = ParquetReader.builder(new TestGroupReadSupport(embFooterPos), file)
        .withConf(conf)
        .build();

    verifyTableContent(embReader, NUM_OF_EMB_TABLE1_ENTRIES);

  }

  @Test
  public void testWriteWithTwoEmbeddedTables() throws IOException, InterruptedException {
    enforceEmptyDir(conf, root);
    GroupWriteSupport.setSchema(schema, conf);

    Path file = new Path(root, "twoEmbeddedTables.par");

    ParquetEmbeddedTableWriter<Group> writer = createEmbeddedTableWriter(file);

    populateMainTableContent(writer, NUM_OF_MAIN_TABLE_ENTRIES);

    EmbeddedTableMetadata embMet = new EmbeddedIndexMetadata(EMB_INDEX1, INDEXED_COLUMN, EMB_TABLE1_NAME);

    ParquetEmbeddedTableRecordWriter<Group> embWriter =  writer.addEmbeddedTableRecordWriter(
        new GroupWriteSupport(), false, false, embMet);

    populateEmbeddedTableContent(embWriter, NUM_OF_EMB_TABLE1_ENTRIES);

    EmbeddedTableMetadata embMet2 = new EmbeddedIndexMetadata(EMB_INDEX2, INDEXED_COLUMN, EMB_TABLE2_NAME);
    ParquetEmbeddedTableRecordWriter<Group> embWriter2 =  writer.addEmbeddedTableRecordWriter(
        new GroupWriteSupport(), false, false, embMet2);

    populateEmbeddedTableContent(embWriter2, NUM_OF_EMB_TABLE2_ENTRIES);


    writer.close();

    //verify footer metadata
    ParquetMetadata footer = ParquetFileReader.readFooter(conf, file, NO_FILTER);
    Map<String,String> footerKeyVal = footer.getFileMetaData().getKeyValueMetaData();
    EmbeddedIndexMetadata embIndexMeta = EmbeddedIndexMetadata.getEmbeddedIndexMetadata(
        footerKeyVal, EMB_INDEX1, INDEXED_COLUMN);

    assertNotNull(embIndexMeta);

    long embFooter1Pos = embIndexMeta.getEmbeddedIndexFooterPos();

    EmbeddedIndexMetadata embIndex2Meta = EmbeddedIndexMetadata.getEmbeddedIndexMetadata(
        footerKeyVal, EMB_INDEX2, INDEXED_COLUMN);

    assertNotNull(embIndex2Meta);

    long embFooter2Pos = embIndex2Meta.getEmbeddedIndexFooterPos();


    //verify main table content
    ParquetReader<Group> reader = ParquetReader.builder(new GroupReadSupport(), file).withConf(conf).build();
    verifyTableContent(reader, NUM_OF_MAIN_TABLE_ENTRIES);

    //verify embedded table content
    ParquetReader<Group> embReader = ParquetReader.builder(new TestGroupReadSupport(embFooter1Pos), file)
        .withConf(conf)
        .build();

    verifyTableContent(embReader, NUM_OF_EMB_TABLE1_ENTRIES);

    //verify embedded table 2 content
    ParquetReader<Group> embReader2 = ParquetReader.builder(new TestGroupReadSupport(embFooter2Pos), file)
        .withConf(conf)
        .build();

    verifyTableContent(embReader2, NUM_OF_EMB_TABLE2_ENTRIES);


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


}