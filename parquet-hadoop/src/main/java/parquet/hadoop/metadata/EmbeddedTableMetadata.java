package parquet.hadoop.metadata;

import java.util.Map;

/**
 * Created by Jaspreet Singh
 */
public interface EmbeddedTableMetadata {

  /**
   * add the Embedded table metadata to the input parquet file metadata map.
   *
   * @param fileKeyValMetadata
   */

  public void addEmbeddedTableMetadataToFileMetadata(Map<String, String> fileKeyValMetadata);

  /**
   * set the footer position
   *
   * @param footerPosition
   */
  public void setEmbeddedTableFooterPosition(long footerPosition);

}
