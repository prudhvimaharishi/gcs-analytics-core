/*
 * Copyright 2026 Google LLC
 *
 * Licensed under the Apache License, Version 2.0 (the "License");
 * you may not use this file except in compliance with the License.
 * You may obtain a copy of the License at
 *
 *     http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */

package com.google.cloud.gcs.analyticscore.core.prefetch;

import static com.google.common.truth.Truth.assertThat;

import com.google.common.collect.ImmutableList;
import com.google.common.collect.Range;
import java.io.ByteArrayOutputStream;
import java.io.File;
import java.io.IOException;
import java.nio.ByteBuffer;
import java.nio.ByteOrder;
import java.nio.charset.StandardCharsets;
import java.util.ArrayList;
import java.util.List;
import java.util.Optional;
import org.apache.hadoop.conf.Configuration;
import org.apache.parquet.format.ColumnChunk;
import org.apache.parquet.format.ColumnMetaData;
import org.apache.parquet.format.CompressionCodec;
import org.apache.parquet.format.Encoding;
import org.apache.parquet.format.FileMetaData;
import org.apache.parquet.format.PageEncodingStats;
import org.apache.parquet.format.PageType;
import org.apache.parquet.format.RowGroup;
import org.apache.parquet.format.SchemaElement;
import org.apache.parquet.format.Type;
import org.apache.parquet.format.Util;
import org.apache.parquet.hadoop.ParquetFileReader;
import org.apache.parquet.hadoop.metadata.BlockMetaData;
import org.apache.parquet.hadoop.metadata.ColumnChunkMetaData;
import org.apache.parquet.hadoop.util.HadoopInputFile;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.io.TempDir;

class ParquetFooterParserTest {

  private static final int RECORD_COUNT = 500;
  private static final int MANY_RECORDS_COUNT = 20_000;
  private static final String CRAFTED_COLUMN = "crafted";
  private static final long CRAFTED_DICTIONARY_PAGE_OFFSET = 4;
  private static final long CRAFTED_DATA_PAGE_OFFSET = 24;
  private static final long CRAFTED_CHUNK_SIZE = 100;
  private static final byte[] PARQUET_MAGIC = "PAR1".getBytes(StandardCharsets.US_ASCII);

  @TempDir File tempDir;

  @Test
  void parse_validParquetFile_returnsLayout() throws IOException {
    byte[] fileBytes = createSingleRowGroupFile();

    Optional<ParquetFileLayout> layout = parseWholeFile(fileBytes);

    assertThat(layout).isPresent();
  }

  @Test
  void parse_validParquetFile_extractsAllColumnPaths() throws IOException {
    byte[] fileBytes = createSingleRowGroupFile();

    ParquetFileLayout layout = parseWholeFile(fileBytes).get();

    assertThat(layout.getRowGroups().get(0).getColumnChunks().keySet())
        .containsExactly(
            ParquetTestFiles.ID_COLUMN,
            ParquetTestFiles.CATEGORY_COLUMN,
            ParquetTestFiles.VALUE_COLUMN);
  }

  @Test
  void parse_columnChunkOffsets_matchReferenceImplementation() throws IOException {
    File file =
        ParquetTestFiles.createFile(
            tempDir,
            "reference.parquet",
            RECORD_COUNT,
            ParquetTestFiles.SINGLE_ROW_GROUP_SIZE_BYTES);

    ParquetFileLayout layout = parseWholeFile(ParquetTestFiles.readAllBytes(file)).get();

    assertThat(startOffsetsOf(layout)).isEqualTo(referenceStartOffsets(file));
  }

  @Test
  void parse_columnChunkSizes_matchReferenceImplementation() throws IOException {
    File file =
        ParquetTestFiles.createFile(
            tempDir, "sizes.parquet", RECORD_COUNT, ParquetTestFiles.SINGLE_ROW_GROUP_SIZE_BYTES);

    ParquetFileLayout layout = parseWholeFile(ParquetTestFiles.readAllBytes(file)).get();

    assertThat(compressedSizesOf(layout)).isEqualTo(referenceCompressedSizes(file));
  }

  @Test
  void parse_multipleRowGroups_returnsEveryRowGroup() throws IOException {
    File file =
        ParquetTestFiles.createFile(
            tempDir,
            "multi.parquet",
            MANY_RECORDS_COUNT,
            ParquetTestFiles.TINY_ROW_GROUP_SIZE_BYTES);

    ParquetFileLayout layout = parseWholeFile(ParquetTestFiles.readAllBytes(file)).get();

    assertThat(layout.getRowGroups()).hasSize(referenceRowGroupCount(file));
  }

  @Test
  void parse_rowGroupIndices_areSequential() throws IOException {
    File file =
        ParquetTestFiles.createFile(
            tempDir,
            "indices.parquet",
            MANY_RECORDS_COUNT,
            ParquetTestFiles.TINY_ROW_GROUP_SIZE_BYTES);

    ParquetFileLayout layout = parseWholeFile(ParquetTestFiles.readAllBytes(file)).get();

    assertThat(indicesOf(layout)).isInOrder();
  }

  @Test
  void parse_dictionaryEncodedColumn_exposesDictionaryPageRange() throws IOException {
    byte[] fileBytes = createSingleRowGroupFile();

    ParquetFileLayout layout = parseWholeFile(fileBytes).get();

    assertThat(categoryChunkOf(layout).getDictionaryPageRange()).isPresent();
  }

  @Test
  void parse_dictionaryPageRange_endsWhereDataPagesBegin() throws IOException {
    byte[] fileBytes = createSingleRowGroupFile();

    ParquetColumnChunk chunk = categoryChunkOf(parseWholeFile(fileBytes).get());

    assertThat(chunk.getDictionaryPageRange().get().upperEndpoint())
        .isEqualTo(chunk.getDataPageOffset());
  }

  @Test
  void parse_columnChunkWithoutDictionaryPage_omitsDictionaryPageRange() throws IOException {
    // The id dictionary overflows before the first page is written, so the writer drops it.
    File file = ParquetTestFiles.createFileWithDictionaryFallback(tempDir, "fallback.parquet", 200);
    ParquetFileLayout layout = parseWholeFile(ParquetTestFiles.readAllBytes(file)).get();

    ParquetColumnChunk idChunk =
        layout.getRowGroups().get(0).getColumnChunk(ParquetTestFiles.ID_COLUMN).get();

    assertThat(idChunk.getDictionaryPageRange()).isEmpty();
  }

  @Test
  void parse_identicalSchemas_produceEqualFingerprints() throws IOException {
    File first =
        ParquetTestFiles.createFile(
            tempDir, "first.parquet", RECORD_COUNT, ParquetTestFiles.SINGLE_ROW_GROUP_SIZE_BYTES);
    File second =
        ParquetTestFiles.createFile(
            tempDir,
            "second.parquet",
            RECORD_COUNT * 2,
            ParquetTestFiles.SINGLE_ROW_GROUP_SIZE_BYTES);

    int firstFingerprint = fingerprintOf(first);

    assertThat(firstFingerprint).isEqualTo(fingerprintOf(second));
  }

  @Test
  void parse_differentSchemas_produceDifferentFingerprints() throws IOException {
    File first =
        ParquetTestFiles.createFile(
            tempDir, "records.parquet", RECORD_COUNT, ParquetTestFiles.SINGLE_ROW_GROUP_SIZE_BYTES);
    File second =
        ParquetTestFiles.createFileWithAlternateSchema(tempDir, "others.parquet", RECORD_COUNT);

    int firstFingerprint = fingerprintOf(first);

    assertThat(firstFingerprint).isNotEqualTo(fingerprintOf(second));
  }

  @Test
  void parse_tailContainingOnlyFooter_returnsLayout() throws IOException {
    byte[] fileBytes = createSingleRowGroupFile();
    int tailLength = fileBytes.length - footerStartOffsetOf(fileBytes);

    Optional<ParquetFileLayout> layout = ParquetFooterParser.parse(tailOf(fileBytes, tailLength));

    assertThat(layout).isPresent();
  }

  @Test
  void parse_tailTruncatedBeforeMetadata_returnsEmpty() throws IOException {
    byte[] fileBytes = createSingleRowGroupFile();
    int tooShortLength = fileBytes.length - footerStartOffsetOf(fileBytes) - 1;

    Optional<ParquetFileLayout> layout =
        ParquetFooterParser.parse(tailOf(fileBytes, tooShortLength));

    assertThat(layout).isEmpty();
  }

  @Test
  void parse_bytesWithoutParquetMagic_returnsEmpty() {
    byte[] notParquet = "this is definitely not a parquet file".getBytes(StandardCharsets.UTF_8);

    Optional<ParquetFileLayout> layout = ParquetFooterParser.parse(ByteBuffer.wrap(notParquet));

    assertThat(layout).isEmpty();
  }

  @Test
  void parse_bufferShorterThanTrailer_returnsEmpty() {
    byte[] tooShort = {'P', 'A', 'R', '1'};

    Optional<ParquetFileLayout> layout = ParquetFooterParser.parse(ByteBuffer.wrap(tooShort));

    assertThat(layout).isEmpty();
  }

  @Test
  void parse_corruptedMetadataBytes_returnsEmpty() throws IOException {
    byte[] fileBytes = createSingleRowGroupFile();
    fileBytes[footerStartOffsetOf(fileBytes) + 1] ^= (byte) 0xFF;

    Optional<ParquetFileLayout> corruptedLayout = parseWholeFile(fileBytes);

    assertThat(corruptedLayout).isEmpty();
  }

  @Test
  void parse_doesNotConsumeCallerBuffer() throws IOException {
    ByteBuffer buffer = ByteBuffer.wrap(createSingleRowGroupFile());

    Optional<ParquetFileLayout> unused = ParquetFooterParser.parse(buffer);

    assertThat(buffer.position()).isEqualTo(0);
  }

  @Test
  void parse_negativeMetadataLength_returnsEmpty() {
    ByteBuffer tail = withTrailer(new byte[1], /* declaredMetadataLength= */ -1);

    Optional<ParquetFileLayout> layout = ParquetFooterParser.parse(tail);

    assertThat(layout).isEmpty();
  }

  @Test
  void parse_columnChunkWithoutMetaData_omitsThatColumn() throws IOException {
    ColumnChunk withoutMetaData = new ColumnChunk(/* file_offset= */ 0);

    ParquetRowGroup rowGroup =
        parseCraftedRowGroup(withoutMetaData, columnChunkOf(craftedMetaData()));

    assertThat(rowGroup.getColumnChunks().keySet()).containsExactly(CRAFTED_COLUMN);
  }

  @Test
  void parse_dictionaryPageOffsetAfterDataPages_startsChunkAtDataPageOffset() throws IOException {
    ColumnMetaData metaData =
        craftedMetaData().setDictionary_page_offset(CRAFTED_DATA_PAGE_OFFSET + 1);

    ParquetColumnChunk chunk = parseCraftedChunk(metaData);

    assertThat(chunk.getStartOffset()).isEqualTo(CRAFTED_DATA_PAGE_OFFSET);
  }

  @Test
  void parse_dictionaryEncodedColumnWithoutEncodingStats_exposesDictionaryPageRange()
      throws IOException {
    ParquetColumnChunk chunk = parseCraftedChunk(craftedDictionaryEncodedMetaData());

    assertThat(chunk.getDictionaryPageRange())
        .hasValue(Range.closedOpen(CRAFTED_DICTIONARY_PAGE_OFFSET, CRAFTED_DATA_PAGE_OFFSET));
  }

  @Test
  void parse_plainEncodedDataPage_omitsDictionaryPageRange() throws IOException {
    ColumnMetaData metaData =
        craftedDictionaryEncodedMetaData()
            .setEncoding_stats(
                ImmutableList.of(
                    dictionaryPageStats(),
                    new PageEncodingStats(PageType.DATA_PAGE, Encoding.PLAIN_DICTIONARY, 2),
                    new PageEncodingStats(PageType.DATA_PAGE, Encoding.PLAIN, 1)));

    ParquetColumnChunk chunk = parseCraftedChunk(metaData);

    assertThat(chunk.getDictionaryPageRange()).isEmpty();
  }

  @Test
  void parse_plainEncodedDataPageV2_omitsDictionaryPageRange() throws IOException {
    ColumnMetaData metaData =
        craftedDictionaryEncodedMetaData()
            .setEncoding_stats(
                ImmutableList.of(
                    dictionaryPageStats(),
                    new PageEncodingStats(PageType.DATA_PAGE_V2, Encoding.PLAIN, 1)));

    ParquetColumnChunk chunk = parseCraftedChunk(metaData);

    assertThat(chunk.getDictionaryPageRange()).isEmpty();
  }

  @Test
  void parse_rleDictionaryEncodedDataPages_exposesDictionaryPageRange() throws IOException {
    ColumnMetaData metaData =
        craftedDictionaryEncodedMetaData()
            .setEncoding_stats(
                ImmutableList.of(
                    dictionaryPageStats(),
                    new PageEncodingStats(PageType.DATA_PAGE, Encoding.RLE_DICTIONARY, 3)));

    ParquetColumnChunk chunk = parseCraftedChunk(metaData);

    assertThat(chunk.getDictionaryPageRange()).isPresent();
  }

  @Test
  void parse_plainEncodingStatsWithZeroPages_exposesDictionaryPageRange() throws IOException {
    ColumnMetaData metaData =
        craftedDictionaryEncodedMetaData()
            .setEncoding_stats(
                ImmutableList.of(
                    dictionaryPageStats(),
                    new PageEncodingStats(PageType.DATA_PAGE, Encoding.PLAIN_DICTIONARY, 3),
                    new PageEncodingStats(PageType.DATA_PAGE, Encoding.PLAIN, 0)));

    ParquetColumnChunk chunk = parseCraftedChunk(metaData);

    assertThat(chunk.getDictionaryPageRange()).isPresent();
  }

  private byte[] createSingleRowGroupFile() throws IOException {
    return ParquetTestFiles.readAllBytes(
        ParquetTestFiles.createFile(
            tempDir, "single.parquet", RECORD_COUNT, ParquetTestFiles.SINGLE_ROW_GROUP_SIZE_BYTES));
  }

  private static Optional<ParquetFileLayout> parseWholeFile(byte[] fileBytes) {
    return ParquetFooterParser.parse(ByteBuffer.wrap(fileBytes));
  }

  /** Returns the offset of the footer metadata, read from the 4-byte length before the magic. */
  private static int footerStartOffsetOf(byte[] fileBytes) {
    int trailerLength = 8;
    int metadataLength =
        ByteBuffer.wrap(fileBytes)
            .order(ByteOrder.LITTLE_ENDIAN)
            .getInt(fileBytes.length - trailerLength);
    return fileBytes.length - trailerLength - metadataLength;
  }

  private static ByteBuffer tailOf(byte[] fileBytes, int tailLength) {
    return ByteBuffer.wrap(fileBytes, fileBytes.length - tailLength, tailLength);
  }

  private static int fingerprintOf(File file) throws IOException {
    return parseWholeFile(ParquetTestFiles.readAllBytes(file)).get().getSchemaFingerprint();
  }

  private static ParquetColumnChunk categoryChunkOf(ParquetFileLayout layout) {
    return layout.getRowGroups().get(0).getColumnChunk(ParquetTestFiles.CATEGORY_COLUMN).get();
  }

  private static List<Long> startOffsetsOf(ParquetFileLayout layout) {
    List<Long> offsets = new ArrayList<>();
    for (ParquetRowGroup rowGroup : layout.getRowGroups()) {
      for (ParquetColumnChunk chunk : rowGroup.getColumnChunks().values()) {
        offsets.add(chunk.getStartOffset());
      }
    }
    return offsets;
  }

  private static List<Long> compressedSizesOf(ParquetFileLayout layout) {
    List<Long> sizes = new ArrayList<>();
    for (ParquetRowGroup rowGroup : layout.getRowGroups()) {
      for (ParquetColumnChunk chunk : rowGroup.getColumnChunks().values()) {
        sizes.add(chunk.getCompressedSize());
      }
    }
    return sizes;
  }

  private static List<Integer> indicesOf(ParquetFileLayout layout) {
    List<Integer> indices = new ArrayList<>();
    for (ParquetRowGroup rowGroup : layout.getRowGroups()) {
      indices.add(rowGroup.getIndex());
    }
    return indices;
  }

  private static List<Long> referenceStartOffsets(File file) throws IOException {
    List<Long> offsets = new ArrayList<>();
    for (BlockMetaData block : readReferenceBlocks(file)) {
      for (ColumnChunkMetaData column : block.getColumns()) {
        offsets.add(column.getStartingPos());
      }
    }
    return offsets;
  }

  private static List<Long> referenceCompressedSizes(File file) throws IOException {
    List<Long> sizes = new ArrayList<>();
    for (BlockMetaData block : readReferenceBlocks(file)) {
      for (ColumnChunkMetaData column : block.getColumns()) {
        sizes.add(column.getTotalSize());
      }
    }
    return sizes;
  }

  private static int referenceRowGroupCount(File file) throws IOException {
    return readReferenceBlocks(file).size();
  }

  private static List<BlockMetaData> readReferenceBlocks(File file) throws IOException {
    try (ParquetFileReader reader =
        ParquetFileReader.open(
            HadoopInputFile.fromPath(ParquetTestFiles.toHadoopPath(file), new Configuration()))) {
      return reader.getFooter().getBlocks();
    }
  }

  private static ColumnMetaData craftedMetaData() {
    return new ColumnMetaData(
        Type.INT64,
        ImmutableList.of(Encoding.PLAIN),
        ImmutableList.of(CRAFTED_COLUMN),
        CompressionCodec.UNCOMPRESSED,
        /* num_values= */ 10,
        /* total_uncompressed_size= */ CRAFTED_CHUNK_SIZE,
        /* total_compressed_size= */ CRAFTED_CHUNK_SIZE,
        /* data_page_offset= */ CRAFTED_DATA_PAGE_OFFSET);
  }

  private static ColumnMetaData craftedDictionaryEncodedMetaData() {
    return craftedMetaData().setDictionary_page_offset(CRAFTED_DICTIONARY_PAGE_OFFSET);
  }

  private static PageEncodingStats dictionaryPageStats() {
    return new PageEncodingStats(PageType.DICTIONARY_PAGE, Encoding.PLAIN, 1);
  }

  private static ColumnChunk columnChunkOf(ColumnMetaData metaData) {
    return new ColumnChunk(/* file_offset= */ 0).setMeta_data(metaData);
  }

  private static ParquetColumnChunk parseCraftedChunk(ColumnMetaData metaData) throws IOException {
    return parseCraftedRowGroup(columnChunkOf(metaData)).getColumnChunk(CRAFTED_COLUMN).get();
  }

  /**
   * Parses a footer built directly from Thrift structures, to reach layouts that real writers
   * rarely produce.
   */
  private static ParquetRowGroup parseCraftedRowGroup(ColumnChunk... columns) throws IOException {
    RowGroup rowGroup =
        new RowGroup(
            ImmutableList.copyOf(columns),
            /* total_byte_size= */ CRAFTED_CHUNK_SIZE,
            /* num_rows= */ 10);
    FileMetaData fileMetaData =
        new FileMetaData(
            /* version= */ 1,
            ImmutableList.of(new SchemaElement("schema")),
            /* num_rows= */ 10,
            ImmutableList.of(rowGroup));
    ByteArrayOutputStream metadata = new ByteArrayOutputStream();
    Util.writeFileMetaData(fileMetaData, metadata);

    ByteBuffer tail = withTrailer(metadata.toByteArray(), metadata.size());
    return ParquetFooterParser.parse(tail).get().getRowGroups().get(0);
  }

  /** Appends the little-endian metadata length and the Parquet magic to {@code metadata}. */
  private static ByteBuffer withTrailer(byte[] metadata, int declaredMetadataLength) {
    ByteBuffer tail =
        ByteBuffer.allocate(metadata.length + Integer.BYTES + PARQUET_MAGIC.length)
            .order(ByteOrder.LITTLE_ENDIAN);
    tail.put(metadata).putInt(declaredMetadataLength).put(PARQUET_MAGIC);
    tail.flip();
    return tail;
  }
}
