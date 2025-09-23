package com.google.cloud.gcs.analyticscore.core;

import org.apache.parquet.filter2.predicate.FilterPredicate;
import org.apache.parquet.io.api.Binary;
import org.openjdk.jmh.annotations.*;

import java.io.IOException;
import java.net.URI;
import java.util.concurrent.TimeUnit;

import static org.apache.parquet.filter2.predicate.FilterApi.binaryColumn;
import static org.apache.parquet.filter2.predicate.FilterApi.eq;

@State(Scope.Benchmark)
public class VectoredReadBenchmark {
    @Setup(Level.Invocation)
    public void uploadSampleFiles() throws IOException {
        uploadBundledResourceToGcs("tpcds_customer_sf1.parquet");
        uploadBundledResourceToGcs("tpcds_customer_sf10.parquet");
        uploadBundledResourceToGcs("tpcds_customer_sf100.parquet");
    }

    @TearDown(Level.Invocation)
    public void deleteSampleFiles() throws IOException {
        IntegrationTestHelper.deleteUploadedFilesFromGcs();
    }

    @Benchmark
    @BenchmarkMode(Mode.AverageTime)
    @OutputTimeUnit(TimeUnit.MILLISECONDS)
    @Warmup(iterations = 1, time = 1)
    @Measurement(iterations = 2, time = 1)
    @Fork(value = 2, warmups = 1)
    public void read_3mbFile_withVectoredReadEnabled() throws IOException {
        FilterPredicate byCountryFilter = eq(binaryColumn("c_birth_country"), Binary.fromString("EGYPT"));
        URI uri = IntegrationTestHelper.getGcsObjectUriForFile("tpcds_customer_sf1.parquet");
        ParquetHelper.readParquetObjectRecords(uri, byCountryFilter,false, true);
    }

    @Benchmark
    @BenchmarkMode(Mode.AverageTime)
    @OutputTimeUnit(TimeUnit.MILLISECONDS)
    @Warmup(iterations = 1, time = 1)
    @Measurement(iterations = 2, time = 1)
    @Fork(value = 2, warmups = 1)
    public void read_3mbFile_withVectoredReadDisabled() throws IOException {
        FilterPredicate byCountryFilter = eq(binaryColumn("c_birth_country"), Binary.fromString("EGYPT"));
        URI uri = IntegrationTestHelper.getGcsObjectUriForFile("tpcds_customer_sf1.parquet");
        ParquetHelper.readParquetObjectRecords(uri, byCountryFilter,false, false);
    }

    @Benchmark
    @BenchmarkMode(Mode.AverageTime)
    @OutputTimeUnit(TimeUnit.MILLISECONDS)
    @Warmup(iterations = 1, time = 1)
    @Measurement(iterations = 2, time = 1)
    @Fork(value = 2, warmups = 1)
    public void read_18mbFile_withVectoredReadEnabled() throws IOException {
        FilterPredicate byCountryFilter = eq(binaryColumn("c_birth_country"), Binary.fromString("EGYPT"));
        URI uri = IntegrationTestHelper.getGcsObjectUriForFile("tpcds_customer_sf10.parquet");
        ParquetHelper.readParquetObjectRecords(uri, byCountryFilter,false, true);
    }

    @Benchmark
    @BenchmarkMode(Mode.AverageTime)
    @OutputTimeUnit(TimeUnit.MILLISECONDS)
    @Warmup(iterations = 1, time = 1)
    @Measurement(iterations = 2, time = 1)
    @Fork(value = 2, warmups = 1)
    public void read_18mbFile_withVectoredReadDisabled() throws IOException {
        FilterPredicate byCountryFilter = eq(binaryColumn("c_birth_country"), Binary.fromString("EGYPT"));
        URI uri = IntegrationTestHelper.getGcsObjectUriForFile("tpcds_customer_sf10.parquet");
        ParquetHelper.readParquetObjectRecords(uri, byCountryFilter,false, false);
    }

    @Benchmark
    @BenchmarkMode(Mode.AverageTime)
    @OutputTimeUnit(TimeUnit.MILLISECONDS)
    @Warmup(iterations = 1, time = 1)
    @Measurement(iterations = 5, time = 1)
    @Fork(value = 2, warmups = 1)
    public void read_50mbFile_withVectoredReadEnabled() throws IOException {
        FilterPredicate byCountryFilter = eq(binaryColumn("c_birth_country"), Binary.fromString("EGYPT"));
        URI uri = IntegrationTestHelper.getGcsObjectUriForFile("tpcds_customer_sf100.parquet");
        ParquetHelper.readParquetObjectRecords(uri, byCountryFilter, false, true);
    }

    @Benchmark
    @BenchmarkMode(Mode.AverageTime)
    @OutputTimeUnit(TimeUnit.MILLISECONDS)
    @Warmup(iterations = 1, time = 1)
    @Measurement(iterations = 5, time = 1)
    @Fork(value = 2, warmups = 1)
    public void read_50mbFile_withVectoredReadDisabled() throws IOException {
        FilterPredicate byCountryFilter = eq(binaryColumn("c_birth_country"), Binary.fromString("EGYPT"));
        URI uri = IntegrationTestHelper.getGcsObjectUriForFile("tpcds_customer_sf100.parquet");
        ParquetHelper.readParquetObjectRecords(uri, byCountryFilter, false, false);
    }

    private void uploadBundledResourceToGcs(String fileName) {
        IntegrationTestHelper.uploadFileToGcs(
                getClass().getResourceAsStream("/sampleParquetFiles/" + fileName),
                fileName);
    }
}
