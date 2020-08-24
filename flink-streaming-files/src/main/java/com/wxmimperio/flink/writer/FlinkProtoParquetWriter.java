package com.wxmimperio.flink.writer;

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
import com.google.protobuf.Message;
import com.google.protobuf.MessageOrBuilder;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.Path;
import org.apache.parquet.column.ParquetProperties;
import org.apache.parquet.hadoop.ParquetFileWriter;
import org.apache.parquet.hadoop.ParquetWriter;
import org.apache.parquet.hadoop.api.WriteSupport;
import org.apache.parquet.hadoop.metadata.CompressionCodecName;
import org.apache.parquet.io.OutputFile;

import java.io.IOException;

/**
 * Write Protobuf records to a Parquet file.
 */
public class FlinkProtoParquetWriter<T extends MessageOrBuilder> extends ParquetWriter<T> {

    /**
     * Create a new {@link org.apache.parquet.proto.ProtoParquetWriter}.
     *
     * @param file                 The file name to write to.
     * @param protoMessage         Protobuf message class
     * @param compressionCodecName Compression code to use, or CompressionCodecName.UNCOMPRESSED
     * @param blockSize            HDFS block size
     * @param pageSize             See parquet write up. Blocks are subdivided into pages for alignment and other purposes.
     * @throws IOException if there is an error while writing
     */
    public FlinkProtoParquetWriter(Path file, Class<? extends Message> protoMessage,
                                   CompressionCodecName compressionCodecName, int blockSize,
                                   int pageSize) throws IOException {
        super(file, new BiliProtoWriteSupport(protoMessage),
                compressionCodecName, blockSize, pageSize);
    }

    /**
     * Create a new {@link org.apache.parquet.proto.ProtoParquetWriter}.
     *
     * @param file                 The file name to write to.
     * @param protoMessage         Protobuf message class
     * @param compressionCodecName Compression code to use, or CompressionCodecName.UNCOMPRESSED
     * @param blockSize            HDFS block size
     * @param pageSize             See parquet write up. Blocks are subdivided into pages for alignment and other purposes.
     * @param enableDictionary     Whether to use a dictionary to compress columns.
     * @param validating           to turn on validation using the schema
     * @throws IOException if there is an error while writing
     */
    public FlinkProtoParquetWriter(Path file, Class<? extends Message> protoMessage,
                                   CompressionCodecName compressionCodecName, int blockSize,
                                   int pageSize, boolean enableDictionary, boolean validating) throws IOException {
        super(file, new BiliProtoWriteSupport(protoMessage),
                compressionCodecName, blockSize, pageSize, enableDictionary, validating);
    }

    /**
     * Create a new {@link org.apache.parquet.proto.ProtoParquetWriter}. The default block size is 50 MB.The default
     * page size is 1 MB.  Default compression is no compression. (Inherited from {@link ParquetWriter})
     *
     * @param file         The file name to write to.
     * @param protoMessage Protobuf message class
     * @throws IOException if there is an error while writing
     */
    public FlinkProtoParquetWriter(Path file, Class<? extends Message> protoMessage) throws IOException {
        this(file, protoMessage, CompressionCodecName.UNCOMPRESSED,
                DEFAULT_BLOCK_SIZE, DEFAULT_PAGE_SIZE);
    }

    private static <T> WriteSupport<T> writeSupport(Configuration conf, Class<T> protoMessage) {
        return new BiliProtoWriteSupport(protoMessage);
    }

    public static <T> Builder<T> builder(OutputFile file) {
        return new Builder<T>(file);
    }


    public static class Builder<T> extends ParquetWriter.Builder<T, Builder<T>> {
        Class<T> protoMessage = null;


        private Builder(Path file) {
            super(file);
        }

        private Builder(OutputFile file) {
            super(file);
        }

        public Builder<T> withProtoMessage(Class<T> protoMessage) {
            this.protoMessage = protoMessage;
            return this;
        }

        @Override
        public Builder<T> withCompressionCodec(CompressionCodecName codecName) {
            return super.withCompressionCodec(codecName);
        }

        @Override
        public Builder<T> withConf(Configuration conf) {
            return super.withConf(conf);
        }

        @Override
        public Builder<T> withWriteMode(ParquetFileWriter.Mode mode) {
            return super.withWriteMode(mode);
        }

        @Override
        public Builder<T> withRowGroupSize(int rowGroupSize) {
            return super.withRowGroupSize(rowGroupSize);
        }

        @Override
        public Builder<T> withPageSize(int pageSize) {
            return super.withPageSize(pageSize);
        }

        @Override
        public Builder<T> withPageRowCountLimit(int rowCount) {
            return super.withPageRowCountLimit(rowCount);
        }

        @Override
        public Builder<T> withDictionaryPageSize(int dictionaryPageSize) {
            return super.withDictionaryPageSize(dictionaryPageSize);
        }

        @Override
        public Builder<T> withMaxPaddingSize(int maxPaddingSize) {
            return super.withMaxPaddingSize(maxPaddingSize);
        }

        @Override
        public Builder<T> withDictionaryEncoding(boolean enableDictionary) {
            return super.withDictionaryEncoding(enableDictionary);
        }

        @Override
        public Builder<T> withValidation(boolean enableValidation) {
            return super.withValidation(enableValidation);
        }

        @Override
        public Builder<T> withWriterVersion(ParquetProperties.WriterVersion version) {
            return super.withWriterVersion(version);
        }

        @Override
        public Builder<T> withPageWriteChecksumEnabled(boolean enablePageWriteChecksum) {
            return super.withPageWriteChecksumEnabled(enablePageWriteChecksum);
        }

        @Override
        protected Builder<T> self() {
            return this;
        }

        @Override
        protected WriteSupport<T> getWriteSupport(Configuration conf) {
            return FlinkProtoParquetWriter.writeSupport(conf, protoMessage);
        }
    }
}