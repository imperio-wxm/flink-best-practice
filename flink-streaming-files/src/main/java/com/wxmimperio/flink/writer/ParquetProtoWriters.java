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
 *     http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */

import org.apache.flink.formats.parquet.ParquetBuilder;
import org.apache.parquet.hadoop.ParquetWriter;
import org.apache.parquet.hadoop.metadata.CompressionCodecName;
import org.apache.parquet.io.OutputFile;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.io.IOException;

/**
 * Convenience builder to create {@link ParquetWriterFactory} instances for the different Avro types.
 */
public class ParquetProtoWriters {
    private static final Logger LOG = LoggerFactory.getLogger(ParquetProtoWriters.class);

    private static final int pageSize = 64 * 1024;

    public static <T> ParquetWriterFactory<T> forType(final Class<T> protoClass) {
        ParquetBuilder<T> builder = null;
        try {
            builder = (out) -> createProtoParquetWriter(protoClass, out);
        } catch (Exception e) {
            LOG.error("Can not get proto parquet writer!", e);
        }
        return new ParquetWriterFactory<T>(builder);
    }

    private static <T> ParquetWriter<T> createProtoParquetWriter(Class<T> protoClass, OutputFile out) throws IOException {
        return FlinkProtoParquetWriter.<T>builder(out)
                .withProtoMessage(protoClass)
                .withCompressionCodec(CompressionCodecName.GZIP)
                .build();
    }
}
