/*
 * Copyright 2013 Cloudera Inc.
 *
 * Licensed under the Apache License, Version 2.0 (the "License");
 * you may not use this file except in compliance with the License.
 * You may obtain a copy of the License at
 *
 *   http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */

package org.kitesdk.cli.commands;

import com.beust.jcommander.Parameter;
import com.beust.jcommander.Parameters;
import com.beust.jcommander.internal.Lists;
import com.fasterxml.jackson.databind.JsonNode;
import com.fasterxml.jackson.databind.ObjectMapper;
import com.google.common.annotations.VisibleForTesting;
import com.google.common.base.Preconditions;
import com.google.common.collect.ImmutableSet;

import java.io.File;
import java.io.IOException;
import java.nio.charset.Charset;
import java.util.Iterator;
import java.util.List;
import java.util.Set;

import org.apache.avro.Schema;
import org.apache.avro.generic.GenericData;
import org.apache.avro.generic.GenericRecord;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.Path;
import org.kitesdk.data.spi.JsonUtil;
import org.kitesdk.data.spi.filesystem.CSVProperties;
import org.kitesdk.data.spi.filesystem.CSVUtil;
import org.slf4j.Logger;
import org.apache.parquet.avro.AvroParquetWriter;
import org.apache.parquet.hadoop.ParquetWriter;

@Parameters(commandDescription="Convert a JSON file to Parquet or Avro")
public class JSONConvertCommand extends BaseCommand {

  private final Logger console;

  public JSONConvertCommand(Logger console) {
    this.console = console;
  }

  @Parameter(description="<schema path> <json path> <output path>")
  List<String> targets;

  @Override
  public int run() throws IOException {
    Preconditions.checkArgument(targets != null && targets.size() == 3,
        "Usage: <schema path> <json path> <output path>");

    File schemaPath = new File(targets.get(0));
    File jsonPath = new File(targets.get(1));
    Path outputPath = qualifiedPath(targets.get(2));

    Schema schema = Schema.parse(schemaPath);
    ObjectMapper mapper = new ObjectMapper();
    JsonNode root = mapper.readValue(jsonPath, JsonNode.class);
    Preconditions.checkArgument(root.isArray(),
        "Input JSON should be an array of records");
    
    Configuration conf = new Configuration();
    conf.set("parquet.avro.write-old-list-structure", "false");

    AvroParquetWriter<GenericRecord> writer = new AvroParquetWriter<GenericRecord>(outputPath, schema, 
        AvroParquetWriter.DEFAULT_COMPRESSION_CODEC_NAME, AvroParquetWriter.DEFAULT_BLOCK_SIZE,
        AvroParquetWriter.DEFAULT_PAGE_SIZE, true, conf);

    for (JsonNode jsonRecord : root) {
      System.out.println("record: " + jsonRecord);
      GenericRecord record =
          (GenericRecord) JsonUtil.convertToAvro(GenericData.get(), jsonRecord, schema);
      writer.write(record);
    }

    writer.close();
    return 0;
  }

  @Override
  public List<String> getExamples() {
    return Lists.newArrayList(
        "# Print the schema for samples.json to standard out:",
        "samples.json --record-name Sample",
        "# Write schema to sample.avsc:",
        "samples.json -o sample.avsc --record-name Sample"
    );
  }
}
