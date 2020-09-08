/*
 * Copyright 2008-present MongoDB, Inc.
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
package com.mongodb.kafka.connect;

import static com.mongodb.kafka.connect.mongodb.ChangeStreamOperations.concat;
import static com.mongodb.kafka.connect.mongodb.ChangeStreamOperations.createDropCollection;
import static com.mongodb.kafka.connect.mongodb.ChangeStreamOperations.createInserts;
import static com.mongodb.kafka.connect.source.MongoSourceConfig.OUTPUT_FORMAT_KEY_CONFIG;
import static com.mongodb.kafka.connect.source.MongoSourceConfig.OUTPUT_FORMAT_VALUE_CONFIG;
import static com.mongodb.kafka.connect.source.MongoSourceConfig.OUTPUT_SCHEMA_KEY_CONFIG;
import static com.mongodb.kafka.connect.source.MongoSourceConfig.OUTPUT_SCHEMA_VALUE_CONFIG;
import static com.mongodb.kafka.connect.source.MongoSourceConfig.PIPELINE_CONFIG;
import static java.util.Collections.singletonList;
import static java.util.stream.Collectors.toList;
import static java.util.stream.IntStream.rangeClosed;
import static org.junit.jupiter.api.Assertions.assertAll;
import static org.junit.jupiter.api.Assertions.assertIterableEquals;
import static org.junit.jupiter.api.Assumptions.assumeTrue;

import java.util.List;
import java.util.Properties;
import java.util.stream.StreamSupport;

import org.apache.kafka.clients.consumer.ConsumerRecord;
import org.apache.kafka.common.serialization.Deserializer;
import org.apache.kafka.connect.json.JsonDeserializer;
import org.junit.jupiter.api.AfterEach;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.DisplayName;
import org.junit.jupiter.api.Test;

import org.bson.Document;

import com.mongodb.client.MongoCollection;
import com.mongodb.client.MongoDatabase;

import com.mongodb.kafka.connect.mongodb.MongoKafkaTestCase;
import com.mongodb.kafka.connect.source.MongoSourceConfig;
import com.mongodb.kafka.connect.source.MongoSourceConfig.OutputFormat;

public class WBAMongoSourceConnectorTest extends MongoKafkaTestCase {

    @BeforeEach
    void setUp() {
        assumeTrue(isReplicaSetOrSharded());
    }

    @AfterEach
    void tearDown() {
        getMongoClient().listDatabaseNames().into(new ArrayList<>()).forEach(i -> {
            if (i.startsWith(getDatabaseName())) {
                getMongoClient().getDatabase(i).drop();
            }
        });
    }

    @Test
    @DisplayName("Ensure source loads data from MongoClient")
    void testSourceLoadsDataFromMongoClient() {
        assumeTrue(isGreaterThanThreeDotSix());
        addSourceConnector();

        MongoDatabase db1 = getDatabaseWithPostfix();
        MongoDatabase db2 = getDatabaseWithPostfix();
        MongoDatabase db3 = getDatabaseWithPostfix();
        MongoCollection<Document> coll1 = db1.getCollection("coll");
        MongoCollection<Document> coll2 = db2.getCollection("coll");
        MongoCollection<Document> coll3 = db3.getCollection("coll");
        MongoCollection<Document> coll4 = db1.getCollection("db1Coll2");

        insertMany(rangeClosed(1, 50), coll1, coll2);

        assertAll(
                () -> assertProduced(createInserts(1, 50), coll1),
                () -> assertProduced(createInserts(1, 50), coll2),
                () -> assertProduced(emptyList(), coll3)
        );


        db1.drop();
        insertMany(rangeClosed(51, 60), coll2, coll4);
        insertMany(rangeClosed(1, 70), coll3);

        assertAll(
                () -> assertProduced(concat(createInserts(1, 50), singletonList(createDropCollection())), coll1),
                () -> assertProduced(createInserts(1, 60), coll2),
                () -> assertProduced(createInserts(1, 70), coll3),
                () -> assertProduced(createInserts(51, 60), coll4)
        );
    }

    @Test
    @DisplayName("Ensure source loads data from MongoClient with copy existing data")
    void testSourceLoadsDataFromMongoClientWithCopyExisting() {
        assumeTrue(isGreaterThanThreeDotSix());
        Properties sourceProperties = new Properties();
        sourceProperties.put(MongoSourceConfig.COPY_EXISTING_CONFIG, "true");
        addSourceConnector(sourceProperties);

        MongoDatabase db1 = getDatabaseWithPostfix();
        MongoDatabase db2 = getDatabaseWithPostfix();
        MongoDatabase db3 = getDatabaseWithPostfix();
        MongoCollection<Document> coll1 = db1.getCollection("coll");
        MongoCollection<Document> coll2 = db2.getCollection("coll");
        MongoCollection<Document> coll3 = db3.getCollection("coll");
        MongoCollection<Document> coll4 = db1.getCollection("db1Coll2");

        insertMany(rangeClosed(1, 50), coll1, coll2);

        assertAll(
                () -> assertProduced(createInserts(1, 50), coll1),
                () -> assertProduced(createInserts(1, 50), coll2),
                () -> assertProduced(emptyList(), coll3)
        );

        db1.drop();
        insertMany(rangeClosed(51, 60), coll2, coll4);
        insertMany(rangeClosed(1, 70), coll3);

        assertAll(
                () -> assertProduced(concat(createInserts(1, 50), singletonList(createDropCollection())), coll1),
                () -> assertProduced(createInserts(1, 60), coll2),
                () -> assertProduced(createInserts(1, 70), coll3),
                () -> assertProduced(createInserts(51, 60), coll4)
        );
    }

    @Test
    @DisplayName("Ensure source loads data from database")
    void testSourceLoadsDataFromDatabase() {
        assumeTrue(isGreaterThanThreeDotSix());
        try (KafkaConsumer<?, ?> consumer = createConsumer()) {
            Pattern pattern = Pattern.compile(format("^%s.*", getDatabaseName()));
            consumer.subscribe(pattern);

            MongoDatabase db = getDatabaseWithPostfix();

            Properties sourceProperties = new Properties();
            sourceProperties.put(MongoSourceConfig.DATABASE_CONFIG, db.getName());
            addSourceConnector(sourceProperties);

            MongoCollection<Document> coll1 = db.getCollection("coll1");
            MongoCollection<Document> coll2 = db.getCollection("coll2");
            MongoCollection<Document> coll3 = db.getCollection("coll3");

            insertMany(rangeClosed(1, 50), coll1, coll2);

            assertAll(
                    () -> assertProduced(createInserts(1, 50), coll1),
                    () -> assertProduced(createInserts(1, 50), coll2),
                    () -> assertProduced(emptyList(), coll3)
            );

            // Update some of the collections
            coll1.drop();
            coll2.drop();

            insertMany(rangeClosed(1, 20), coll3);

            String collName4 = "coll4";
            coll3.renameCollection(new MongoNamespace(getDatabaseName(), collName4));
            MongoCollection<Document> coll4 = db.getCollection(collName4);

            insertMany(rangeClosed(21, 30), coll4);

            assertAll(
                    () -> assertProduced(concat(createInserts(1, 50), singletonList(createDropCollection())), coll1),
                    () -> assertProduced(concat(createInserts(1, 50), singletonList(createDropCollection())), coll2),
                    () -> assertProduced(concat(createInserts(1, 20), singletonList(createDropCollection())), coll3),
                    () -> assertProduced(createInserts(21, 30), coll4)
            );
        }
    }

    @Test
    @DisplayName("Ensure source loads data from database with copy existing data")
    void testSourceLoadsDataFromDatabaseCopyExisting() {
        assumeTrue(isGreaterThanThreeDotSix());
        try (KafkaConsumer<?, ?> consumer = createConsumer()) {
            Pattern pattern = Pattern.compile(format("^%s.*", getDatabaseName()));
            consumer.subscribe(pattern);

            MongoDatabase db = getDatabaseWithPostfix();

            MongoCollection<Document> coll1 = db.getCollection("coll1");
            MongoCollection<Document> coll2 = db.getCollection("coll2");
            MongoCollection<Document> coll3 = db.getCollection("coll3");

            insertMany(rangeClosed(1, 50), coll1, coll2);

            Properties sourceProperties = new Properties();
            sourceProperties.put(MongoSourceConfig.DATABASE_CONFIG, db.getName());
            sourceProperties.put(MongoSourceConfig.COPY_EXISTING_CONFIG, "true");
            addSourceConnector(sourceProperties);

            assertAll(
                    () -> assertProduced(createInserts(1, 50), coll1),
                    () -> assertProduced(createInserts(1, 50), coll2),
                    () -> assertProduced(emptyList(), coll3)
            );

            // Update some of the collections
            coll1.drop();
            coll2.drop();

            insertMany(rangeClosed(1, 20), coll3);

            String collName4 = "coll4";
            coll3.renameCollection(new MongoNamespace(getDatabaseName(), collName4));
            MongoCollection<Document> coll4 = db.getCollection(collName4);

            insertMany(rangeClosed(21, 30), coll4);

            assertAll(
                    () -> assertProduced(concat(createInserts(1, 50), singletonList(createDropCollection())), coll1),
                    () -> assertProduced(concat(createInserts(1, 50), singletonList(createDropCollection())), coll2),
                    () -> assertProduced(concat(createInserts(1, 20), singletonList(createDropCollection())), coll3),
                    () -> assertProduced(createInserts(21, 30), coll4)
            );
        }
    }

    @Test
    @DisplayName("Ensure source can handle non existent database and survive dropping")
    void testSourceCanHandleNonExistentDatabaseAndSurviveDropping() throws InterruptedException {
        assumeTrue(isGreaterThanThreeDotSix());
        try (KafkaConsumer<?, ?> consumer = createConsumer()) {
            Pattern pattern = Pattern.compile(format("^%s.*", getDatabaseName()));
            consumer.subscribe(pattern);

            MongoDatabase db = getDatabaseWithPostfix();
            MongoCollection<Document> coll1 = db.getCollection("coll1");
            MongoCollection<Document> coll2 = db.getCollection("coll2");
            MongoCollection<Document> coll3 = db.getCollection("coll3");
            db.drop();

            Properties sourceProperties = new Properties();
            sourceProperties.put(MongoSourceConfig.DATABASE_CONFIG, db.getName());
            addSourceConnector(sourceProperties);

            Thread.sleep(5000);
            assertAll(
                    () -> assertProduced(emptyList(), coll1),
                    () -> assertProduced(emptyList(), coll2),
                    () -> assertProduced(emptyList(), coll3),
                    () -> assertProduced(emptyList(), db.getName())
            );

            insertMany(rangeClosed(1, 50), coll1, coll2);
            insertMany(rangeClosed(1, 1), coll3);

            assertAll(
                    () -> assertProduced(createInserts(1, 50), coll1),
                    () -> assertProduced(createInserts(1, 50), coll2),
                    () -> assertProduced(singletonList(createInsert(1)), coll3),
                    () -> assertProduced(emptyList(), db.getName())
            );

            db.drop();
            assertAll(
                    () -> assertProduced(concat(createInserts(1, 50), singletonList(createDropCollection())), coll1),
                    () -> assertProduced(concat(createInserts(1, 50), singletonList(createDropCollection())), coll2),
                    () -> assertProduced(asList(createInsert(1), createDropCollection()), coll3),
                    () -> assertProduced(singletonList(createDropDatabase()), db.getName())
            );

            insertMany(rangeClosed(51, 100), coll1, coll2, coll3);

            assertAll(
                () -> assertProduced(concat(createInserts(1, 50), singletonList(createDropCollection()), createInserts(51, 100)), coll1),
                () -> assertProduced(concat(createInserts(1, 50), singletonList(createDropCollection()), createInserts(51, 100)), coll2),
                () -> assertProduced(concat(asList(createInsert(1), createDropCollection()), createInserts(51, 100)), coll3),
                () -> assertProduced(singletonList(createDropDatabase()), db.getName())
            );
        }
    }

    @Test
    @DisplayName("Ensure source loads data from collection")
    void testSourceLoadsDataFromCollection() {
        MongoCollection<Document> coll = getAndCreateCollection();

        Properties sourceProperties = new Properties();
        sourceProperties.put(MongoSourceConfig.DATABASE_CONFIG, coll.getNamespace().getDatabaseName());
        sourceProperties.put(MongoSourceConfig.COLLECTION_CONFIG, coll.getNamespace().getCollectionName());
        addSourceConnector(sourceProperties);

        insertMany(rangeClosed(1, 100), coll);
        assertProduced(createInserts(1, 100), coll);

        if (isGreaterThanThreeDotSix()) {
            coll.drop();
            assertProduced(concat(createInserts(1, 100), singletonList(createDropCollection())), coll);
        }
    }

    @Test
    @DisplayName("Ensure source loads data from collection with copy existing data")
    void testSourceLoadsDataFromCollectionCopyExisting() {
        MongoCollection<Document> coll = getAndCreateCollection();

        insertMany(rangeClosed(1, 50), coll);

        Properties sourceProperties = new Properties();
        sourceProperties.put(MongoSourceConfig.DATABASE_CONFIG, coll.getNamespace().getDatabaseName());
        sourceProperties.put(MongoSourceConfig.COLLECTION_CONFIG, coll.getNamespace().getCollectionName());
        sourceProperties.put(MongoSourceConfig.COPY_EXISTING_CONFIG, "true");
        addSourceConnector(sourceProperties);

        assertProduced(createInserts(1, 50), coll);

        insertMany(rangeClosed(51, 100), coll);
        assertProduced(createInserts(1, 100), coll);

        if (isGreaterThanThreeDotSix()) {
            coll.drop();
            assertProduced(concat(createInserts(1, 100), singletonList(createDropCollection())), coll);
        }
    }

    @Test
    @DisplayName("Ensure source can handle non existent collection and survive dropping")
    void testSourceCanHandleNonExistentCollectionAndSurviveDropping() throws InterruptedException {
        assumeTrue(isGreaterThanThreeDotSix());
        MongoCollection<Document> coll = getDatabaseWithPostfix().getCollection("coll");

        Properties sourceProperties = new Properties();
        sourceProperties.put(MongoSourceConfig.DATABASE_CONFIG, coll.getNamespace().getDatabaseName());
        sourceProperties.put(MongoSourceConfig.COLLECTION_CONFIG, coll.getNamespace().getCollectionName());
        addSourceConnector(sourceProperties);

        Thread.sleep(5000);
        assertProduced(emptyList(), coll);

        insertMany(rangeClosed(1, 100), coll);
        assertProduced(createInserts(1, 100), coll);

        coll.drop();
        assertProduced(concat(createInserts(1, 100), singletonList(createDropCollection())), coll);

        insertMany(rangeClosed(101, 200), coll);
        assertProduced(concat(createInserts(1, 100), singletonList(createDropCollection()), createInserts(101, 200)), coll);
    }

    @Test
    @DisplayName("Ensure source can handle a pipeline watching inserts on a non existent collection and survive dropping")
    void testSourceCanSurviveDroppingWithPipelineWatchingInsertsOnly() throws InterruptedException {
        assumeTrue(isGreaterThanThreeDotSix());
        MongoCollection<Document> coll = getDatabaseWithPostfix().getCollection("coll");

        Properties sourceProperties = new Properties();
        sourceProperties.put(MongoSourceConfig.DATABASE_CONFIG, coll.getNamespace().getDatabaseName());
        sourceProperties.put(MongoSourceConfig.COLLECTION_CONFIG, coll.getNamespace().getCollectionName());
        sourceProperties.put(MongoSourceConfig.PIPELINE_CONFIG, "[{\"$match\": {\"operationType\": \"insert\"}}]");
        addSourceConnector(sourceProperties);

        Thread.sleep(5000);
        assertProduced(emptyList(), coll);

        insertMany(rangeClosed(1, 50), coll);
        assertProduced(createInserts(1, 50), coll);

        coll.drop();
        Thread.sleep(5000);

        insertMany(rangeClosed(51, 100), coll);
        assertProduced(createInserts(1, 100), coll);
    }

    @Test
    @DisplayName("Ensure source loads data from collection and outputs documents only")
    void testSourceLoadsDataFromCollectionDocumentOnly() {
        MongoCollection<Document> coll = getAndCreateCollection();

        List<Document> docs = insertMany(rangeClosed(1, 50), coll);

        Properties sourceProperties = new Properties();
        sourceProperties.put(MongoSourceConfig.DATABASE_CONFIG, coll.getNamespace().getDatabaseName());
        sourceProperties.put(MongoSourceConfig.COLLECTION_CONFIG, coll.getNamespace().getCollectionName());
        sourceProperties.put(MongoSourceConfig.PUBLISH_FULL_DOCUMENT_ONLY_CONFIG, "true");
        sourceProperties.put(MongoSourceConfig.COPY_EXISTING_CONFIG, "true");
        addSourceConnector(sourceProperties);

        assertProducedDocs(docs, coll);

        List<Document> allDocs = new ArrayList<>(docs);
        allDocs.addAll(insertMany(rangeClosed(51, 100), coll));

        coll.drop();
        assertProducedDocs(allDocs, coll);
    }

    @Test
    @DisplayName("Ensure source can survive a restart")
    void testSourceSurvivesARestart() {
        MongoCollection<Document> coll = getAndCreateCollection();

        Properties sourceProperties = new Properties();
        sourceProperties.put(MongoSourceConfig.DATABASE_CONFIG, coll.getNamespace().getDatabaseName());
        sourceProperties.put(MongoSourceConfig.COLLECTION_CONFIG, coll.getNamespace().getCollectionName());
        addSourceConnector(sourceProperties);

        insertMany(rangeClosed(1, 50), coll);
        assertProduced(createInserts(1, 50), coll);

        restartSourceConnector(sourceProperties);
        insertMany(rangeClosed(51, 100), coll);

        assertProduced(createInserts(1, 100), coll);
    }

    @Test
    @DisplayName("Ensure source can survive a restart with a drop")
    void testSourceSurvivesARestartWithDrop() {
        assumeTrue(isGreaterThanThreeDotSix());
        MongoCollection<Document> coll = getDatabaseWithPostfix().getCollection("coll");

        Properties sourceProperties = new Properties();
        sourceProperties.put(MongoSourceConfig.DATABASE_CONFIG, coll.getNamespace().getDatabaseName());
        sourceProperties.put(MongoSourceConfig.COLLECTION_CONFIG, coll.getNamespace().getCollectionName());
        addSourceConnector(sourceProperties);

        insertMany(rangeClosed(1, 50), coll);
        assertProduced(createInserts(1, 50), coll);

        coll.drop();
        assertProduced(concat(createInserts(1, 50), singletonList(createDropCollection())), coll);

        restartSourceConnector(sourceProperties);
        insertMany(rangeClosed(51, 100), coll);

        assertProduced(concat(createInserts(1, 50), singletonList(createDropCollection()), createInserts(51, 100)), coll);
    }

    @Test
    @DisplayName("Ensure source can survive a restart with a drop when watching just inserts")
    void testSourceSurvivesARestartWithDropIncludingPipeline() {
        assumeTrue(isGreaterThanThreeDotSix());
        MongoCollection<Document> coll = getDatabaseWithPostfix().getCollection("coll");

public class MongoSourceConnectorTest extends MongoKafkaTestCase {

  @BeforeEach
  void setUp() {
    assumeTrue(isReplicaSetOrSharded());
    cleanUp();
  }

  @AfterEach
  void tearDown() {
    cleanUp();
  }

  @Test
  @DisplayName("Ensure source loads data from MongoClient")
  void testSourceLoadsDataFromMongoClient() {
    assumeTrue(isGreaterThanThreeDotSix());
    addSourceConnector();

    MongoDatabase db1 = getDatabaseWithPostfix();
    MongoDatabase db2 = getDatabaseWithPostfix();
    MongoDatabase db3 = getDatabaseWithPostfix();
    MongoCollection<Document> coll1 = db1.getCollection("coll");
    MongoCollection<Document> coll2 = db2.getCollection("coll");
    MongoCollection<Document> coll3 = db3.getCollection("coll");
    MongoCollection<Document> coll4 = db1.getCollection("db1Coll2");

    insertMany(rangeClosed(1, 50), coll1, coll2);

    db1.drop();
    sleep();
    insertMany(rangeClosed(51, 60), coll2, coll4);
    insertMany(rangeClosed(1, 70), coll3);

    assertAll(
        () ->
            assertProduced(
                concat(createInserts(1, 50), singletonList(createDropCollection())), coll1),
        () -> assertProduced(createInserts(1, 60), coll2),
        () -> assertProduced(createInserts(1, 70), coll3),
        () -> assertProduced(createInserts(51, 60), coll4));
  }

  @Test
  @DisplayName("Ensure source loads data from MongoClient with copy existing data")
  void testSourceLoadsDataFromMongoClientWithCopyExisting() {
    assumeTrue(isGreaterThanThreeDotSix());
    Properties sourceProperties = new Properties();
    sourceProperties.put(MongoSourceConfig.COPY_EXISTING_CONFIG, "true");
    addSourceConnector(sourceProperties);

    MongoDatabase db1 = getDatabaseWithPostfix();
    MongoDatabase db2 = getDatabaseWithPostfix();
    MongoDatabase db3 = getDatabaseWithPostfix();
    MongoCollection<Document> coll1 = db1.getCollection("coll");
    MongoCollection<Document> coll2 = db2.getCollection("coll");
    MongoCollection<Document> coll3 = db3.getCollection("coll");
    MongoCollection<Document> coll4 = db1.getCollection("db1Coll2");

    insertMany(rangeClosed(1, 50), coll1, coll2);
    db1.drop();
    sleep();
    insertMany(rangeClosed(51, 60), coll2, coll4);
    insertMany(rangeClosed(1, 70), coll3);

    assertAll(
        () ->
            assertProduced(
                concat(createInserts(1, 50), singletonList(createDropCollection())), coll1),
        () -> assertProduced(createInserts(1, 60), coll2),
        () -> assertProduced(createInserts(1, 70), coll3),
        () -> assertProduced(createInserts(51, 60), coll4));
  }

  @Test
  @DisplayName("Ensure source loads data from collection with copy existing data - outputting json")
  void testSourceLoadsDataFromCollectionCopyExistingJson() {
    assumeTrue(isGreaterThanFourDotZero());
    MongoCollection<Document> coll = getAndCreateCollection();

    insertMany(rangeClosed(1, 50), coll);

    Properties sourceProperties = new Properties();
    sourceProperties.put(MongoSourceConfig.DATABASE_CONFIG, coll.getNamespace().getDatabaseName());
    sourceProperties.put(
        MongoSourceConfig.COLLECTION_CONFIG, coll.getNamespace().getCollectionName());
    sourceProperties.put(MongoSourceConfig.COPY_EXISTING_CONFIG, "true");
    addSourceConnector(sourceProperties);

    insertMany(rangeClosed(51, 100), coll);
    assertProduced(createInserts(1, 100), coll);
  }

  @Test
  @DisplayName("Ensure source loads data from collection with copy existing data - outputting bson")
  void testSourceLoadsDataFromCollectionCopyExistingBson() {
    assumeTrue(isGreaterThanFourDotZero());
    MongoCollection<Document> coll = getAndCreateCollection();

    insertMany(rangeClosed(1, 50), coll);

    Properties sourceProperties = new Properties();
    sourceProperties.put(MongoSourceConfig.DATABASE_CONFIG, coll.getNamespace().getDatabaseName());
    sourceProperties.put(
        MongoSourceConfig.COLLECTION_CONFIG, coll.getNamespace().getCollectionName());
    sourceProperties.put(MongoSourceConfig.COPY_EXISTING_CONFIG, "true");
    sourceProperties.put(OUTPUT_FORMAT_KEY_CONFIG, OutputFormat.BSON.name());
    sourceProperties.put(MongoSourceConfig.OUTPUT_FORMAT_VALUE_CONFIG, OutputFormat.BSON.name());
    sourceProperties.put("key.converter", "org.apache.kafka.connect.converters.ByteArrayConverter");
    sourceProperties.put(
        "value.converter", "org.apache.kafka.connect.converters.ByteArrayConverter");

    addSourceConnector(sourceProperties);

    insertMany(rangeClosed(51, 100), coll);
    assertProduced(createInserts(1, 100), coll, OutputFormat.BSON);
  }

  @Test
  @DisplayName("Ensure Schema Key and Value output")
  void testSchemaKeyAndValueOutput() {
    assumeTrue(isGreaterThanFourDotZero());
    MongoCollection<Document> coll = getDatabaseWithPostfix().getCollection("coll");
    insertMany(rangeClosed(1, 10), coll);

    Properties sourceProperties = new Properties();
    sourceProperties.put(MongoSourceConfig.DATABASE_CONFIG, coll.getNamespace().getDatabaseName());
    sourceProperties.put(MongoSourceConfig.COPY_EXISTING_CONFIG, "true");

    sourceProperties.put(OUTPUT_FORMAT_KEY_CONFIG, OutputFormat.SCHEMA.name());
    sourceProperties.put(
        OUTPUT_SCHEMA_KEY_CONFIG,
        "{\"type\" : \"record\", \"name\" : \"key\","
            + "\"fields\" : [{\"name\": \"key\", \"type\": [\"int\",  \"null\"]}]}");
    sourceProperties.put(OUTPUT_FORMAT_VALUE_CONFIG, OutputFormat.SCHEMA.name());
    sourceProperties.put(
        OUTPUT_SCHEMA_VALUE_CONFIG,
        "{\"type\" : \"record\", \"name\" : \"fullDocument\","
            + "\"fields\" : [{\"name\": \"value\", \"type\": [\"int\",  \"null\"]}]}");
    sourceProperties.put("key.converter", "org.apache.kafka.connect.json.JsonConverter");
    sourceProperties.put("key.converter.schemas.enable", "false");
    sourceProperties.put("value.converter", "org.apache.kafka.connect.json.JsonConverter");
    sourceProperties.put("value.converter.schemas.enable", "false");
    sourceProperties.put(
        PIPELINE_CONFIG,
        "[{\"$addFields\": {\"key\": \"$fullDocument._id\", "
            + "\"value\": \"$fullDocument._id\"}}]");
    addSourceConnector(sourceProperties);

    List<ConsumerRecord<Integer, Integer>> expected =
        rangeClosed(1, 10)
            .boxed()
            .map(i -> new ConsumerRecord<>(coll.getNamespace().getFullName(), 0, 0, i, i))
            .collect(toList());

    Deserializer<Integer> deserializer = new KeyValueDeserializer();
    List<ConsumerRecord<Integer, Integer>> produced =
        getProduced(
            coll.getNamespace().getFullName(),
            deserializer,
            deserializer,
            cr ->
                new ConsumerRecord<>(coll.getNamespace().getFullName(), 0, 0, cr.key(), cr.value()),
            expected,
            10);

    assertIterableEquals(
        produced.stream().map(ConsumerRecord::key).collect(toList()),
        expected.stream().map(ConsumerRecord::key).collect(toList()));
    assertIterableEquals(
        produced.stream().map(ConsumerRecord::value).collect(toList()),
        expected.stream().map(ConsumerRecord::value).collect(toList()));
  }

  public static class KeyValueDeserializer implements Deserializer<Integer> {
    static final JsonDeserializer JSON_DESERIALIZER = new JsonDeserializer();

    @Override
    public Integer deserialize(final String topic, final byte[] data) {
      JsonNode node = JSON_DESERIALIZER.deserialize(topic, data);
      Iterable<String> iterable = node::fieldNames;
      List<String> fieldNames =
          StreamSupport.stream(iterable.spliterator(), false).collect(toList());
      if (fieldNames.contains("key")) {
        return node.get("key").asInt();
      } else if (fieldNames.contains("value")) {
        return node.get("value").asInt();
      }
      return -1;
    };
  }
}
