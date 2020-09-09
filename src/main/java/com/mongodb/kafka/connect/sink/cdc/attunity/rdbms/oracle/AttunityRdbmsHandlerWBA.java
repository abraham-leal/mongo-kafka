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
 *
 * Original Work: Apache License, Version 2.0, Copyright 2017 Hans-Peter Grahsl.
 */

/*
 * Attunity extension by: Abraham Leal
 */

package com.mongodb.kafka.connect.sink.cdc.attunity.rdbms.oracle;

import java.nio.charset.StandardCharsets;
import java.util.Map;

import org.apache.kafka.clients.producer.KafkaProducer;
import org.apache.kafka.clients.producer.ProducerRecord;
import org.apache.kafka.connect.errors.DataException;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import org.bson.BsonDocument;
import org.bson.BsonInvalidOperationException;
import org.bson.BsonObjectId;

import com.mongodb.kafka.connect.sink.MongoSinkTopicConfig;
import com.mongodb.kafka.connect.sink.cdc.CdcOperation;
import com.mongodb.kafka.connect.sink.cdc.debezium.OperationType;
import com.mongodb.kafka.connect.sink.converter.SinkDocument;

public class AttunityRdbmsHandlerWBA extends AttunityWBACdcHandler {
  static final String ID_FIELD = "_id";
  private static final String JSON_DOC_BEFORE_FIELD = "beforeData";
  private static final String JSON_DOC_AFTER_FIELD = "data";
  private static final String JSON_DOC_WRAPPER_FIELD = "message";
  private boolean keyExtractionEnabled = false;
  private java.util.List<String> fieldsToExtract = new java.util.ArrayList<>();
  private static final Logger LOGGER = LoggerFactory.getLogger(AttunityRdbmsHandlerWBA.class);
  private static final java.util.Map<
          com.mongodb.kafka.connect.sink.cdc.debezium.OperationType,
          com.mongodb.kafka.connect.sink.cdc.CdcOperation>
      DEFAULT_OPERATIONS =
          new java.util.HashMap<
              com.mongodb.kafka.connect.sink.cdc.debezium.OperationType,
              com.mongodb.kafka.connect.sink.cdc.CdcOperation>() {
            {
              put(OperationType.CREATE, new AttunityRdbmsInsert());
              put(OperationType.READ, new AttunityRdbmsInsert());
              put(OperationType.UPDATE, new AttunityRdbmsUpdate());
              put(OperationType.DELETE, new AttunityRdbmsDelete());
            }
          };

  public AttunityRdbmsHandlerWBA(final MongoSinkTopicConfig config) {
    this(config, DEFAULT_OPERATIONS);
  }

  public AttunityRdbmsHandlerWBA(
      final MongoSinkTopicConfig config, final Map<OperationType, CdcOperation> operations) {
    super(config);
    keyExtractionEnabled =
        getConfig().originals().get("key.extraction.enabled") != null
            && getConfig()
                .originalsStrings()
                .get("key.extraction.enabled")
                .equalsIgnoreCase("true");
    if (keyExtractionEnabled) {
      LOGGER.info("Key extraction from value is enabled!");
      fieldsToExtract =
          java.util.Arrays.asList(
              getConfig()
                  .getString(MongoSinkTopicConfig.KEY_PROJECTION_LIST_CONFIG)
                  .split("\\s*,\\s*"));
    }
    registerOperations(operations);
  }

  @Override
  public java.util.Optional<com.mongodb.client.model.WriteModel<org.bson.BsonDocument>> handle(
      final SinkDocument doc) {
    BsonDocument keyDoc = new BsonDocument();

    BsonDocument valueDoc = doc.getValueDoc().orElseGet(BsonDocument::new);

    if (valueDoc.isEmpty()) {
      LOGGER.debug("skipping attunity tombstone event for kafka topic compaction");
      return java.util.Optional.empty();
    }

    if (keyExtractionEnabled) {
      if (valueDoc.containsKey(JSON_DOC_WRAPPER_FIELD)) {
        for (String field : fieldsToExtract) {
          keyDoc.put(
              field,
              valueDoc
                  .getDocument(JSON_DOC_WRAPPER_FIELD)
                  .getDocument(JSON_DOC_AFTER_FIELD)
                  .get(field));
        }
      } else {
        for (String field : fieldsToExtract) {
          keyDoc.put(field, valueDoc.getDocument(JSON_DOC_AFTER_FIELD).get(field));
        }
      }
    } else {
      keyDoc = doc.getKeyDoc().orElseGet(BsonDocument::new);
    }

    return java.util.Optional.ofNullable(
        getCdcOperation(valueDoc).perform(new SinkDocument(keyDoc, valueDoc)));
  }

  @Override
  public java.util.Optional<com.mongodb.client.model.WriteModel<org.bson.BsonDocument>> handle(
      final SinkDocument doc, final KafkaProducer<String, String> dlqProducer) {

    BsonDocument keyDoc = new BsonDocument();

    BsonDocument valueDoc = doc.getValueDoc().orElseGet(BsonDocument::new);

    if (valueDoc.isEmpty()) {
      LOGGER.debug("skipping attunity tombstone event for kafka topic compaction");
      return java.util.Optional.empty();
    }

    try {
      if (keyExtractionEnabled) {
        if (valueDoc.containsKey(JSON_DOC_WRAPPER_FIELD)) {
          for (String field : fieldsToExtract) {
            keyDoc.put(
                field,
                valueDoc
                    .getDocument(JSON_DOC_WRAPPER_FIELD)
                    .getDocument(JSON_DOC_AFTER_FIELD)
                    .get(field));
          }
        } else {
          for (String field : fieldsToExtract) {
            keyDoc.put(field, valueDoc.getDocument(JSON_DOC_AFTER_FIELD).get(field));
          }
        }
      } else {
        keyDoc = doc.getKeyDoc().orElseGet(BsonDocument::new);
      }

      return java.util.Optional.ofNullable(
          getCdcOperation(valueDoc).perform(new SinkDocument(keyDoc, valueDoc)));
    } catch (Exception e) {
      ProducerRecord<String, String> badRecord =
          new ProducerRecord<>(
              getConfig().originals().get("errors.deadletterqueue.topic.name").toString(),
              keyDoc.toJson(),
              valueDoc.toJson());
      badRecord.headers().add("stacktrace", e.toString().getBytes(StandardCharsets.UTF_8));
      dlqProducer.send(badRecord);
      LOGGER.info("Bad Record, sending to DLQ...");
      return java.util.Optional.empty();
    }
  }

  static BsonDocument generateFilterDoc(
      final BsonDocument keyDoc, final BsonDocument valueDoc, final OperationType opType) {
    final boolean checkOpType =
        opType.equals(OperationType.CREATE) || opType.equals(OperationType.READ);
    if (valueDoc.containsKey(JSON_DOC_WRAPPER_FIELD)) {
      if (keyDoc.keySet().isEmpty()) {
        if (checkOpType) {
          // create: no PK info in keyDoc -> generate ObjectId
          return new BsonDocument(ID_FIELD, new BsonObjectId());
        }
        // update or delete: no PK info in keyDoc -> take everything in 'beforeData' field
        try {
          BsonDocument filter =
              valueDoc.getDocument(JSON_DOC_WRAPPER_FIELD).getDocument(JSON_DOC_BEFORE_FIELD);
          if (filter.isEmpty()) {
            throw new BsonInvalidOperationException("value doc beforeData field is empty");
          }
          return filter;
        } catch (BsonInvalidOperationException exc) {
          throw new DataException(
              "Error: value doc 'beforeData' field is empty or has invalid type"
                  + " for update/delete operation which seems severely wrong -> defensive actions taken!",
              exc);
        }
      }
    } else {
      if (keyDoc.keySet().isEmpty()) {
        if (checkOpType) {
          // create: no PK info in keyDoc -> generate ObjectId
          return new BsonDocument(ID_FIELD, new BsonObjectId());
        }
        // update or delete: no PK info in keyDoc -> take everything in 'beforeData' field
        try {
          BsonDocument filter = valueDoc.getDocument(JSON_DOC_BEFORE_FIELD);
          if (filter.isEmpty()) {
            throw new BsonInvalidOperationException("value doc beforeData field is empty");
          }
          return filter;
        } catch (BsonInvalidOperationException exc) {
          throw new DataException(
              "Error: value doc 'beforeData' field is empty or has invalid type"
                  + " for update/delete operation which seems severely wrong -> defensive actions taken!",
              exc);
        }
      }
    }

    // build filter document composed of all PK columns
    BsonDocument pk = new BsonDocument();
    for (String f : keyDoc.keySet()) {
      pk.put(f, keyDoc.get(f));
    }
    return new BsonDocument(ID_FIELD, pk);
  }

  static BsonDocument generateUpsertOrReplaceDoc(
      final BsonDocument keyDoc, final BsonDocument valueDoc, final BsonDocument filterDoc) {
    BsonDocument afterDoc;
    BsonDocument upsertDoc = new BsonDocument();
    if (valueDoc.containsKey(JSON_DOC_WRAPPER_FIELD)) {
      if (!valueDoc.getDocument(JSON_DOC_WRAPPER_FIELD).containsKey(JSON_DOC_AFTER_FIELD)
          || valueDoc.getDocument(JSON_DOC_WRAPPER_FIELD).get(JSON_DOC_AFTER_FIELD).isNull()
          || !valueDoc.getDocument(JSON_DOC_WRAPPER_FIELD).get(JSON_DOC_AFTER_FIELD).isDocument()
          || valueDoc
              .getDocument(JSON_DOC_WRAPPER_FIELD)
              .getDocument(JSON_DOC_AFTER_FIELD)
              .isEmpty()) {
        throw new DataException(
            "Error: valueDoc must contain non-empty 'data' field"
                + " of type document for insert/update operation");
      }

      afterDoc = valueDoc.getDocument(JSON_DOC_WRAPPER_FIELD).getDocument(JSON_DOC_AFTER_FIELD);
    } else {
      if (!valueDoc.containsKey(JSON_DOC_AFTER_FIELD)
          || valueDoc.get(JSON_DOC_AFTER_FIELD).isNull()
          || !valueDoc.get(JSON_DOC_AFTER_FIELD).isDocument()
          || valueDoc.getDocument(JSON_DOC_AFTER_FIELD).isEmpty()) {
        throw new DataException(
            "Error: valueDoc must contain non-empty 'data' field"
                + " of type document for insert/update operation");
      }

      afterDoc = valueDoc.getDocument(JSON_DOC_AFTER_FIELD);
    }

    if (filterDoc.containsKey(ID_FIELD)) {
      upsertDoc.put(ID_FIELD, filterDoc.get(ID_FIELD));
    }

    for (String f : afterDoc.keySet()) {
      upsertDoc.put(f, afterDoc.get(f));
    }
    return upsertDoc;
  }

  static BsonDocument generateUpdateDoc(
      final BsonDocument keyDoc, final BsonDocument valueDoc, final BsonDocument filterDoc) {
    BsonDocument updateDoc = new BsonDocument();
    BsonDocument updates = new BsonDocument();
    BsonDocument beforeDoc;
    BsonDocument afterDoc;
    if (valueDoc.containsKey(JSON_DOC_WRAPPER_FIELD)) {
      if (!valueDoc.getDocument(JSON_DOC_WRAPPER_FIELD).containsKey(JSON_DOC_AFTER_FIELD)
          || valueDoc.getDocument(JSON_DOC_WRAPPER_FIELD).get(JSON_DOC_AFTER_FIELD).isNull()
          || !valueDoc.getDocument(JSON_DOC_WRAPPER_FIELD).get(JSON_DOC_AFTER_FIELD).isDocument()
          || valueDoc
              .getDocument(JSON_DOC_WRAPPER_FIELD)
              .getDocument(JSON_DOC_AFTER_FIELD)
              .isEmpty()) {
        throw new DataException(
            "Error: valueDoc must contain non-empty 'data' field"
                + " of type document for insert/update operation");
      }

      beforeDoc = valueDoc.getDocument(JSON_DOC_WRAPPER_FIELD).getDocument(JSON_DOC_BEFORE_FIELD);
      afterDoc = valueDoc.getDocument(JSON_DOC_WRAPPER_FIELD).getDocument(JSON_DOC_AFTER_FIELD);

    } else {
      if (!valueDoc.containsKey(JSON_DOC_AFTER_FIELD)
          || valueDoc.get(JSON_DOC_AFTER_FIELD).isNull()
          || !valueDoc.get(JSON_DOC_AFTER_FIELD).isDocument()
          || valueDoc.getDocument(JSON_DOC_AFTER_FIELD).isEmpty()) {
        throw new DataException(
            "Error: valueDoc must contain non-empty 'data' field"
                + " of type document for insert/update operation");
      }
      beforeDoc = valueDoc.getDocument(JSON_DOC_BEFORE_FIELD);
      afterDoc = valueDoc.getDocument(JSON_DOC_AFTER_FIELD);
    }

    for (String key : afterDoc.keySet()) {
      if (!afterDoc.get(key).equals(beforeDoc.get(key))) {
        updates.put(key, afterDoc.get(key));
      }
    }

    updateDoc.append("$set", updates);
    return updateDoc;
  }
}
