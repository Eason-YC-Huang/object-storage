package ink.eason.tools.storage.bson;

import ink.eason.tools.storage.bson.RawBsonDocumentProjector.ProjectionMode;
import org.bson.BsonDocument;
import org.bson.BsonString;
import org.bson.Document;
import org.bson.RawBsonDocument;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.DisplayName;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.params.ParameterizedTest;
import org.junit.jupiter.params.provider.Arguments;
import org.junit.jupiter.params.provider.MethodSource;

import java.nio.ByteBuffer;
import java.util.Collections;
import java.util.Set;
import java.util.stream.Stream;

import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertThrows;
import static org.junit.jupiter.params.provider.Arguments.arguments;

class RawBsonDocumentProjectorTest {

    private RawBsonDocumentProjector projector;

    private static final String rawJson = """
                {
                  "_id": { "$oid": "65fd79d47b59e42e191daab1" },
                  "name": "Comprehensive BSON Test",
                  "level": { "$numberInt": "100" },
                  "isPublished": true,
                  "rating": { "$numberDouble": "4.8" },
                  "version": { "$numberLong": "123456789012345" },
                  "price": { "$numberDecimal": "199.99" },
                  "author": {
                    "name": "Eason",
                    "email": "eason@example.com",
                    "address": {
                      "city": "Cyber City",
                      "zip": "90210"
                    }
                  },
                  "tags": [ "bson", "java", "mongodb", "test" ],
                  "history": [
                    { "version": 1, "action": "created", "details": { "by": "user1" } },
                    { "version": 2, "action": "updated", "status": "approved" },
                    { "version": 3, "action": "reviewed" }
                  ],
                  "binaryData": { "$binary": { "base64": "AAECAwQFBgc=", "subType": "00" } },
                  "creationDate": { "$date": "2025-09-20T10:30:00.000Z" },
                  "lastModified": { "$timestamp": { "t": 1758400200, "i": 1 } },
                  "regex": { "$regularExpression": { "pattern": "^test", "options": "i" } },
                  "dbPointer": { "$dbPointer": { "$ref": "users.prod", "$id": { "$oid": "65fd79d47b59e42e191daab2" } } },
                  "jsCodeWithScope": {
                    "$code": "function() { return x; }",
                    "$scope": { "x": 1 }
                  },
                  "symbol": { "$symbol": "mySymbol" },
                  "metadata": {
                    "nullableField": null,
                    "undefinedField": { "$undefined": true },
                    "minKeyField": { "$minKey": 1 },
                    "maxKeyField": { "$maxKey": 1 },
                    "extra": {
                      "level3": {
                        "value": "deeply nested value"
                      }
                    }
                  }
                }
                """;

    private static final RawBsonDocument PROTO_DOCUMENT = RawBsonDocument.parse(rawJson);

    @BeforeEach
    void setUp() {
        projector = new RawBsonDocumentProjector();
    }

    // 提供测试用例数据
    static Stream<Arguments> projectionTestCases() {
        return Stream.of(
                arguments(
                        "Projecting top-level primitive fields",
                        Set.of("name", "isPublished", "rating"),
                        """
                        {
                          "name": "Comprehensive BSON Test",
                          "isPublished": true,
                          "rating": { "$numberDouble": "4.8" }
                        }
                        """
                ),
                arguments(
                        "Projecting a whole nested document",
                        Set.of("author"),
                        """
                        {
                          "author": {
                            "name": "Eason",
                            "email": "eason@example.com",
                            "address": {
                              "city": "Cyber City",
                              "zip": "90210"
                            }
                          }
                        }
                        """
                ),
                arguments(
                        "Projecting specific fields from a nested document",
                        Set.of("author.name", "author.address.city"),
                        """
                        {
                          "author": {
                            "name": "Eason",
                            "address": {
                              "city": "Cyber City"
                            }
                          }
                        }
                        """
                ),
                arguments(
                        "Projecting a whole simple array",
                        Set.of("tags"),
                        """
                        { "tags": [ "bson", "java", "mongodb", "test" ] }
                        """
                ),
                arguments(
                        "Projecting a whole array of documents",
                        Set.of("history"),
                        """
                        {
                          "history": [
                            { "version": 1, "action": "created", "details": { "by": "user1" } },
                            { "version": 2, "action": "updated", "status": "approved" },
                            { "version": 3, "action": "reviewed" }
                          ]
                        }
                        """
                ),
                arguments(
                        "Projecting specific fields from all elements in an array of documents",
                        Set.of("history[0].action","history[1].action","history[2].action"),
                        """
                        {
                          "history": [
                            { "action": "created" },
                            { "action": "updated" },
                            { "action": "reviewed" }
                          ]
                        }
                        """
                ),
                arguments(
                        "Projecting array elements using index notation",
                        Set.of("tags[0]", "tags[2]"),
                        """
                        { "tags": [ "bson", "mongodb" ] }
                        """
                ),
                arguments(
                        "Projecting fields from specific elements in an array of documents",
                        Set.of("history[0].action", "history[0].details.by", "history[2].action", "level"),
                        """
                        {
                           "level": { "$numberInt": "100" },
                           "history": [
                             { "action": "created", "details": { "by": "user1" } },
                             { "action": "reviewed" }
                           ]
                        }
                        """
                ),
                arguments(
                        "Projecting a deeply nested field",
                        Set.of("metadata.extra.level3.value"),
                        """
                        {
                          "metadata": {
                            "extra": {
                              "level3": {
                                "value": "deeply nested value"
                              }
                            }
                          }
                        }
                        """
                ),
                arguments(
                        "Projecting a mix of existing and non-existing fields",
                        Set.of("name", "nonExistentField", "author.name", "author.nonExistent"),
                        """
                        {
                          "name": "Comprehensive BSON Test",
                          "author": { "name": "Eason" }
                        }
                        """
                ),
                arguments(
                        "Projection that results in an empty document because no fields match",
                        Set.of("nonExistent1", "nonExistent2.field"),
                        "{}"
                ),
                arguments(
                        "Projection that should produce an empty nested document (but mark/reset prevents it)",
                        Set.of("name", "author.nonExistent"),
                        """
                        { "name": "Comprehensive BSON Test" }
                        """
                ),
                arguments(
                        "Projection that should produce an empty array (but mark/reset prevents it)",
                        Set.of("name", "history[0].nonExistent"),
                        """
                        { "name": "Comprehensive BSON Test" }
                        """
                ),
                arguments(
                        "Projecting various BSON data types",
                        Set.of("_id", "version", "price", "binaryData", "creationDate", "lastModified", "regex", "jsCodeWithScope", "symbol"),
                        """
                        {
                          "_id": { "$oid": "65fd79d47b59e42e191daab1" },
                          "version": { "$numberLong": "123456789012345" },
                          "price": { "$numberDecimal": "199.99" },
                          "binaryData": { "$binary": { "base64": "AAECAwQFBgc=", "subType": "00" } },
                          "creationDate": { "$date": "2025-09-20T10:30:00.000Z" },
                          "lastModified": { "$timestamp": { "t": 1758400200, "i": 1 } },
                          "regex": { "$regularExpression": { "pattern": "^test", "options": "i" } },
                          "jsCodeWithScope": {
                            "$code": "function() { return x; }",
                            "$scope": { "x": 1 }
                          },
                          "symbol": { "$symbol": "mySymbol" }
                        }
                        """
                ),
                arguments(
                        "Projecting special BSON data types (null, undefined, min/max keys)",
                        Set.of("metadata.nullableField", "metadata.undefinedField", "metadata.minKeyField", "metadata.maxKeyField"),
                        """
                       {
                         "metadata": {
                           "nullableField": null,
                           "undefinedField": { "$undefined": true },
                           "minKeyField": { "$minKey": 1 },
                           "maxKeyField": { "$maxKey": 1 }
                         }
                       }
                       """
                )
        );
    }

    @ParameterizedTest(name = "[{index}] {0}")
    @MethodSource("projectionTestCases")
    @DisplayName("Should correctly project fields based on various projections")
    void shouldProjectCorrectly(String testName, Set<String> projection, String expectedJson) {

        byte[] docBytes = new byte[PROTO_DOCUMENT.getByteBuffer().asNIO().remaining()];
        PROTO_DOCUMENT.getByteBuffer().asNIO().get(docBytes);
        RawBsonDocument document = new RawBsonDocument(docBytes);

        RawBsonDocument result = projector.project(document, projection);

        // 解析为Document对象进行比较，忽略字段顺序
        Document expectedDoc = Document.parse(expectedJson);
        Document actualDoc = Document.parse(result.toJson());

        assertEquals(expectedDoc, actualDoc);
    }

    @Test
    @DisplayName("Should throw IllegalArgumentException for null projection")
    void shouldThrowExceptionForNullProjection() {
        assertThrows(IllegalArgumentException.class, () -> {
            byte[] docBytes = new byte[PROTO_DOCUMENT.getByteBuffer().asNIO().remaining()];
            PROTO_DOCUMENT.getByteBuffer().asNIO().get(docBytes);
            RawBsonDocument document = new RawBsonDocument(docBytes);
            projector.project(document, null);
        });
    }

    @Test
    @DisplayName("Should throw IllegalArgumentException for empty projection")
    void shouldThrowExceptionForEmptyProjection() {
        assertThrows(IllegalArgumentException.class, () -> {
            byte[] docBytes = new byte[PROTO_DOCUMENT.getByteBuffer().asNIO().remaining()];
            PROTO_DOCUMENT.getByteBuffer().asNIO().get(docBytes);
            RawBsonDocument document = new RawBsonDocument(docBytes);
            projector.project(document, Collections.emptySet());
        });
    }

    @Test
    void testFilter() {
        byte[] docBytes = new byte[PROTO_DOCUMENT.getByteBuffer().asNIO().remaining()];
        PROTO_DOCUMENT.getByteBuffer().asNIO().get(docBytes);
        RawBsonDocument document = new RawBsonDocument(docBytes);
        RawBsonDocument result = projector.project(document, Collections.emptySet(), ProjectionMode.EXCLUSIVE, new BsonDocument()
                .append("name", new BsonString("Comprehensive BSON Test"))
                .append("author.name", new BsonString("Eason")));
        System.out.println(result);

    }
}
