package ink.eason.tools.storage.bson;

import org.bson.RawBsonDocument;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;

import java.util.HashSet;
import java.util.Set;

import static ink.eason.tools.storage.bson.RawBsonProjector.ProjectionMode.EXCLUSIVE;
import static org.junit.jupiter.api.Assertions.assertEquals;

public class RawBsonProjectorExclusiveTest {

    @BeforeEach
    void setUp() {

    }

    @Test
    void testExclusiveProjectionWithAllBsonTypesAndNestedStructures() {
        // 原始 BSON 文档，包含所有常见的 BSON 类型和复杂的嵌套结构
        String originalJson = """
                {
                  "_id": { "$oid": "60a7e8e1f0b7c7a1b4e0d1f2" },
                  "name": "Test User",
                  "age": 30,
                  "balance": { "$numberDecimal": "123.45" },
                  "isActive": true,
                  "score": 99.5,
                  "description": "This is a test description.",
                  "lastLogin": { "$date": "2023-01-15T10:30:00Z" },
                  "binaryData": { "$binary": { "base64": "SGVsbG8gQmluYXJ5IERhdGE=", "subType": "00" } },
                  "nullField": null,
                  "undefinedField": { "$undefined": true },
                  "minKeyField": { "$minKey": 1 },
                  "maxKeyField": { "$maxKey": 1 },
                  "regexField": { "$regularExpression": { "pattern": "^abc", "options": "i" } },
                  "javascriptField": { "$javascript": "function() { return 1; }" },
                  "symbolField": { "$symbol": "testSymbol" },
                  "longField": { "$numberLong": "123456789012345" },
                  "timestampField": { "$timestamp": { "t": 1673778600, "i": 1 } },
                  "dbPointerField": { "$dbPointer": { "$ref": "collection", "$id": { "$oid": "60a7e8e1f0b7c7a1b4e0d1f3" } } },
                  "address": {
                    "street": "123 Main St",
                    "city": "Anytown",
                    "zip": "12345",
                    "coordinates": [40.7128, -74.0060]
                  },
                  "hobbies": [
                    "reading",
                    { "name": "hiking", "level": "intermediate" },
                    "swimming"
                  ],
                  "complexNesting": {
                    "level1Doc": {
                      "level2Array": [
                        "itemA",
                        {
                          "level3Doc": {
                            "deepValue1": "value1",
                            "deepArray1": [1, 2, { "finalDoc": "finalValue" }]
                          }
                        },
                        "itemB"
                      ],
                      "anotherField": "xyz"
                    }
                  },
                  "arrayOfDocsAndArrays": [
                    { "docInArray1": "value1A" },
                    [
                      "nestedArrayItem1",
                      { "docInNestedArray": "value1B" }
                    ],
                    { "docInArray2": "value2A", "nestedDoc": { "innerField": "innerValue" } }
                  ]
                }
                """;
        RawBsonDocument rawDoc = RawBsonDocument.parse(originalJson);

        // 排除的字段集合
        Set<String> projectionExclusions = new HashSet<>();
        projectionExclusions.add("age"); // 简单字段
        projectionExclusions.add("balance"); // Decimal128 字段
        projectionExclusions.add("address.city"); // 嵌套文档中的字段
        projectionExclusions.add("hobbies.1.level"); // 数组中文档的字段
        projectionExclusions.add("hobbies.2"); // 排除 hobbies 数组的第三个元素 ("swimming")

        projectionExclusions.add("complexNesting.level1Doc.level2Array.1.level3Doc.deepValue1"); // 深层嵌套字段
        projectionExclusions.add("complexNesting.level1Doc.level2Array.1.level3Doc.deepArray1.2.finalDoc"); // 数组中嵌套文档的字段

        projectionExclusions.add("arrayOfDocsAndArrays.0.docInArray1"); // 数组中文档的字段
        projectionExclusions.add("arrayOfDocsAndArrays.1.1.docInNestedArray"); // 数组中嵌套数组中文档的字段
        projectionExclusions.add("arrayOfDocsAndArrays.2.nestedDoc.innerField"); // 数组中文档中嵌套文档的字段

        // 排除整个数组中的一个文档元素
        projectionExclusions.add("arrayOfDocsAndArrays.0"); // 排除第一个文档元素

        // 排除 BSON 类型字段
        projectionExclusions.add("undefinedField"); // BSON Undefined 类型
        projectionExclusions.add("minKeyField"); // BSON MinKey 类型
        projectionExclusions.add("maxKeyField"); // BSON MaxKey 类型
        projectionExclusions.add("regexField"); // BSON RegularExpression 类型
        projectionExclusions.add("javascriptField"); // BSON JavaScript 类型
        projectionExclusions.add("symbolField"); // BSON Symbol 类型
        projectionExclusions.add("dbPointerField"); // BSON DBPointer 类型


        // 执行投影
        RawBsonDocument outputDoc = RawBsonProjector.project(rawDoc, projectionExclusions, EXCLUSIVE);

        // 预期输出 - 修正后，被排除的字段和数组元素应该完全消失
        String expectedJson = """
                {
                  "_id": { "$oid": "60a7e8e1f0b7c7a1b4e0d1f2" },
                  "name": "Test User",
                  "isActive": true,
                  "score": 99.5,
                  "description": "This is a test description.",
                  "lastLogin": { "$date": "2023-01-15T10:30:00Z" },
                  "binaryData": { "$binary": { "base64": "SGVsbG8gQmluYXJ5IERhdGE=", "subType": "00" } },
                  "nullField": null,
                  "longField": { "$numberLong": "123456789012345" },
                  "timestampField": { "$timestamp": { "t": 1673778600, "i": 1 } },
                  "address": {
                    "street": "123 Main St",
                    "zip": "12345",
                    "coordinates": [40.7128, -74.0060]
                  },
                  "hobbies": [
                    "reading",
                    { "name": "hiking" }
                  ],
                  "complexNesting": {
                    "level1Doc": {
                      "level2Array": [
                        "itemA",
                        {
                          "level3Doc": {
                            "deepArray1": [1, 2]
                          }
                        },
                        "itemB"
                      ],
                      "anotherField": "xyz"
                    }
                  },
                  "arrayOfDocsAndArrays": [
                    [
                      "nestedArrayItem1"
                    ],
                    { "docInArray2": "value2A" }
                  ]
                }
                """;
        RawBsonDocument expectedDoc = RawBsonDocument.parse(expectedJson);

        assertEquals(expectedDoc.toJson(), outputDoc.toJson());
    }

    @Test
    void testExclusiveProjectionOfRootLevelFields() {
        String originalJson = "{ \"a\": 1, \"b\": 2, \"c\": 3 }";
        RawBsonDocument rawDoc = RawBsonDocument.parse(originalJson);

        Set<String> exclusions = new HashSet<>();
        exclusions.add("b"); // 排除根级别的字段 'b'

        RawBsonDocument outputDoc = RawBsonProjector.project(rawDoc, exclusions, EXCLUSIVE);
        RawBsonDocument expectedDoc = RawBsonDocument.parse("{ \"a\": 1, \"c\": 3 }");

        assertEquals(expectedDoc.toJson(), outputDoc.toJson());
    }

    @Test
    void testExclusiveProjectionOfNonExistentField() {
        String originalJson = "{ \"a\": 1, \"b\": { \"c\": 2 } }";
        RawBsonDocument rawDoc = RawBsonDocument.parse(originalJson);

        Set<String> exclusions = new HashSet<>();
        exclusions.add("d"); // 排除不存在的根级别字段
        exclusions.add("b.e"); // 排除不存在的嵌套字段

        RawBsonDocument outputDoc = RawBsonProjector.project(rawDoc, exclusions, EXCLUSIVE);
        // 期望：因为排除的字段不存在，所以文档应该保持不变
        RawBsonDocument expectedDoc = RawBsonDocument.parse("{ \"a\": 1, \"b\": { \"c\": 2 } }");

        assertEquals(expectedDoc.toJson(), outputDoc.toJson());
    }

    @Test
    void testExclusiveProjectionOfEntireNestedDocument() {
        String originalJson = "{ \"user\": { \"name\": \"Alice\", \"age\": 30 }, \"product\": { \"id\": 101, \"price\": 99.99 } }";
        RawBsonDocument rawDoc = RawBsonDocument.parse(originalJson);

        Set<String> exclusions = new HashSet<>();
        exclusions.add("user"); // 排除整个 'user' 文档

        RawBsonDocument outputDoc = RawBsonProjector.project(rawDoc, exclusions, EXCLUSIVE);
        RawBsonDocument expectedDoc = RawBsonDocument.parse("{ \"product\": { \"id\": 101, \"price\": 99.99 } }");

        assertEquals(expectedDoc.toJson(), outputDoc.toJson());
    }

    @Test
    void testExclusiveProjectionOfEntireArray() {
        String originalJson = "{ \"items\": [1, 2, 3], \"settings\": { \"mode\": \"on\" } }";
        RawBsonDocument rawDoc = RawBsonDocument.parse(originalJson);

        Set<String> exclusions = new HashSet<>();
        exclusions.add("items"); // 排除整个 'items' 数组

        RawBsonDocument outputDoc = RawBsonProjector.project(rawDoc, exclusions, EXCLUSIVE);
        RawBsonDocument expectedDoc = RawBsonDocument.parse("{ \"settings\": { \"mode\": \"on\" } }");

        assertEquals(expectedDoc.toJson(), outputDoc.toJson());
    }

    @Test
    void testExclusiveProjectionWithEmptyExclusions() {
        String originalJson = "{ \"a\": 1, \"b\": 2 }";
        RawBsonDocument rawDoc = RawBsonDocument.parse(originalJson);

        Set<String> exclusions = new HashSet<>(); // 空的排除集合

        RawBsonDocument outputDoc = RawBsonProjector.project(rawDoc, exclusions, EXCLUSIVE);
        // 期望：没有排除任何字段，文档应该保持不变
        RawBsonDocument expectedDoc = RawBsonDocument.parse("{ \"a\": 1, \"b\": 2 }");

        assertEquals(expectedDoc.toJson(), outputDoc.toJson());
    }

    @Test
    void testExclusiveProjectionArrayElementsAreRemovedCompletely() {
        String originalJson = "{ \"arr\": [1, { \"a\": 1, \"b\": 2 }, 3, { \"x\": 1 }] }";
        RawBsonDocument rawDoc = RawBsonDocument.parse(originalJson);

        Set<String> exclusions = new HashSet<>();
        exclusions.add("arr.0"); // 排除数组第一个元素 (1)
        exclusions.add("arr.2"); // 排除数组第三个元素 (3)
        exclusions.add("arr.1.a"); // 排除数组第二个文档的 'a' 字段

        RawBsonDocument outputDoc = RawBsonProjector.project(rawDoc, exclusions, EXCLUSIVE);
        // 期望：arr.0 (1) 和 arr.2 (3) 被完全移除，arr.1 中的 'a' 被移除
        // 最终数组会变为 [ { "b": 2 }, { "x": 1 } ]
        RawBsonDocument expectedDoc = RawBsonDocument.parse("{ \"arr\": [ { \"b\": 2 }, { \"x\": 1 } ] }");
        assertEquals(expectedDoc.toJson(), outputDoc.toJson());
    }

    @Test
    void testExclusiveProjectionNestedDocumentsInArrayOfDocs() {
        String originalJson = """
                {
                  "employees": [
                    {
                      "id": 1,
                      "details": { "name": "Alice", "age": 30 },
                      "skills": ["Java", "Python"]
                    },
                    {
                      "id": 2,
                      "details": { "name": "Bob", "age": 25 },
                      "skills": ["C++", "Go"]
                    }
                  ]
                }
                """;
        RawBsonDocument rawDoc = RawBsonDocument.parse(originalJson);

        Set<String> exclusions = new HashSet<>();
        exclusions.add("employees.0.details.age"); // 排除第一个员工的年龄
        exclusions.add("employees.1.skills.0"); // 排除第二个员工的第一个技能
        exclusions.add("employees.0.skills"); // 排除第一个员工的整个 skills 数组
        exclusions.add("employees.1.details"); // 排除第二个员工的整个 details 文档
        exclusions.add("employees.0"); // 排除第一个员工整个文档

        RawBsonDocument outputDoc = RawBsonProjector.project(rawDoc, exclusions, EXCLUSIVE);
        String expectedJson = """
                {
                  "employees": [
                    {
                      "id": 2,
                      "skills": ["Go"]
                    }
                  ]
                }
                """;
        RawBsonDocument expectedDoc = RawBsonDocument.parse(expectedJson);

        assertEquals(expectedDoc.toJson(), outputDoc.toJson());
    }

    @Test
    void testExclusiveProjectionOnEmptyDocumentAndArray() {
        String originalJson = "{ \"emptyDoc\": {}, \"emptyArr\": [], \"data\": 1 }";
        RawBsonDocument rawDoc = RawBsonDocument.parse(originalJson);

        Set<String> exclusions = new HashSet<>();
        exclusions.add("emptyDoc"); // 排除空文档
        exclusions.add("emptyArr"); // 排除空数组

        RawBsonDocument outputDoc = RawBsonProjector.project(rawDoc, exclusions, EXCLUSIVE);
        RawBsonDocument expectedDoc = RawBsonDocument.parse("{ \"data\": 1 }");

        assertEquals(expectedDoc.toJson(), outputDoc.toJson());
    }

    @Test
    void testExclusiveProjectionOnAllFields() {
        String originalJson = "{ \"a\": 1, \"b\": 2, \"c\": 3 }";
        RawBsonDocument rawDoc = RawBsonDocument.parse(originalJson);

        Set<String> exclusions = new HashSet<>();
        exclusions.add("a");
        exclusions.add("b");
        exclusions.add("c");

        RawBsonDocument outputDoc = RawBsonProjector.project(rawDoc, exclusions, EXCLUSIVE);
        RawBsonDocument expectedDoc = RawBsonDocument.parse("{}"); // 排除所有字段后得到空文档

        assertEquals(expectedDoc.toJson(), outputDoc.toJson());
    }
}
