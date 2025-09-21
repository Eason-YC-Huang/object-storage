package ink.eason.tools.storage.bson;

import org.bson.BsonDocument;
import org.bson.RawBsonDocument;
import org.junit.jupiter.api.BeforeAll;
import org.junit.jupiter.api.DisplayName;
import org.junit.jupiter.api.Nested;
import org.junit.jupiter.api.Test;

import java.util.Set;

import static org.junit.jupiter.api.Assertions.assertNotNull;
import static org.junit.jupiter.api.Assertions.assertNull;

/**
 * 针对 RawBsonDocumentProjector 的过滤（Filtering）功能的综合测试。
 * 所有测试都通过 project 方法执行，断言返回结果是否为 null 来判断过滤是否成功。
 */
@DisplayName("RawBsonDocumentProjector 过滤器功能测试")
class RawBsonDocumentProjectorFilterTest {

    private static RawBsonDocumentProjector projector;
    private static RawBsonDocument testDoc;
    // 定义一个简单的投影，用于所有测试，以避免投影逻辑干扰过滤测试
    private static final Set<String> DUMMY_PROJECTION = Set.of("docId");

    @BeforeAll
    static void setUp() {
        projector = new RawBsonDocumentProjector();
        // 创建一个包含所有BSON类型和复杂嵌套结构的文档用于测试
        String json = """
                {
                  "docId": "doc-1",
                  "_id": { "$oid": "6514213e8a48af317e3e622a" },
                  "doubleValue": 123.456,
                  "stringValue": "hello world",
                  "intValue": 100,
                  "longValue": { "$numberLong": "9876543210" },
                  "decimalValue": { "$numberDecimal": "12345.6789" },
                  "booleanValue": true,
                  "nullValue": null,
                  "dateValue": { "$date": "2023-10-27T10:00:00Z" },
                  "binaryData": { "$binary": { "base64": "dGVzdA==", "subType": "00" } },
                  "regexStr": "abc123xyz",
                  "tags": ["mongodb", "java", "bson"],
                  "nestedDoc": {
                    "nestedString": "I am nested",
                    "nestedInt": 50,
                    "nestedArray": [10, 20]
                  },
                  "items": [
                    {
                      "itemId": "A",
                      "price": 19.99,
                      "stock": 20,
                      "details": {
                        "manufacturer": "ACME",
                        "colors": ["red", "blue"]
                      }
                    },
                    {
                      "itemId": "B",
                      "price": 29.99,
                      "stock": 15,
                      "details": {
                        "manufacturer": "XYZ",
                        "colors": ["green", "blue"]
                      }
                    }
                  ]
                }
                """;
        testDoc = RawBsonDocument.parse(json);
    }

    @Nested
    @DisplayName("基础类型过滤测试")
    class BasicTypeFilterTests {

        @Test
        @DisplayName("字符串: 使用 $eq 隐式匹配成功")
        void testMatch_String_ImplicitEq_Success() {
            BsonDocument filter = BsonDocument.parse("{ 'stringValue': 'hello world' }");
            RawBsonDocument result = projector.project(testDoc, DUMMY_PROJECTION, RawBsonDocumentProjector.ProjectionMode.INCLUSIVE, filter);
            assertNotNull(result, "当字符串值完全匹配时，文档应该被保留");
        }

        @Test
        @DisplayName("字符串: 使用 $ne 匹配成功")
        void testMatch_String_Ne_Success() {
            BsonDocument filter = BsonDocument.parse("{ 'stringValue': { '$ne': 'goodbye' } }");
            RawBsonDocument result = projector.project(testDoc, DUMMY_PROJECTION, RawBsonDocumentProjector.ProjectionMode.INCLUSIVE, filter);
            assertNotNull(result, "当字符串值不等于给定值时，文档应该被保留");
        }

        @Test
        @DisplayName("字符串: 使用 $ne 匹配失败")
        void testMatch_String_Ne_Fail() {
            BsonDocument filter = BsonDocument.parse("{ 'stringValue': { '$ne': 'hello world' } }");
            RawBsonDocument result = projector.project(testDoc, DUMMY_PROJECTION, RawBsonDocumentProjector.ProjectionMode.INCLUSIVE, filter);
            assertNull(result, "当字符串值等于 $ne 的值时，文档应该被过滤");
        }

        @Test
        @DisplayName("整型: 使用 $gt 匹配成功")
        void testMatch_Int_Gt_Success() {
            BsonDocument filter = BsonDocument.parse("{ 'intValue': { '$gt': 99 } }");
            RawBsonDocument result = projector.project(testDoc, DUMMY_PROJECTION, RawBsonDocumentProjector.ProjectionMode.INCLUSIVE, filter);
            assertNotNull(result, "当整型值大于给定值时，文档应该被保留");
        }

        @Test
        @DisplayName("整型: 使用 $lte 匹配失败")
        void testMatch_Int_Lte_Fail() {
            BsonDocument filter = BsonDocument.parse("{ 'intValue': { '$lte': 99 } }");
            RawBsonDocument result = projector.project(testDoc, DUMMY_PROJECTION, RawBsonDocumentProjector.ProjectionMode.INCLUSIVE, filter);
            assertNull(result, "当整型值不小于等于给定值时，文档应该被过滤");
        }

        @Test
        @DisplayName("长整型: 使用 $lt 匹配成功")
        void testMatch_Long_Lt_Success() {
            BsonDocument filter = BsonDocument.parse("{ 'longValue': { '$lt': { '$numberLong': '9876543211' } } }");
            RawBsonDocument result = projector.project(testDoc, DUMMY_PROJECTION, RawBsonDocumentProjector.ProjectionMode.INCLUSIVE, filter);
            assertNotNull(result, "当长整型值小于给定值时，文档应该被保留");
        }

        @Test
        @DisplayName("双精度浮点型: 使用 $gte 匹配成功")
        void testMatch_Double_Gte_Success() {
            BsonDocument filter = BsonDocument.parse("{ 'doubleValue': { '$gte': 123.456 } }");
            RawBsonDocument result = projector.project(testDoc, DUMMY_PROJECTION, RawBsonDocumentProjector.ProjectionMode.INCLUSIVE, filter);
            assertNotNull(result, "当浮点型值大于等于给定值时，文档应该被保留");
        }

        @Test
        @DisplayName("布尔型: 匹配 true 成功")
        void testMatch_Boolean_True_Success() {
            BsonDocument filter = BsonDocument.parse("{ 'booleanValue': true }");
            RawBsonDocument result = projector.project(testDoc, DUMMY_PROJECTION, RawBsonDocumentProjector.ProjectionMode.INCLUSIVE, filter);
            assertNotNull(result, "当布尔值为 true 时，文档应该被保留");
        }

        @Test
        @DisplayName("布尔型: 匹配 false 失败")
        void testMatch_Boolean_False_Fail() {
            BsonDocument filter = BsonDocument.parse("{ 'booleanValue': false }");
            RawBsonDocument result = projector.project(testDoc, DUMMY_PROJECTION, RawBsonDocumentProjector.ProjectionMode.INCLUSIVE, filter);
            assertNull(result, "当布尔值不为 false 时，文档应该被过滤");
        }

        @Test
        @DisplayName("日期: 使用 $eq 匹配成功")
        void testMatch_Date_Eq_Success() {
            BsonDocument filter = BsonDocument.parse("{ 'dateValue': { '$eq': { '$date': '2023-10-27T10:00:00Z' } } }");
            RawBsonDocument result = projector.project(testDoc, DUMMY_PROJECTION, RawBsonDocumentProjector.ProjectionMode.INCLUSIVE, filter);
            assertNotNull(result, "当日期值相等时，文档应该被保留");
        }

        @Test
        @DisplayName("ObjectId: 匹配成功")
        void testMatch_ObjectId_Success() {
            BsonDocument filter = BsonDocument.parse("{ '_id': { '$oid': '6514213e8a48af317e3e622a' } }");
            RawBsonDocument result = projector.project(testDoc, DUMMY_PROJECTION, RawBsonDocumentProjector.ProjectionMode.INCLUSIVE, filter);
            assertNotNull(result, "当 ObjectId 匹配时，文档应该被保留");
        }

        @Test
        @DisplayName("Null: 匹配 null 值成功")
        void testMatch_Null_Success() {
            BsonDocument filter = BsonDocument.parse("{ 'nullValue': null }");
            RawBsonDocument result = projector.project(testDoc, DUMMY_PROJECTION, RawBsonDocumentProjector.ProjectionMode.INCLUSIVE, filter);
            assertNotNull(result, "当字段值为 null 时，文档应该被保留");
        }

        @Test
        @DisplayName("正则表达式: 使用 $regex 匹配成功")
        void testMatch_Regex_Success() {
            BsonDocument filter = BsonDocument.parse("{ 'regexStr': { '$regex': '123' } }");
            RawBsonDocument result = projector.project(testDoc, DUMMY_PROJECTION, RawBsonDocumentProjector.ProjectionMode.INCLUSIVE, filter);
            assertNotNull(result, "当字符串符合正则表达式时，文档应该被保留");
        }
    }

    @Nested
    @DisplayName("数组和范围操作符过滤测试")
    class ArrayAndRangeOperatorTests {

        @Test
        @DisplayName("$in: 顶级字段值在列表中，匹配成功")
        void testMatch_In_TopLevel_Success() {
            BsonDocument filter = BsonDocument.parse("{ 'intValue': { '$in': [100, 200, 300] } }");
            RawBsonDocument result = projector.project(testDoc, DUMMY_PROJECTION, RawBsonDocumentProjector.ProjectionMode.INCLUSIVE, filter);
            assertNotNull(result, "当字段值在 $in 列表中时，文档应该被保留");
        }

        @Test
        @DisplayName("$in: 顶级字段值不在列表中，匹配失败")
        void testMatch_In_TopLevel_Fail() {
            BsonDocument filter = BsonDocument.parse("{ 'intValue': { '$in': [200, 300] } }");
            RawBsonDocument result = projector.project(testDoc, DUMMY_PROJECTION, RawBsonDocumentProjector.ProjectionMode.INCLUSIVE, filter);
            assertNull(result, "当字段值不在 $in 列表中时，文档应该被过滤");
        }

        @Test
        @DisplayName("$nin: 顶级字段值不在列表中，匹配成功")
        void testMatch_Nin_TopLevel_Success() {
            BsonDocument filter = BsonDocument.parse("{ 'stringValue': { '$nin': ['apple', 'banana'] } }");
            RawBsonDocument result = projector.project(testDoc, DUMMY_PROJECTION, RawBsonDocumentProjector.ProjectionMode.INCLUSIVE, filter);
            assertNotNull(result, "当字段值不在 $nin 列表中时，文档应该被保留");
        }
    }

    @Nested
    @DisplayName("逻辑操作符过滤测试")
    class LogicalOperatorTests {

        @Test
        @DisplayName("$and: 所有条件都为真，匹配成功")
        void testMatch_And_AllTrue_Success() {
            BsonDocument filter = BsonDocument.parse("{ '$and': [ { 'intValue': { '$gt': 50 } }, { 'booleanValue': true } ] }");
            RawBsonDocument result = projector.project(testDoc, DUMMY_PROJECTION, RawBsonDocumentProjector.ProjectionMode.INCLUSIVE, filter);
            assertNotNull(result, "$and 中所有条件都满足，文档应该被保留");
        }

        @Test
        @DisplayName("$and: 一个条件为假，匹配失败")
        void testMatch_And_OneFalse_Fail() {
            BsonDocument filter = BsonDocument.parse("{ '$and': [ { 'intValue': { '$gt': 50 } }, { 'booleanValue': false } ] }");
            RawBsonDocument result = projector.project(testDoc, DUMMY_PROJECTION, RawBsonDocumentProjector.ProjectionMode.INCLUSIVE, filter);
            assertNull(result, "$and 中有一个条件不满足，文档应该被过滤");
        }

        @Test
        @DisplayName("$or: 一个条件为真，匹配成功")
        void testMatch_Or_OneTrue_Success() {
            BsonDocument filter = BsonDocument.parse("{ '$or': [ { 'intValue': { '$lt': 50 } }, { 'booleanValue': true } ] }");
            RawBsonDocument result = projector.project(testDoc, DUMMY_PROJECTION, RawBsonDocumentProjector.ProjectionMode.INCLUSIVE, filter);
            assertNotNull(result, "$or 中有一个条件满足，文档应该被保留");
        }

        @Test
        @DisplayName("$or: 所有条件都为假，匹配失败")
        void testMatch_Or_AllFalse_Fail() {
            BsonDocument filter = BsonDocument.parse("{ '$or': [ { 'intValue': { '$lt': 50 } }, { 'booleanValue': false } ] }");
            RawBsonDocument result = projector.project(testDoc, DUMMY_PROJECTION, RawBsonDocumentProjector.ProjectionMode.INCLUSIVE, filter);
            assertNull(result, "$or 中所有条件都不满足，文档应该被过滤");
        }
    }

    @Nested
    @DisplayName("复杂嵌套结构过滤测试")
    class NestedStructureTests {

        @Test
        @DisplayName("嵌套文档: 对嵌套文档中的字段过滤成功")
        void testMatch_NestedDocumentField_Success() {
            // filter: nestedDoc.nestedInt > 49
            BsonDocument filter = BsonDocument.parse("{ 'nestedDoc.nestedInt': { '$gt': 49 } }");
            RawBsonDocument result = projector.project(testDoc, DUMMY_PROJECTION, RawBsonDocumentProjector.ProjectionMode.INCLUSIVE, filter);
            assertNotNull(result, "应能成功匹配嵌套文档中的字段");
        }

        @Test
        @DisplayName("嵌套文档: 对嵌套文档中的字段过滤失败")
        void testMatch_NestedDocumentField_Fail() {
            // filter: nestedDoc.nestedString == "non-existent"
            BsonDocument filter = BsonDocument.parse("{ 'nestedDoc.nestedString': 'non-existent' }");
            RawBsonDocument result = projector.project(testDoc, DUMMY_PROJECTION, RawBsonDocumentProjector.ProjectionMode.INCLUSIVE, filter);
            assertNull(result, "当嵌套文档中的字段值不匹配时，应过滤文档");
        }

        @Test
        @DisplayName("数组中的文档: 对数组中第一个文档的字段过滤成功")
        void testMatch_FieldInFirstDocInArray_Success() {
            // filter: items[0].price < 20
            BsonDocument filter = BsonDocument.parse("{ 'items.0.price': { '$lt': 20 } }");
            RawBsonDocument result = projector.project(testDoc, DUMMY_PROJECTION, RawBsonDocumentProjector.ProjectionMode.INCLUSIVE, filter);
            assertNotNull(result, "应能成功匹配数组中第一个文档的字段");
        }

        @Test
        @DisplayName("数组中的文档: 对数组中第二个文档的字段过滤成功")
        void testMatch_FieldInSecondDocInArray_Success() {
            // filter: items[1].stock == 15
            BsonDocument filter = BsonDocument.parse("{ 'items.1.stock': 15 }");
            RawBsonDocument result = projector.project(testDoc, DUMMY_PROJECTION, RawBsonDocumentProjector.ProjectionMode.INCLUSIVE, filter);
            assertNotNull(result, "应能成功匹配数组中第二个文档的字段");
        }

        @Test
        @DisplayName("数组中的文档: 过滤条件不满足，匹配失败")
        void testMatch_FieldInDocInArray_Fail() {
            // filter: items[0].itemId == "B"
            BsonDocument filter = BsonDocument.parse("{ 'items.0.itemId': 'B' }");
            RawBsonDocument result = projector.project(testDoc, DUMMY_PROJECTION, RawBsonDocumentProjector.ProjectionMode.INCLUSIVE, filter);
            assertNull(result, "当数组中指定文档的字段不匹配时，应过滤文档");
        }

        @Test
        @DisplayName("文档中数组的文档: 复杂嵌套过滤成功")
        void testMatch_DeeplyNestedField_Success() {
            // filter: items[0].details.manufacturer == "ACME"
            BsonDocument filter = BsonDocument.parse("{ 'items.0.details.manufacturer': 'ACME' }");
            RawBsonDocument result = projector.project(testDoc, DUMMY_PROJECTION, RawBsonDocumentProjector.ProjectionMode.INCLUSIVE, filter);
            assertNotNull(result, "应能成功匹配深层嵌套的字段");
        }

        @Test
        @DisplayName("文档中数组的文档: 复杂嵌套过滤失败")
        void testMatch_DeeplyNestedField_Fail() {
            // filter: items[1].details.manufacturer == "ACME"
            BsonDocument filter = BsonDocument.parse("{ 'items.1.details.manufacturer': 'ACME' }");
            RawBsonDocument result = projector.project(testDoc, DUMMY_PROJECTION, RawBsonDocumentProjector.ProjectionMode.INCLUSIVE, filter);
            assertNull(result, "当深层嵌套的字段不匹配时，应过滤文档");
        }

        @Test
        @DisplayName("不存在的字段: 对不存在的字段进行过滤应失败")
        void testMatch_NonExistentField_Fail() {
            BsonDocument filter = BsonDocument.parse("{ 'nonExistentField': 'someValue' }");
            RawBsonDocument result = projector.project(testDoc, DUMMY_PROJECTION, RawBsonDocumentProjector.ProjectionMode.INCLUSIVE, filter);
            assertNull(result, "对不存在的字段进行过滤时，文档应该被过滤");
        }
    }
}
