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
 * Comprehensive tests for the filtering functionality of RawBsonDocumentProjector.
 * All tests are executed via the project method, asserting whether the result is null to determine if the filter passed.
 */
@DisplayName("RawBsonDocumentProjector Filter Functionality Tests")
class RawBsonDocumentProjectorFilterTest {

    private static RawBsonDocumentProjector projector;
    private static RawBsonDocument testDoc;
    // Define a simple projection to be used in all tests to avoid projection logic interfering with filter tests
    private static final Set<String> DUMMY_PROJECTION = Set.of("docId");

    @BeforeAll
    static void setUp() {
        projector = new RawBsonDocumentProjector();
        // Create a document containing all BSON types and complex nested structures for testing
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
    @DisplayName("Basic Type Filter Tests")
    class BasicTypeFilterTests {

        @Test
        @DisplayName("String: Success with implicit $eq match")
        void testMatch_String_ImplicitEq_Success() {
            BsonDocument filter = BsonDocument.parse("{ 'stringValue': 'hello world' }");
            RawBsonDocument result = projector.project(testDoc, DUMMY_PROJECTION, RawBsonDocumentProjector.ProjectionMode.INCLUSIVE, filter);
            assertNotNull(result, "Document should be kept when string value matches exactly");
        }

        @Test
        @DisplayName("String: Success with $ne match")
        void testMatch_String_Ne_Success() {
            BsonDocument filter = BsonDocument.parse("{ 'stringValue': { '$ne': 'goodbye' } }");
            RawBsonDocument result = projector.project(testDoc, DUMMY_PROJECTION, RawBsonDocumentProjector.ProjectionMode.INCLUSIVE, filter);
            assertNotNull(result, "Document should be kept when string value does not equal the given value");
        }

        @Test
        @DisplayName("String: Fail with $ne match")
        void testMatch_String_Ne_Fail() {
            BsonDocument filter = BsonDocument.parse("{ 'stringValue': { '$ne': 'hello world' } }");
            RawBsonDocument result = projector.project(testDoc, DUMMY_PROJECTION, RawBsonDocumentProjector.ProjectionMode.INCLUSIVE, filter);
            assertNull(result, "Document should be filtered when string value equals the value in $ne");
        }

        @Test
        @DisplayName("Integer: Success with $gt match")
        void testMatch_Int_Gt_Success() {
            BsonDocument filter = BsonDocument.parse("{ 'intValue': { '$gt': 99 } }");
            RawBsonDocument result = projector.project(testDoc, DUMMY_PROJECTION, RawBsonDocumentProjector.ProjectionMode.INCLUSIVE, filter);
            assertNotNull(result, "Document should be kept when integer value is greater than the given value");
        }

        @Test
        @DisplayName("Integer: Fail with $lte match")
        void testMatch_Int_Lte_Fail() {
            BsonDocument filter = BsonDocument.parse("{ 'intValue': { '$lte': 99 } }");
            RawBsonDocument result = projector.project(testDoc, DUMMY_PROJECTION, RawBsonDocumentProjector.ProjectionMode.INCLUSIVE, filter);
            assertNull(result, "Document should be filtered when integer value is not less than or equal to the given value");
        }

        @Test
        @DisplayName("Long: Success with $lt match")
        void testMatch_Long_Lt_Success() {
            BsonDocument filter = BsonDocument.parse("{ 'longValue': { '$lt': { '$numberLong': '9876543211' } } }");
            RawBsonDocument result = projector.project(testDoc, DUMMY_PROJECTION, RawBsonDocumentProjector.ProjectionMode.INCLUSIVE, filter);
            assertNotNull(result, "Document should be kept when long value is less than the given value");
        }

        @Test
        @DisplayName("Double: Success with $gte match")
        void testMatch_Double_Gte_Success() {
            BsonDocument filter = BsonDocument.parse("{ 'doubleValue': { '$gte': 123.456 } }");
            RawBsonDocument result = projector.project(testDoc, DUMMY_PROJECTION, RawBsonDocumentProjector.ProjectionMode.INCLUSIVE, filter);
            assertNotNull(result, "Document should be kept when double value is greater than or equal to the given value");
        }

        @Test
        @DisplayName("Boolean: Success with true match")
        void testMatch_Boolean_True_Success() {
            BsonDocument filter = BsonDocument.parse("{ 'booleanValue': true }");
            RawBsonDocument result = projector.project(testDoc, DUMMY_PROJECTION, RawBsonDocumentProjector.ProjectionMode.INCLUSIVE, filter);
            assertNotNull(result, "Document should be kept when boolean value is true");
        }

        @Test
        @DisplayName("Boolean: Fail with false match")
        void testMatch_Boolean_False_Fail() {
            BsonDocument filter = BsonDocument.parse("{ 'booleanValue': false }");
            RawBsonDocument result = projector.project(testDoc, DUMMY_PROJECTION, RawBsonDocumentProjector.ProjectionMode.INCLUSIVE, filter);
            assertNull(result, "Document should be filtered when boolean value is not false");
        }

        @Test
        @DisplayName("Date: Success with $eq match")
        void testMatch_Date_Eq_Success() {
            BsonDocument filter = BsonDocument.parse("{ 'dateValue': { '$eq': { '$date': '2023-10-27T10:00:00Z' } } }");
            RawBsonDocument result = projector.project(testDoc, DUMMY_PROJECTION, RawBsonDocumentProjector.ProjectionMode.INCLUSIVE, filter);
            assertNotNull(result, "Document should be kept when date values are equal");
        }

        @Test
        @DisplayName("ObjectId: Success with match")
        void testMatch_ObjectId_Success() {
            BsonDocument filter = BsonDocument.parse("{ '_id': { '$oid': '6514213e8a48af317e3e622a' } }");
            RawBsonDocument result = projector.project(testDoc, DUMMY_PROJECTION, RawBsonDocumentProjector.ProjectionMode.INCLUSIVE, filter);
            assertNotNull(result, "Document should be kept when ObjectId matches");
        }

        @Test
        @DisplayName("Null: Success with null value match")
        void testMatch_Null_Success() {
            BsonDocument filter = BsonDocument.parse("{ 'nullValue': null }");
            RawBsonDocument result = projector.project(testDoc, DUMMY_PROJECTION, RawBsonDocumentProjector.ProjectionMode.INCLUSIVE, filter);
            assertNotNull(result, "Document should be kept when field value is null");
        }

        @Test
        @DisplayName("Regex: Success with $regex match")
        void testMatch_Regex_Success() {
            BsonDocument filter = BsonDocument.parse("{ 'regexStr': { '$regex': '123' } }");
            RawBsonDocument result = projector.project(testDoc, DUMMY_PROJECTION, RawBsonDocumentProjector.ProjectionMode.INCLUSIVE, filter);
            assertNotNull(result, "Document should be kept when string matches the regex");
        }
    }

    @Nested
    @DisplayName("Array and Range Operator Filter Tests")
    class ArrayAndRangeOperatorTests {

        @Test
        @DisplayName("$in: Success when top-level field value is in the list")
        void testMatch_In_TopLevel_Success() {
            BsonDocument filter = BsonDocument.parse("{ 'intValue': { '$in': [100, 200, 300] } }");
            RawBsonDocument result = projector.project(testDoc, DUMMY_PROJECTION, RawBsonDocumentProjector.ProjectionMode.INCLUSIVE, filter);
            assertNotNull(result, "Document should be kept when field value is in the $in list");
        }

        @Test
        @DisplayName("$in: Fail when top-level field value is not in the list")
        void testMatch_In_TopLevel_Fail() {
            BsonDocument filter = BsonDocument.parse("{ 'intValue': { '$in': [200, 300] } }");
            RawBsonDocument result = projector.project(testDoc, DUMMY_PROJECTION, RawBsonDocumentProjector.ProjectionMode.INCLUSIVE, filter);
            assertNull(result, "Document should be filtered when field value is not in the $in list");
        }

        @Test
        @DisplayName("$nin: Success when top-level field value is not in the list")
        void testMatch_Nin_TopLevel_Success() {
            BsonDocument filter = BsonDocument.parse("{ 'stringValue': { '$nin': ['apple', 'banana'] } }");
            RawBsonDocument result = projector.project(testDoc, DUMMY_PROJECTION, RawBsonDocumentProjector.ProjectionMode.INCLUSIVE, filter);
            assertNotNull(result, "Document should be kept when field value is not in the $nin list");
        }
    }

    @Nested
    @DisplayName("Logical Operator Filter Tests")
    class LogicalOperatorTests {

        @Test
        @DisplayName("$and: Success when all conditions are true")
        void testMatch_And_AllTrue_Success() {
            BsonDocument filter = BsonDocument.parse("{ '$and': [ { 'intValue': { '$gt': 50 } }, { 'booleanValue': true } ] }");
            RawBsonDocument result = projector.project(testDoc, DUMMY_PROJECTION, RawBsonDocumentProjector.ProjectionMode.INCLUSIVE, filter);
            assertNotNull(result, "Document should be kept when all conditions in $and are met");
        }

        @Test
        @DisplayName("$and: Fail when one condition is false")
        void testMatch_And_OneFalse_Fail() {
            BsonDocument filter = BsonDocument.parse("{ '$and': [ { 'intValue': { '$gt': 50 } }, { 'booleanValue': false } ] }");
            RawBsonDocument result = projector.project(testDoc, DUMMY_PROJECTION, RawBsonDocumentProjector.ProjectionMode.INCLUSIVE, filter);
            assertNull(result, "Document should be filtered when one condition in $and is not met");
        }

        @Test
        @DisplayName("$or: Success when one condition is true")
        void testMatch_Or_OneTrue_Success() {
            BsonDocument filter = BsonDocument.parse("{ '$or': [ { 'intValue': { '$lt': 50 } }, { 'booleanValue': true } ] }");
            RawBsonDocument result = projector.project(testDoc, DUMMY_PROJECTION, RawBsonDocumentProjector.ProjectionMode.INCLUSIVE, filter);
            assertNotNull(result, "Document should be kept when one condition in $or is met");
        }

        @Test
        @DisplayName("$or: Fail when all conditions are false")
        void testMatch_Or_AllFalse_Fail() {
            BsonDocument filter = BsonDocument.parse("{ '$or': [ { 'intValue': { '$lt': 50 } }, { 'booleanValue': false } ] }");
            RawBsonDocument result = projector.project(testDoc, DUMMY_PROJECTION, RawBsonDocumentProjector.ProjectionMode.INCLUSIVE, filter);
            assertNull(result, "Document should be filtered when all conditions in $or are not met");
        }
    }

    @Nested
    @DisplayName("Complex Nested Structure Filter Tests")
    class NestedStructureTests {

        @Test
        @DisplayName("Nested Document: Success filtering on a field in a nested document")
        void testMatch_NestedDocumentField_Success() {
            // filter: nestedDoc.nestedInt > 49
            BsonDocument filter = BsonDocument.parse("{ 'nestedDoc.nestedInt': { '$gt': 49 } }");
            RawBsonDocument result = projector.project(testDoc, DUMMY_PROJECTION, RawBsonDocumentProjector.ProjectionMode.INCLUSIVE, filter);
            assertNotNull(result, "Should successfully match a field in a nested document");
        }

        @Test
        @DisplayName("Nested Document: Fail filtering on a field in a nested document")
        void testMatch_NestedDocumentField_Fail() {
            // filter: nestedDoc.nestedString == "non-existent"
            BsonDocument filter = BsonDocument.parse("{ 'nestedDoc.nestedString': 'non-existent' }");
            RawBsonDocument result = projector.project(testDoc, DUMMY_PROJECTION, RawBsonDocumentProjector.ProjectionMode.INCLUSIVE, filter);
            assertNull(result, "Should filter document when the nested field value does not match");
        }

        @Test
        @DisplayName("Doc in Array: Success filtering on a field in the first document in an array")
        void testMatch_FieldInFirstDocInArray_Success() {
            // filter: items[0].price < 20
            BsonDocument filter = BsonDocument.parse("{ 'items.0.price': { '$lt': 20 } }");
            RawBsonDocument result = projector.project(testDoc, DUMMY_PROJECTION, RawBsonDocumentProjector.ProjectionMode.INCLUSIVE, filter);
            assertNotNull(result, "Should successfully match a field in the first document of an array");
        }

        @Test
        @DisplayName("Doc in Array: Success filtering on a field in the second document in an array")
        void testMatch_FieldInSecondDocInArray_Success() {
            // filter: items[1].stock == 15
            BsonDocument filter = BsonDocument.parse("{ 'items.1.stock': 15 }");
            RawBsonDocument result = projector.project(testDoc, DUMMY_PROJECTION, RawBsonDocumentProjector.ProjectionMode.INCLUSIVE, filter);
            assertNotNull(result, "Should successfully match a field in the second document of an array");
        }

        @Test
        @DisplayName("Doc in Array: Fail when filter condition is not met")
        void testMatch_FieldInDocInArray_Fail() {
            // filter: items[0].itemId == "B"
            BsonDocument filter = BsonDocument.parse("{ 'items.0.itemId': 'B' }");
            RawBsonDocument result = projector.project(testDoc, DUMMY_PROJECTION, RawBsonDocumentProjector.ProjectionMode.INCLUSIVE, filter);
            assertNull(result, "Should filter document when the specified field in a doc in an array does not match");
        }

        @Test
        @DisplayName("Deeply Nested: Success with deeply nested field filter")
        void testMatch_DeeplyNestedField_Success() {
            // filter: items[0].details.manufacturer == "ACME"
            BsonDocument filter = BsonDocument.parse("{ 'items.0.details.manufacturer': 'ACME' }");
            RawBsonDocument result = projector.project(testDoc, DUMMY_PROJECTION, RawBsonDocumentProjector.ProjectionMode.INCLUSIVE, filter);
            assertNotNull(result, "Should successfully match a deeply nested field");
        }

        @Test
        @DisplayName("Deeply Nested: Fail with deeply nested field filter")
        void testMatch_DeeplyNestedField_Fail() {
            // filter: items[1].details.manufacturer == "ACME"
            BsonDocument filter = BsonDocument.parse("{ 'items.1.details.manufacturer': 'ACME' }");
            RawBsonDocument result = projector.project(testDoc, DUMMY_PROJECTION, RawBsonDocumentProjector.ProjectionMode.INCLUSIVE, filter);
            assertNull(result, "Should filter document when the deeply nested field does not match");
        }

        @Test
        @DisplayName("Non-existent field: Filtering on a non-existent field should fail")
        void testMatch_NonExistentField_Fail() {
            BsonDocument filter = BsonDocument.parse("{ 'nonExistentField': 'someValue' }");
            RawBsonDocument result = projector.project(testDoc, DUMMY_PROJECTION, RawBsonDocumentProjector.ProjectionMode.INCLUSIVE, filter);
            assertNull(result, "Document should be filtered when filtering on a non-existent field");
        }
    }
}

