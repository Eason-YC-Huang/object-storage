package ink.eason.tools.storage.bson;

import org.bson.BsonBinaryReader;
import org.bson.BsonBinaryWriter;
import org.bson.BsonInvalidOperationException;
import org.bson.BsonType;
import org.bson.RawBsonDocument;
import org.bson.io.BasicOutputBuffer;

import java.nio.ByteBuffer;
import java.nio.ByteOrder;
import java.util.Objects;
import java.util.Set;

/**
 * 一个高效的、基于流的BSON投影器，其行为与MongoDB的投影逻辑对齐。
 * 它直接在ByteBuffer上操作，通过局部缓冲来避免为空的子文档创建父字段。
 */
public class RawBsonDocumentProjector {

    private static final String ROOT_PATH = "";

    public RawBsonDocument project(RawBsonDocument input, Set<String> projection) {
        Objects.requireNonNull(input);
        ByteBuffer output = this.project(input.getByteBuffer().asNIO(), projection);
        return new RawBsonDocument(output.array(), 0, output.remaining());
    }

    private ByteBuffer project(ByteBuffer bsonInputByteBuffer, Set<String> projection) {
        if (projection == null || projection.isEmpty()) {
            throw new IllegalArgumentException("projection is null or empty");
        }

        ByteBuffer bsonOutputByteBuffer = ByteBuffer.allocate(bsonInputByteBuffer.remaining());

        try (BsonBinaryReader reader = new BsonBinaryReader(bsonInputByteBuffer);
             BsonBinaryWriter writer = new BsonBinaryWriter(new BsonOutputByteBuffer(bsonOutputByteBuffer))) {
            pipeDocument(reader, writer, projection, ROOT_PATH);
        }

        bsonOutputByteBuffer.limit(bsonOutputByteBuffer.position());
        bsonOutputByteBuffer.position(0);
        return bsonOutputByteBuffer;
    }

    /**
     * 递归地处理BSON文档的投影。
     */
    private void pipeDocument(BsonBinaryReader reader, BsonBinaryWriter writer,
                              Set<String> projKeys, String currentPath) {
        reader.readStartDocument();
        writer.writeStartDocument();

        while (reader.readBsonType() != BsonType.END_OF_DOCUMENT) {
            String fieldName = reader.readName();
            BsonType bsonType = reader.getCurrentBsonType();

            String fullPath = currentPath.isEmpty() ? fieldName : currentPath + "." + fieldName;
            boolean isExactMatch = projKeys.contains(fullPath);

            if (isExactMatch) {
                if ((bsonType == BsonType.DOCUMENT)) {
                    writer.writeName(fieldName);
                    writer.pipe(reader);
                } else {
                    writer.writeName(fieldName);
                    pipeValue(reader, writer, projKeys, fullPath);
                }
            } else if (bsonType == BsonType.DOCUMENT){
                // None Exact Matched Document
                BasicOutputBuffer tempBuffer = new BasicOutputBuffer();
                BsonBinaryWriter tempWriter = new BsonBinaryWriter(tempBuffer);

                tempWriter.writeStartDocument();
                tempWriter.writeName("v");
                pipeValue(reader, tempWriter, projKeys, fullPath);
                tempWriter.writeEndDocument();

                byte[] tempBytes = tempBuffer.toByteArray();
                if (tempBytes.length > 13) {
                    writer.writeName(fieldName);
                    BsonBinaryReader tempReader = new BsonBinaryReader(ByteBuffer.wrap(tempBytes).order(ByteOrder.LITTLE_ENDIAN));
                    tempReader.readStartDocument();
                    tempReader.readName("v");
                    writer.pipe(tempReader);
                    tempReader.close();
                }

            } else {
                reader.skipValue();
            }

        }

        reader.readEndDocument();
        writer.writeEndDocument();
    }

    private void pipeArray(BsonBinaryReader reader, BsonBinaryWriter writer,
                           Set<String> projKeys, String currentPath) {
        reader.readStartArray();
        writer.writeStartArray();
        while (reader.readBsonType() != BsonType.END_OF_DOCUMENT) {
            pipeValue(reader, writer, projKeys, currentPath);
        }
        reader.readEndArray();
        writer.writeEndArray();
    }

    /**
     * 根据当前BSON类型，将值从reader传输到writer，并在必要时进行递归投影。
     */
    private void pipeValue(BsonBinaryReader reader, BsonBinaryWriter writer,
                           Set<String> projKeys, String currentPath) {
        BsonType bsonType = reader.getCurrentBsonType();
        switch (bsonType) {
            case DOCUMENT:
                pipeDocument(reader, writer, projKeys, currentPath);
                break;
            case ARRAY:
                pipeArray(reader, writer, projKeys, currentPath);
                break;
            // 为所有其他原始类型直接传输值
            case DOUBLE:
                writer.writeDouble(reader.readDouble());
                break;
            case STRING:
                writer.writeString(reader.readString());
                break;
            case BINARY:
                writer.writeBinaryData(reader.readBinaryData());
                break;
            case UNDEFINED:
                reader.readUndefined();
                writer.writeUndefined();
                break;
            case OBJECT_ID:
                writer.writeObjectId(reader.readObjectId());
                break;
            case BOOLEAN:
                writer.writeBoolean(reader.readBoolean());
                break;
            case DATE_TIME:
                writer.writeDateTime(reader.readDateTime());
                break;
            case NULL:
                reader.readNull();
                writer.writeNull();
                break;
            case REGULAR_EXPRESSION:
                writer.writeRegularExpression(reader.readRegularExpression());
                break;
            case JAVASCRIPT:
                //writer.writeJavaScript(reader.readJavaScript());
                throw new IllegalArgumentException("JAVASCRIPT type is not supported");
            case SYMBOL:
                writer.writeSymbol(reader.readSymbol());
                break;
            case JAVASCRIPT_WITH_SCOPE:
                writer.writeJavaScriptWithScope(reader.readJavaScriptWithScope());
                break;
            case INT32:
                writer.writeInt32(reader.readInt32());
                break;
            case TIMESTAMP:
                writer.writeTimestamp(reader.readTimestamp());
                break;
            case INT64:
                writer.writeInt64(reader.readInt64());
                break;
            case DECIMAL128:
                writer.writeDecimal128(reader.readDecimal128());
                break;
            case MIN_KEY:
                reader.readMinKey();
                writer.writeMinKey();
                break;
            case DB_POINTER:
                writer.writeDBPointer(reader.readDBPointer());
                break;
            case MAX_KEY:
                reader.readMaxKey();
                writer.writeMaxKey();
                break;
            default:
                throw new BsonInvalidOperationException("Unsupported BSON type: " + bsonType);
        }
    }


    public static void main(String[] args) {
        RawBsonDocument rawDoc = RawBsonDocument.parse("""
                {
                    "_id": 1,
                    "name": "Eason",
                    "age": 25,
                    "address": {
                        "city": "New York",
                        "zip": 10001,
                        "details":{
                            "street": "123 Main St",
                            "room": "101"
                        }
                    },
                    "hobbies": ["reading", "traveling"]
                }
                """);

        {
            RawBsonDocumentProjector projector = new RawBsonDocumentProjector();
            RawBsonDocument actual = projector.project(rawDoc, Set.of("name", "age"));
            RawBsonDocument expected = RawBsonDocument.parse("""
                {
                    "name": "Eason",
                    "age": 25
                }
                """);

            System.out.println(actual.equals(expected));
            System.out.println(actual.toJson());

        }

        {
            RawBsonDocumentProjector projector = new RawBsonDocumentProjector();
            ByteBuffer projected = projector.project(rawDoc.getByteBuffer().asNIO(), Set.of("name", "age","address.city"));
            RawBsonDocument actual = new RawBsonDocument(projected.array());
            RawBsonDocument expected = RawBsonDocument.parse("""
                {
                    "name": "Eason",
                    "age": 25,
                    "address": {
                        "city": "New York"
                    }
                }
                """);

            System.out.println(actual.equals(expected));
            System.out.println(actual.toJson());
        }

        {
            RawBsonDocumentProjector projector = new RawBsonDocumentProjector();
            ByteBuffer projected = projector.project(rawDoc.getByteBuffer().asNIO(), Set.of("name", "age","address.city","hobbies"));
            RawBsonDocument actual = new RawBsonDocument(projected.array());
            RawBsonDocument expected = RawBsonDocument.parse("""
                {
                    "name": "Eason",
                    "age": 25,
                    "address": {
                        "city": "New York"
                    },
                    "hobbies": ["reading", "traveling"]
                }
                """);

            System.out.println(actual.equals(expected));
            System.out.println(actual.toJson());
        }

        {
            {
                RawBsonDocumentProjector projector = new RawBsonDocumentProjector();
                ByteBuffer projected = projector.project(rawDoc.getByteBuffer().asNIO(), Set.of("name", "age","address","hobbies"));
                RawBsonDocument actual = new RawBsonDocument(projected.array());
                RawBsonDocument expected = RawBsonDocument.parse("""
                {
                    "name": "Eason",
                    "age": 25,
                    "address": {
                        "city": "New York"
                    },
                    "hobbies": ["reading", "traveling"]
                }
                """);

                System.out.println(actual.equals(expected));
                System.out.println(actual.toJson());
            }
        }

        {
            {
                RawBsonDocumentProjector projector = new RawBsonDocumentProjector();
                ByteBuffer projected = projector.project(rawDoc.getByteBuffer().asNIO(), Set.of("name", "age","address.city","address.details.street","hobbies"));
                RawBsonDocument actual = new RawBsonDocument(projected.array());
                RawBsonDocument expected = RawBsonDocument.parse("""
                {
                    "name": "Eason",
                    "age": 25,
                    "address": {
                        "city": "New York",
                        "details":{
                            "street": "123 Main St"
                        }
                    },
                    "hobbies": ["reading", "traveling"]
                }
                """);

                System.out.println(actual.equals(expected));
                System.out.println(actual.toJson());
            }
        }

    }
}


