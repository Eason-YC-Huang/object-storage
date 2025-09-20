package ink.eason.tools.storage.bson;

import ink.eason.tools.storage.bson.MyBsonBinaryWriter.Mark;
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
             MyBsonBinaryWriter writer = new MyBsonBinaryWriter(new BsonOutputByteBuffer(bsonOutputByteBuffer))) {
            pipeDocument(bsonInputByteBuffer, bsonOutputByteBuffer, reader, writer, projection, ROOT_PATH);
        }

        bsonOutputByteBuffer.limit(bsonOutputByteBuffer.position());
        bsonOutputByteBuffer.position(0);
        return bsonOutputByteBuffer;
    }

    private boolean pipeDocument(
            ByteBuffer bsonInputByteBuffer, ByteBuffer bsonOutputByteBuffer,
            BsonBinaryReader reader, MyBsonBinaryWriter writer,
                              Set<String> projKeys, String currentPath) {
        reader.readStartDocument();
        writer.writeStartDocument();

        boolean hasValueWritten = false;

        while (reader.readBsonType() != BsonType.END_OF_DOCUMENT) {
            String fieldName = reader.readName();
            BsonType bsonType = reader.getCurrentBsonType();

            String fullPath = currentPath.isEmpty() ? fieldName : currentPath + "." + fieldName;
            boolean isExactMatch = projKeys.contains(fullPath);
            if (isExactMatch) {
                if ((bsonType == BsonType.DOCUMENT)) {
                    writer.writeName(fieldName);
                    writer.pipe(reader);
                    hasValueWritten = true;
                }
                else if(bsonType == BsonType.ARRAY) {
                    writer.writeName(fieldName);
                    pipeArray(bsonInputByteBuffer, bsonOutputByteBuffer, reader, writer, projKeys, fullPath);
                    hasValueWritten = true;
                }
                else {
                    writer.writeName(fieldName);
                    pipeValue(bsonInputByteBuffer, bsonOutputByteBuffer, reader, writer, projKeys, fullPath);
                    hasValueWritten = true;
                }
            }
            else if (bsonType == BsonType.DOCUMENT && projKeys.stream().anyMatch(key -> key.contains(fullPath + "."))) {
                Mark mark = writer.getMark();
                writer.writeName(fieldName);
                if (pipeValue(bsonInputByteBuffer, bsonOutputByteBuffer, reader, writer, projKeys, fullPath)) {
                    hasValueWritten = true;
                } else {
                    writer.resetMark(mark);
                }

                //int pos = bsonInputByteBuffer.position();
                //int docLen = bsonInputByteBuffer.getInt();
                //bsonInputByteBuffer.position(pos);
                //
                //// None Exact Matched Document
                //BsonOutputByteBuffer tempBuffer = new BsonOutputByteBuffer(docLen);
                //BsonBinaryWriter tempWriter = new BsonBinaryWriter(tempBuffer);
                //
                //tempWriter.writeStartDocument();
                //tempWriter.writeName("v");
                //pipeValue(bsonInputByteBuffer,bsonOutputByteBuffer, reader, tempWriter, projKeys, fullPath);
                //tempWriter.writeEndDocument();
                //
                //if (tempBuffer.getPosition() > 13) {
                //    writer.writeName(fieldName);
                //    ByteBuffer internalBuffer = tempBuffer.getInternalBuffer();
                //    internalBuffer.limit(internalBuffer.position());
                //    internalBuffer.position(0);
                //    BsonBinaryReader tempReader = new BsonBinaryReader(internalBuffer);
                //    tempReader.readStartDocument();
                //    tempReader.readName("v");
                //    writer.pipe(tempReader);
                //    tempReader.close();
                //}

            }
            else if (bsonType == BsonType.ARRAY && projKeys.stream().anyMatch(key -> key.contains(fullPath + "."))) {
                Mark mark = writer.getMark();
                writer.writeName(fieldName);
                if (pipeValue(bsonInputByteBuffer, bsonOutputByteBuffer, reader, writer, projKeys, fullPath)) {
                    hasValueWritten = true;
                } else {
                    writer.resetMark(mark);
                }
            }
            else {
                reader.skipValue();
            }

        }

        reader.readEndDocument();
        writer.writeEndDocument();

        return hasValueWritten;
    }

    private boolean pipeArray(ByteBuffer bsonInputByteBuffer, ByteBuffer bsonOutputByteBuffer,
                           BsonBinaryReader reader, MyBsonBinaryWriter writer,
                           Set<String> projKeys, String currentPath) {
        boolean hasValueWritten = false;
        reader.readStartArray();
        writer.writeStartArray();
        int idx = 0;
        while (reader.readBsonType() != BsonType.END_OF_DOCUMENT) {
            Mark mark = writer.getMark();
            if (pipeValue(bsonInputByteBuffer, bsonOutputByteBuffer, reader, writer, projKeys, currentPath + "." + idx)) {
                hasValueWritten = true;
            } else {
                writer.resetMark(mark);
            }
            idx++;
        }
        reader.readEndArray();
        writer.writeEndArray();
        return hasValueWritten;
    }

    private boolean pipeValue(ByteBuffer bsonInputByteBuffer, ByteBuffer bsonOutputByteBuffer,
                           BsonBinaryReader reader, MyBsonBinaryWriter writer,
                           Set<String> projKeys, String currentPath) {

        BsonType bsonType = reader.getCurrentBsonType();
        switch (bsonType) {
            case DOCUMENT:
                return pipeDocument(bsonInputByteBuffer, bsonOutputByteBuffer, reader, writer, projKeys, currentPath);
            case ARRAY:
                return pipeArray(bsonInputByteBuffer, bsonOutputByteBuffer, reader, writer, projKeys, currentPath);
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
        return true;
    }

    public static void main(String[] args) {
        RawBsonDocument rawDoc = RawBsonDocument.parse("""
                {
                  "user": {
                    "name": "Alice",
                    "age": 30
                  },
                  "address": {
                    "city": "New York",
                    "zip": 10001
                  },
                  "hobbies": ["reading", "traveling"],
                  "workExperience": [
                    {
                      "company": "ABC Corp",
                      "position": "Developer",
                      "years": 5
                    },
                    {
                      "company": "XYZ Corp",
                      "position": "Manager",
                      "years": 3
                    }
                  ]
                }
                """);

        RawBsonDocumentProjector projector = new RawBsonDocumentProjector();
        RawBsonDocument output = projector.project(rawDoc, Set.of("user.notExists", "address.city", "hobbies", "workExperience.0.notExists"));
        System.out.println(output.toJson());

    }
}


