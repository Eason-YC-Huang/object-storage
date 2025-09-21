package ink.eason.tools.storage.bson;

import ink.eason.tools.storage.bson.RawBsonDocumentProjector.InternalBsonBinaryWriter.Mark;
import org.bson.AbstractBsonWriter;
import org.bson.BsonBinaryReader;
import org.bson.BsonBinaryWriter;
import org.bson.BsonBinaryWriterSettings;
import org.bson.BsonInvalidOperationException;
import org.bson.BsonType;
import org.bson.BsonWriterSettings;
import org.bson.ByteBuf;
import org.bson.ByteBufNIO;
import org.bson.FieldNameValidator;
import org.bson.RawBsonDocument;
import org.bson.io.BsonOutput;
import org.bson.io.OutputBuffer;
import org.bson.types.ObjectId;

import java.io.IOException;
import java.io.OutputStream;
import java.nio.Buffer;
import java.nio.ByteBuffer;
import java.util.Arrays;
import java.util.Collections;
import java.util.HashSet;
import java.util.List;
import java.util.Objects;
import java.util.Set;
import java.util.stream.Collectors;

import static java.lang.String.format;
import static java.nio.ByteOrder.LITTLE_ENDIAN;

public class RawBsonDocumentProjector {

    public static enum ProjectionMode {
        INCLUSIVE,
        EXCLUSIVE,
    }

    private static final String ROOT_PATH = "";

    private final boolean modifyInPlace;

    public RawBsonDocumentProjector() {
        this.modifyInPlace = false;
    }

    public RawBsonDocumentProjector(boolean modifyInPlace) {
        this.modifyInPlace = modifyInPlace;
    }

    public RawBsonDocument project(RawBsonDocument input, Set<String> projection){
        return project(input, projection, ProjectionMode.INCLUSIVE);
    }

    public ByteBuffer project(ByteBuffer bsonInputByteBuffer, Set<String> projection){
        return project(bsonInputByteBuffer, projection, ProjectionMode.INCLUSIVE);
    }

    public RawBsonDocument project(RawBsonDocument input, Set<String> projection, ProjectionMode mode) {
        Objects.requireNonNull(input);
        ByteBuffer output = this.project(input.getByteBuffer().asNIO(), projection, mode);
        return new RawBsonDocument(output.array(), 0, output.remaining());
    }

    public ByteBuffer project(ByteBuffer bsonInputByteBuffer, Set<String> projection, ProjectionMode mode) {
        if (projection == null || projection.isEmpty()) {
            if (mode == ProjectionMode.EXCLUSIVE) {
                if (modifyInPlace) {
                    return bsonInputByteBuffer;
                } else {
                    return cloneByteBuffer(bsonInputByteBuffer);
                }
            } else {
                throw new IllegalArgumentException("projection is null or empty");
            }
        }

        projection = normalizeProjection(projection);

        ByteBuffer bsonOutputByteBuffer = modifyInPlace
                ? bsonInputByteBuffer.slice().order(LITTLE_ENDIAN)
                : ByteBuffer.allocate(bsonInputByteBuffer.remaining()).order(LITTLE_ENDIAN);

        try (BsonBinaryReader reader = new BsonBinaryReader(bsonInputByteBuffer);
             InternalBsonBinaryWriter writer = new InternalBsonBinaryWriter(new InternalOutputByteBuffer(bsonOutputByteBuffer))) {
            pipeDocument(reader, writer, projection, ROOT_PATH, false, mode);
        }

        bsonOutputByteBuffer.limit(bsonOutputByteBuffer.position());
        bsonOutputByteBuffer.position(0);
        return bsonOutputByteBuffer;
    }

    private Set<String> normalizeProjection(Set<String> projection) {
        return projection.stream().map(key -> key.replaceAll("\\[(\\d+)]", ".$1")).collect(Collectors.toCollection(HashSet::new));
    }

    private boolean pipeDocument(BsonBinaryReader reader, InternalBsonBinaryWriter writer,
                              Set<String> projKeys, String currentPath, boolean writePermitted, ProjectionMode mode) {

        if (projKeys.isEmpty()) {
            return true;
        }

        reader.readStartDocument();
        writer.writeStartDocument();

        boolean hasValueWritten = false;

        while (reader.readBsonType() != BsonType.END_OF_DOCUMENT) {
            String fieldName = reader.readName();
            BsonType bsonType = reader.getCurrentBsonType();

            String fullPath = currentPath.isEmpty() ? fieldName : currentPath + "." + fieldName;
            boolean isExactMatch = projKeys.contains(fullPath);

            if (mode == ProjectionMode.INCLUSIVE) {
                if (isExactMatch||writePermitted) {
                    if ((bsonType == BsonType.DOCUMENT)) {
                        writer.writeName(fieldName);
                        writer.pipe(reader);
                        hasValueWritten = true;
                    }
                    else if(bsonType == BsonType.ARRAY) {
                        writer.writeName(fieldName);
                        pipeArray(reader, writer, projKeys, fullPath, true,mode);
                        hasValueWritten = true;
                    }
                    else {
                        writer.writeName(fieldName);
                        pipeValue(reader, writer, projKeys, fullPath,true,mode);
                        hasValueWritten = true;
                    }
                    if (isExactMatch) {
                        projKeys.remove(fullPath);
                    }
                }
                else if (bsonType == BsonType.DOCUMENT && projKeys.stream().anyMatch(key -> key.contains(fullPath + "."))) {
                    Mark mark = writePermitted ? null : writer.getMark();
                    writer.writeName(fieldName);
                    if (pipeValue(reader, writer, projKeys, fullPath, writePermitted,mode)) {
                        hasValueWritten = true;
                    } else {
                        if (mark != null) {
                            writer.resetMark(mark);
                        }
                    }
                }
                else if (bsonType == BsonType.ARRAY && projKeys.stream().anyMatch(key -> key.contains(fullPath + "."))) {
                    Mark mark = writePermitted ? null : writer.getMark();
                    writer.writeName(fieldName);
                    if (pipeValue(reader, writer, projKeys, fullPath, writePermitted,mode)) {
                        hasValueWritten = true;
                    } else {
                        if (mark != null) {
                            writer.resetMark(mark);
                        }
                    }
                }
                else {
                    reader.skipValue();
                }
            }
            else if (mode == ProjectionMode.EXCLUSIVE) {
                if (isExactMatch) {
                    reader.skipValue();
                    projKeys.remove(fullPath);
                }
                else if (bsonType == BsonType.DOCUMENT && projKeys.stream().anyMatch(key -> key.contains(fullPath + "."))) {
                    Mark mark = writePermitted ? null : writer.getMark();
                    writer.writeName(fieldName);
                    if (pipeValue(reader, writer, projKeys, fullPath, writePermitted,mode)) {
                        hasValueWritten = true;
                    } else {
                        if (mark != null) {
                            writer.resetMark(mark);
                        }
                    }
                }
                else if (bsonType == BsonType.ARRAY && projKeys.stream().anyMatch(key -> key.contains(fullPath + "."))) {
                    Mark mark = writePermitted ? null : writer.getMark();
                    writer.writeName(fieldName);
                    if (pipeValue(reader, writer, projKeys, fullPath, writePermitted,mode)) {
                        hasValueWritten = true;
                    } else {
                        if (mark != null) {
                            writer.resetMark(mark);
                        }
                    }
                }
                else {
                    if ((bsonType == BsonType.DOCUMENT)) {
                        writer.writeName(fieldName);
                        writer.pipe(reader);
                        hasValueWritten = true;
                    }
                    else if(bsonType == BsonType.ARRAY) {
                        writer.writeName(fieldName);
                        pipeArray(reader, writer, projKeys, fullPath, true, mode);
                        hasValueWritten = true;
                    }
                    else {
                        writer.writeName(fieldName);
                        pipeValue(reader, writer, projKeys, fullPath,true, mode);
                        hasValueWritten = true;
                    }
                }
            }



        }

        reader.readEndDocument();
        writer.writeEndDocument();

        return hasValueWritten;
    }

    private boolean pipeArray(BsonBinaryReader reader, InternalBsonBinaryWriter writer,
                              Set<String> projKeys, String currentPath, boolean writePermitted, ProjectionMode mode) {

        reader.readStartArray();
        writer.writeStartArray();
        boolean hasValueWritten = false;

        int idx = 0;
        while (reader.readBsonType() != BsonType.END_OF_DOCUMENT) {

            BsonType bsonType = reader.getCurrentBsonType();
            String fullPath = currentPath + "." + idx;
            boolean isExactMatch = projKeys.contains(fullPath);

            if (mode == ProjectionMode.INCLUSIVE) {
                if (isExactMatch||writePermitted) {
                    if ((bsonType == BsonType.DOCUMENT)) {
                        writer.pipe(reader);
                        hasValueWritten = true;
                    } else if (bsonType == BsonType.ARRAY) {
                        pipeArray(reader, writer, projKeys, fullPath, true, mode);
                        hasValueWritten = true;
                    } else {
                        pipeValue(reader, writer, projKeys, fullPath, true, mode);
                        hasValueWritten = true;
                    }
                    if (isExactMatch) {
                        projKeys.remove(fullPath);
                    }
                }
                else if (bsonType == BsonType.DOCUMENT && projKeys.stream().anyMatch(key -> key.contains(fullPath + "."))) {
                    Mark mark = writePermitted ? null : writer.getMark();
                    if (pipeValue(reader, writer, projKeys, fullPath, writePermitted, mode)) {
                        hasValueWritten = true;
                    } else {
                        if (mark != null) {
                            writer.resetMark(mark);
                        }
                    }
                }
                else if (bsonType == BsonType.ARRAY && projKeys.stream().anyMatch(key -> key.contains(fullPath + "."))) {
                    Mark mark = writePermitted ? null : writer.getMark();
                    if (pipeValue(reader, writer, projKeys, fullPath, writePermitted, mode)) {
                        hasValueWritten = true;
                    } else {
                        if (mark != null) {
                            writer.resetMark(mark);
                        }
                    }
                }
                else {
                    reader.skipValue();
                }
            }
            else if (mode == ProjectionMode.EXCLUSIVE) {
                if (isExactMatch) {
                    reader.skipValue();
                    projKeys.remove(fullPath);
                }
                else if (bsonType == BsonType.DOCUMENT && projKeys.stream().anyMatch(key -> key.contains(fullPath + "."))) {
                    Mark mark = writePermitted ? null : writer.getMark();
                    if (pipeValue(reader, writer, projKeys, fullPath, writePermitted, mode)) {
                        hasValueWritten = true;
                    } else {
                        if (mark != null) {
                            writer.resetMark(mark);
                        }
                    }
                }
                else if (bsonType == BsonType.ARRAY && projKeys.stream().anyMatch(key -> key.contains(fullPath + "."))) {
                    Mark mark = writePermitted ? null : writer.getMark();
                    if (pipeValue(reader, writer, projKeys, fullPath, writePermitted, mode)) {
                        hasValueWritten = true;
                    } else {
                        if (mark != null) {
                            writer.resetMark(mark);
                        }
                    }
                }
                else {
                    if ((bsonType == BsonType.DOCUMENT)) {
                        writer.pipe(reader);
                        hasValueWritten = true;
                    }
                    else if (bsonType == BsonType.ARRAY) {
                        pipeArray(reader, writer, projKeys, fullPath, true, mode);
                        hasValueWritten = true;
                    }
                    else {
                        pipeValue(reader, writer, projKeys, fullPath, true, mode);
                        hasValueWritten = true;
                    }
                }
            }

            idx++;
        }
        reader.readEndArray();
        writer.writeEndArray();
        return hasValueWritten;
    }

    private boolean pipeValue(BsonBinaryReader reader, InternalBsonBinaryWriter writer,
                           Set<String> projKeys, String currentPath, boolean writePermitted,ProjectionMode mode) {

        BsonType bsonType = reader.getCurrentBsonType();
        switch (bsonType) {
            case DOCUMENT:
                return pipeDocument(reader, writer, projKeys, currentPath, writePermitted,mode);
            case ARRAY:
                return pipeArray(reader, writer, projKeys, currentPath, writePermitted,mode);
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
                writer.writeJavaScript(reader.readJavaScript());
            case SYMBOL:
                writer.writeSymbol(reader.readSymbol());
                break;
            case JAVASCRIPT_WITH_SCOPE:
                writer.writeJavaScriptWithScope(reader.readJavaScriptWithScope());
                pipeDocument(reader, writer, projKeys, currentPath, writePermitted,mode);
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

    // ================ internal helper class ================

    /**
     * A BSON output stream that stores the output in a single, un-pooled byte array.
     */
    public static class InternalOutputByteBuffer extends OutputBuffer {

        /**
         * This ByteBuffer allows us to write ObjectIDs without allocating a temporary array per object, and enables us
         * to leverage JVM intrinsics for writing little-endian numeric values.
         */
        private ByteBuffer buffer;

        /**
         * Construct an instance with the specified initial byte array size.
         *
         * @param size the initial size of the byte array
         */
        public InternalOutputByteBuffer(final int size) {
            // Allocate heap buffer to ensure we can access underlying array
            this.buffer = ByteBuffer.allocate(size).order(LITTLE_ENDIAN);
        }

        public InternalOutputByteBuffer(final ByteBuffer buffer) {
            this.buffer = buffer.order(LITTLE_ENDIAN);
        }

        public ByteBuffer getInternalBuffer() {
            return buffer;
        }

        @Override
        public void write(final byte[] b) {
            writeBytes(b, 0, b.length);
        }

        @Override
        public byte[] toByteArray() {
            ensureOpen();
            return Arrays.copyOf(buffer.array(), buffer.position());
        }

        @Override
        public void writeInt32(final int value) {
            ensureOpen();
            ensure(4);
            buffer.putInt(value);
        }

        @Override
        public void writeInt32(final int position, final int value) {
            ensureOpen();
            checkPosition(position, 4);
            buffer.putInt(position, value);
        }

        @Override
        public void writeInt64(final long value) {
            ensureOpen();
            ensure(8);
            buffer.putLong(value);
        }

        @Override
        public void writeObjectId(final ObjectId value) {
            ensureOpen();
            ensure(12);
            value.putToByteBuffer(buffer);
        }

        @Override
        public void writeBytes(final byte[] bytes, final int offset, final int length) {
            ensureOpen();

            ensure(length);
            buffer.put(bytes, offset, length);
        }

        @Override
        public void writeByte(final int value) {
            ensureOpen();

            ensure(1);
            buffer.put((byte) (0xFF & value));
        }

        @Override
        protected void write(final int absolutePosition, final int value) {
            ensureOpen();
            checkPosition(absolutePosition, 1);

            buffer.put(absolutePosition, (byte) (0xFF & value));
        }

        @Override
        public int getPosition() {
            ensureOpen();
            return buffer.position();
        }

        /**
         * @return size of data so far
         */
        @Override
        public int getSize() {
            ensureOpen();
            return buffer.position();
        }

        @Override
        public int pipe(final OutputStream out) throws IOException {
            ensureOpen();
            out.write(buffer.array(), 0, buffer.position());
            return buffer.position();
        }

        @Override
        public void truncateToPosition(final int newPosition) {
            ensureOpen();
            if (newPosition > buffer.position() || newPosition < 0) {
                throw new IllegalArgumentException();
            }
            // The cast is required for compatibility with JDK 9+ where ByteBuffer's position method is inherited from Buffer.
            ((Buffer) buffer).position(newPosition);
        }

        @Override
        public List<ByteBuf> getByteBuffers() {
            ensureOpen();
            // Create a flipped copy of the buffer for reading. Note that ByteBufNIO overwrites the endian-ness.
            ByteBuffer flipped = ByteBuffer.wrap(buffer.array(), 0, buffer.position());
            return Collections.singletonList(new ByteBufNIO(flipped));
        }

        @Override
        public void close() {
            buffer = null;
        }

        private void ensureOpen() {
            if (buffer == null) {
                throw new IllegalStateException("The output is closed");
            }
        }

        private void ensure(final int more) {
            int length = buffer.position();
            int need = length + more;
            if (need <= buffer.capacity()) {
                return;
            }

            throw new IllegalStateException("buffer overflow");
        }

        /**
         * Ensures that `absolutePosition` is a valid index in `this.buffer` and there is room to write at
         * least `bytesToWrite` bytes.
         */
        private void checkPosition(final int absolutePosition, final int bytesToWrite) {
            if (absolutePosition < 0) {
                throw new IllegalArgumentException(format("position must be >= 0 but was %d", absolutePosition));
            }
            if (absolutePosition > buffer.position() - bytesToWrite) {
                throw new IllegalArgumentException(format("position must be <= %d but was %d", buffer.position() - bytesToWrite, absolutePosition));
            }
        }
    }

    public static class InternalBsonBinaryWriter extends BsonBinaryWriter {

        private final BsonOutput bsonOutput;

        public InternalBsonBinaryWriter(BsonOutput bsonOutput, FieldNameValidator validator) {
            super(bsonOutput, validator);
            this.bsonOutput = bsonOutput;
        }

        public InternalBsonBinaryWriter(BsonOutput bsonOutput) {
            super(bsonOutput);
            this.bsonOutput = bsonOutput;
        }

        public InternalBsonBinaryWriter(BsonWriterSettings settings, BsonBinaryWriterSettings binaryWriterSettings, BsonOutput bsonOutput) {
            super(settings, binaryWriterSettings, bsonOutput);
            this.bsonOutput = bsonOutput;
        }

        public InternalBsonBinaryWriter(BsonWriterSettings settings, BsonBinaryWriterSettings binaryWriterSettings, BsonOutput bsonOutput, FieldNameValidator validator) {
            super(settings, binaryWriterSettings, bsonOutput, validator);
            this.bsonOutput = bsonOutput;
        }

        public Mark getMark() {
            return new Mark();
        }

        public void resetMark(Mark mark) {
            mark.reset();
        }

        public class Mark extends AbstractBsonWriter.Mark {
            public final int position;

            /**
             * Creates a new instance storing the current position of the {@link BsonOutput}.
             */
            protected Mark() {
                this.position = bsonOutput.getPosition();
            }

            @Override
            protected void reset() {
                super.reset();
                bsonOutput.truncateToPosition(position);
            }
        }

    }

    private static ByteBuffer cloneByteBuffer(ByteBuffer bsonInputByteBuffer) {
        ByteBuffer clone = ByteBuffer.allocate(bsonInputByteBuffer.capacity());

        // Save the original position and limit.
        int originalPosition = bsonInputByteBuffer.position();
        int originalLimit = bsonInputByteBuffer.limit();

        // Put the original's content into the new buffer.
        // The original's position and limit remain unchanged.
        clone.put(bsonInputByteBuffer.asReadOnlyBuffer());

        // Restore the original position and limit for the clone.
        clone.position(originalPosition);
        clone.limit(originalLimit);

        return clone;
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
                      "years": 5,
                      "skills": ["Java", "Python"]
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
        long s = System.currentTimeMillis();

        for (int i = 0; i < 1_000_000; i++) {
            RawBsonDocument output = projector.project(rawDoc, Set.of("user.notExists", "address.city", "hobbies", "workExperience[0].company", "workExperience[1].company","workExperience[0].skills"));
        }
        System.out.println("cost time: " + (System.currentTimeMillis() - s));
        RawBsonDocument output = projector.project(rawDoc, Set.of("user.notExists", "address.city", "hobbies", "workExperience[0].company", "workExperience[1].company","workExperience[0].skills"));
        System.out.println(output.toJson());

    }
}


