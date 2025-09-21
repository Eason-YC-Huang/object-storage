package ink.eason.tools.storage.bson;

import ink.eason.tools.storage.bson.RawBsonDocumentProjector.InternalBsonBinaryWriter.Mark;
import org.bson.AbstractBsonWriter;
import org.bson.BsonArray;
import org.bson.BsonBinaryReader;
import org.bson.BsonBinaryWriter;
import org.bson.BsonBinaryWriterSettings;
import org.bson.BsonBoolean;
import org.bson.BsonDateTime;
import org.bson.BsonDecimal128;
import org.bson.BsonDocument;
import org.bson.BsonDouble;
import org.bson.BsonInt32;
import org.bson.BsonInt64;
import org.bson.BsonInvalidOperationException;
import org.bson.BsonJavaScript;
import org.bson.BsonJavaScriptWithScope;
import org.bson.BsonMaxKey;
import org.bson.BsonMinKey;
import org.bson.BsonNull;
import org.bson.BsonObjectId;
import org.bson.BsonReaderMark;
import org.bson.BsonString;
import org.bson.BsonSymbol;
import org.bson.BsonType;
import org.bson.BsonUndefined;
import org.bson.BsonValue;
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
import java.util.HashMap;
import java.util.HashSet;
import java.util.List;
import java.util.Map;
import java.util.Set;
import java.util.regex.Pattern;
import java.util.stream.Collectors;

import static java.lang.String.format;
import static java.nio.ByteOrder.LITTLE_ENDIAN;

public class RawBsonDocumentProjector {

    public static enum ProjectionMode {
        INCLUSIVE,
        EXCLUSIVE,
    }

    private static final String ROOT_PATH = "";

    private final ProjectionMode mode;

    private final Set<String> fields;

    private final Set<String> filterKeys;

    private final boolean inPlaceModify;

    private final BsonDocument filters;

    public RawBsonDocumentProjector(Set<String> fields, ProjectionMode mode, boolean inPlaceModify, BsonDocument filters) {
        if ((fields == null || fields.isEmpty()) && (filters == null || filters.isEmpty()) ) {
            throw new IllegalArgumentException("fields and filters can't be empty at the same time");
        }

        this.fields = fields != null ? normalizeProjection(fields) : Collections.emptySet();
        this.mode = mode;
        this.inPlaceModify = inPlaceModify;
        this.filterKeys = filters != null ? normalizeFilterKey(filters) : Collections.emptySet();
        this.filters = filters != null ? filters.clone() : null;
    }

    // =============== public instance methods ===============

    public RawBsonDocument project(RawBsonDocument input){
        ByteBuffer output = project(input.getByteBuffer().asNIO());
        if (output == null) return null;
        return new RawBsonDocument(output.array(), 0, output.remaining());
    }

    public ByteBuffer project(ByteBuffer bsonInputByteBuffer){
        return project(bsonInputByteBuffer, inPlaceModify, fields, mode, filters, filterKeys, new HashMap<>());
    }

    // =============== public static methods ===============

    public static RawBsonDocument project(RawBsonDocument input, Set<String> fields){
        return project(input, false, fields, ProjectionMode.INCLUSIVE, null);
    }

    public static ByteBuffer project(ByteBuffer bsonInputByteBuffer, Set<String> fields){
        return project(bsonInputByteBuffer, false, fields, ProjectionMode.INCLUSIVE, null);
    }

    public static RawBsonDocument project(RawBsonDocument input, Set<String> fields, ProjectionMode mode){
        return project(input, false, fields, mode, null);
    }

    public static ByteBuffer project(ByteBuffer bsonInputByteBuffer, Set<String> fields, ProjectionMode mode){
        return project(bsonInputByteBuffer, false, fields, mode, null);
    }

    public static RawBsonDocument project(RawBsonDocument input, Set<String> fields, ProjectionMode mode, BsonDocument filters){
        return project(input, false, fields, mode, filters);
    }

    public static ByteBuffer project(ByteBuffer bsonInputByteBuffer, Set<String> fields, ProjectionMode mode, BsonDocument filters){
        return project(bsonInputByteBuffer, false, fields, mode, filters);
    }

    public static RawBsonDocument project(RawBsonDocument input, boolean inPlaceModify, Set<String> fields, ProjectionMode mode, BsonDocument filters) {
        ByteBuffer output = project(input.getByteBuffer().asNIO(), inPlaceModify, fields, mode, filters);
        if (output == null) return null;
        return new RawBsonDocument(output.array(), 0, output.remaining());
    }

    public static ByteBuffer project(ByteBuffer bsonInputByteBuffer, boolean inPlaceModify, Set<String> fields, ProjectionMode mode, BsonDocument filters) {

        if ((fields == null || fields.isEmpty()) && (filters == null || filters.isEmpty()) ) {
            throw new IllegalArgumentException("fields and filters can't be empty at the same time");
        }

        fields = fields != null ? normalizeProjection(fields) : Collections.emptySet();
        Set<String> filterKeys = filters != null ? normalizeFilterKey(filters) : Collections.emptySet();
        Map<String, BsonValue> valuesForFilter = filters != null ? new HashMap<>(filterKeys.size()) : Collections.emptyMap();

        return project(bsonInputByteBuffer, inPlaceModify, fields, mode, filters, filterKeys, valuesForFilter);

    }

    // ================ core  ================

    private static ByteBuffer project(ByteBuffer bsonInputByteBuffer, boolean inPlaceModify, Set<String> fields, ProjectionMode mode, BsonDocument filters, Set<String> filterKeys, Map<String,BsonValue> valuesForFilter) {

        ByteBuffer bsonOutputByteBuffer = inPlaceModify
                ? bsonInputByteBuffer.slice().order(LITTLE_ENDIAN)
                : ByteBuffer.allocate(bsonInputByteBuffer.remaining()).order(LITTLE_ENDIAN);

        try (BsonBinaryReader reader = new BsonBinaryReader(bsonInputByteBuffer);
             InternalBsonBinaryWriter writer = new InternalBsonBinaryWriter(new InternalOutputByteBuffer(bsonOutputByteBuffer))) {
            pipeDocument(reader, writer, fields, filterKeys, valuesForFilter, ROOT_PATH, false, mode);
        }

        if (filters != null) {
            boolean matches = BsonDocumentFilter.matches(valuesForFilter, filters);
            if (!matches) {
                return null;
            }
        }

        bsonOutputByteBuffer.limit(bsonOutputByteBuffer.position());
        bsonOutputByteBuffer.position(0);
        return bsonOutputByteBuffer;

    }

    private static Set<String> normalizeProjection(Set<String> projection) {
        return projection.stream().map(key -> key.replaceAll("\\[(\\d+)]", ".$1")).collect(Collectors.toSet());
    }

    private static Set<String> normalizeFilterKey(BsonDocument filter) {
        Set<String> keys = new HashSet<>();

        for (String key : filter.keySet()) {
            BsonValue filterValue = filter.get(key);
            switch (key) {
                case "$and", "$or":
                    for (BsonValue condition : filterValue.asArray()) {
                        keys.addAll(normalizeFilterKey(condition.asDocument()));
                    }
                    break;
                default:
                    keys.add(key);
            }
        }

        return keys.stream().map(key -> key.replaceAll("\\[(\\d+)]", ".$1")).collect(Collectors.toCollection(HashSet::new));
    }

    private static boolean pipeDocument(BsonBinaryReader reader, InternalBsonBinaryWriter writer,
                                 Set<String> projKeys, Set<String> filterKeys, Map<String, BsonValue> valuesForFilter, String currentPath, boolean exactMatched, ProjectionMode mode) {

        reader.readStartDocument();
        writer.writeStartDocument();

        boolean hasValueWritten = false;

        while (reader.readBsonType() != BsonType.END_OF_DOCUMENT) {

            String fieldName = reader.readName();
            String fullPath = currentPath.isEmpty() ? fieldName : currentPath + "." + fieldName;

            if (filterKeys.contains(fullPath)) {
                BsonReaderMark mark = reader.getMark();
                valuesForFilter.put(fullPath, readValue(reader));
                mark.reset();
            }

            BsonType bsonType = reader.getCurrentBsonType();
            boolean isExactMatch = projKeys.contains(fullPath);
            boolean mayHaveProjectKey = false;
            boolean mayHaveFilterKey = false;
            if (!isExactMatch && !exactMatched) {
                // only under (!isExactMatch && !writePermitted) case will need to use these boolean fields
                mayHaveProjectKey = projKeys.stream().anyMatch(key -> key.contains(fullPath + "."));
                mayHaveFilterKey = filterKeys.stream().anyMatch(key -> key.contains(fullPath + "."));
            }

            if (mode == ProjectionMode.INCLUSIVE) {
                if (isExactMatch||exactMatched) {
                    writer.writeName(fieldName);
                    pipeValue(reader, writer, projKeys, filterKeys, valuesForFilter, fullPath, true, mode);
                    hasValueWritten = true;
                }
                else if ((bsonType == BsonType.DOCUMENT || bsonType == BsonType.ARRAY) && (mayHaveProjectKey || mayHaveFilterKey)) {
                    Mark mark = writer.getMark();
                    writer.writeName(fieldName);
                    if (pipeValue(reader, writer, projKeys, filterKeys, valuesForFilter, fullPath, false, mode)) {
                        hasValueWritten = true;
                    } else {
                        writer.resetMark(mark);
                    }
                }
                else {
                    reader.skipValue();
                }
            }
            else if (mode == ProjectionMode.EXCLUSIVE) {
                if (isExactMatch||exactMatched) {
                    // 这个value可以完全不要，因为它的子元素都不需要被采集
                    if (filterKeys.stream().noneMatch(key -> key.contains(fullPath + "."))) {
                        reader.skipValue();
                    }
                    else {
                        Mark mark = writer.getMark();
                        writer.writeName(fieldName);
                        pipeValue(reader, writer, projKeys, filterKeys, valuesForFilter, fullPath, true, mode);
                        writer.resetMark(mark);
                    }
                }
                else if ((bsonType == BsonType.DOCUMENT || bsonType == BsonType.ARRAY) && (mayHaveProjectKey || mayHaveFilterKey)) {
                    // 它包含的子元素可能需要被丢弃， 或者它的子元素需要被采集做filter
                    Mark mark = writer.getMark();
                    writer.writeName(fieldName);
                    if (pipeValue(reader, writer, projKeys, filterKeys, valuesForFilter, fullPath, false, mode)) {
                        hasValueWritten = true;
                    } else {
                        writer.resetMark(mark);
                    }
                }
                else {
                    writer.writeName(fieldName);
                    pipeValue(reader, writer, projKeys, filterKeys, valuesForFilter, fullPath,false, mode);
                    hasValueWritten = true;
                }
            }

        }

        reader.readEndDocument();
        writer.writeEndDocument();

        return hasValueWritten;
    }

    private static boolean pipeArray(BsonBinaryReader reader, InternalBsonBinaryWriter writer,
                              Set<String> projKeys, Set<String> filterKeys, Map<String,BsonValue> valuesForFilter, String currentPath, boolean exactMatched, ProjectionMode mode) {

        reader.readStartArray();
        writer.writeStartArray();
        boolean hasValueWritten = false;

        int idx = 0;
        while (reader.readBsonType() != BsonType.END_OF_DOCUMENT) {

            BsonType bsonType = reader.getCurrentBsonType();
            String fullPath = currentPath + "." + idx;
            if (filterKeys.contains(fullPath)) {
                BsonReaderMark mark = reader.getMark();
                valuesForFilter.put(fullPath, readValue(reader));
                mark.reset();
            }

            boolean isExactMatch = projKeys.contains(fullPath);
            boolean mayHaveProjectKey = false;
            boolean mayHaveFilterKey = false;
            if (!isExactMatch && !exactMatched) {
                // only under (!isExactMatch && !writePermitted) case will need to use these boolean fields
                mayHaveProjectKey = projKeys.stream().anyMatch(key -> key.contains(fullPath + "."));
                mayHaveFilterKey = filterKeys.stream().anyMatch(key -> key.contains(fullPath + "."));
            }

            if (mode == ProjectionMode.INCLUSIVE) {
                if (isExactMatch||exactMatched) {
                    pipeValue(reader, writer, projKeys, filterKeys, valuesForFilter, fullPath, true, mode);
                    hasValueWritten = true;
                }
                else if ((bsonType == BsonType.DOCUMENT || bsonType == BsonType.ARRAY) && (mayHaveProjectKey || mayHaveFilterKey)) {
                    Mark mark = writer.getMark();
                    if (pipeValue(reader, writer, projKeys, filterKeys, valuesForFilter, fullPath, false, mode)) {
                        hasValueWritten = true;
                    } else {
                        writer.resetMark(mark);
                    }
                }
                else {
                    reader.skipValue();
                }
            }
            else if (mode == ProjectionMode.EXCLUSIVE) {
                if (isExactMatch||exactMatched) {
                    if (filterKeys.stream().noneMatch(key -> key.contains(fullPath + "."))) {
                        reader.skipValue();
                    } else {
                        Mark mark = writer.getMark();
                        pipeValue(reader, writer, projKeys, filterKeys, valuesForFilter, fullPath, true, mode);
                        writer.resetMark(mark);
                    }
                }
                else if ((bsonType == BsonType.DOCUMENT || bsonType == BsonType.ARRAY) && (mayHaveProjectKey || mayHaveFilterKey)) {
                    Mark mark = writer.getMark();
                    if (pipeValue(reader, writer, projKeys, filterKeys, valuesForFilter, fullPath, false, mode)) {
                        hasValueWritten = true;
                    } else {
                        writer.resetMark(mark);
                    }
                }
                else {
                    pipeValue(reader, writer, projKeys, filterKeys, valuesForFilter, fullPath, false, mode);
                    hasValueWritten = true;
                }
            }

            idx++;
        }
        reader.readEndArray();
        writer.writeEndArray();
        return hasValueWritten;
    }

    private static boolean pipeValue(BsonBinaryReader reader, InternalBsonBinaryWriter writer,
                              Set<String> projKeys, Set<String> filterKeys, Map<String, BsonValue> valuesForFilter, String currentPath, boolean exactMatched, ProjectionMode mode) {

        BsonType bsonType = reader.getCurrentBsonType();
        switch (bsonType) {
            case DOCUMENT:
                return pipeDocument(reader, writer, projKeys, filterKeys, valuesForFilter, currentPath, exactMatched, mode);
            case ARRAY:
                return pipeArray(reader, writer, projKeys, filterKeys, valuesForFilter, currentPath, exactMatched, mode);
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
                pipeDocument(reader, writer, projKeys, filterKeys, valuesForFilter, currentPath, exactMatched, mode);
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

    private static BsonDocument readDocument(BsonBinaryReader reader) {
        BsonDocument document = new BsonDocument();
        reader.readStartDocument();
        while (reader.readBsonType() != BsonType.END_OF_DOCUMENT) {
            document.put(reader.readName(), readValue(reader));
        }
        reader.readEndDocument();
        return document;
    }

    private static BsonArray readArray(BsonBinaryReader reader) {
        BsonArray array = new BsonArray();
        reader.readStartArray();
        while (reader.readBsonType() != BsonType.END_OF_DOCUMENT) {
            array.add(readValue(reader));
        }
        reader.readEndArray();
        return array;
    }

    private static BsonValue readValue(BsonBinaryReader reader) {
        BsonType bsonType = reader.getCurrentBsonType();
        return switch (bsonType) {
            case DOCUMENT -> readDocument(reader);
            case ARRAY -> readArray(reader);
            case DOUBLE -> new BsonDouble(reader.readDouble());
            case STRING -> new BsonString(reader.readString());
            case BINARY -> reader.readBinaryData();
            case UNDEFINED -> {
                reader.readUndefined();
                yield new BsonUndefined();
            }
            case OBJECT_ID -> new BsonObjectId(reader.readObjectId());
            case BOOLEAN -> new BsonBoolean(reader.readBoolean());
            case DATE_TIME -> new BsonDateTime(reader.readDateTime());
            case NULL -> {
                reader.readNull();
                yield new BsonNull();
            }
            case REGULAR_EXPRESSION -> reader.readRegularExpression();
            case JAVASCRIPT -> new BsonJavaScript(reader.readJavaScript());
            case SYMBOL -> new BsonSymbol(reader.readSymbol());
            case JAVASCRIPT_WITH_SCOPE -> new BsonJavaScriptWithScope(reader.readJavaScriptWithScope(), readDocument(reader));
            case INT32 -> new BsonInt32(reader.readInt32());
            case TIMESTAMP -> reader.readTimestamp();
            case INT64 -> new BsonInt64(reader.readInt64());
            case DECIMAL128 -> new BsonDecimal128(reader.readDecimal128());
            case MIN_KEY -> {
                reader.readMinKey();
                yield new BsonMinKey();
            }
            case DB_POINTER -> reader.readDBPointer();
            case MAX_KEY -> {
                reader.readMaxKey();
                yield new BsonMaxKey();
            }
            default -> throw new BsonInvalidOperationException("Unsupported BSON type: " + bsonType);
        };
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

        private final InternalOutputByteBuffer bsonOutput;

        public InternalBsonBinaryWriter(InternalOutputByteBuffer bsonOutput, FieldNameValidator validator) {
            super(bsonOutput, validator);
            this.bsonOutput = bsonOutput;
        }

        public InternalBsonBinaryWriter(InternalOutputByteBuffer bsonOutput) {
            super(bsonOutput);
            this.bsonOutput = bsonOutput;
        }

        public InternalBsonBinaryWriter(BsonWriterSettings settings, BsonBinaryWriterSettings binaryWriterSettings, InternalOutputByteBuffer bsonOutput) {
            super(settings, binaryWriterSettings, bsonOutput);
            this.bsonOutput = bsonOutput;
        }

        public InternalBsonBinaryWriter(BsonWriterSettings settings, BsonBinaryWriterSettings binaryWriterSettings, InternalOutputByteBuffer bsonOutput, FieldNameValidator validator) {
            super(settings, binaryWriterSettings, bsonOutput, validator);
            this.bsonOutput = bsonOutput;
        }

        public InternalOutputByteBuffer getInternalOutputByteBuffer() {
            return bsonOutput;
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

    public static class BsonDocumentFilter {

        /**
         * 检查单个 doc 是否满足过滤条件
         *
         * @param doc    单个 Bson 文档
         * @param filter 过滤条件
         * @return 如果匹配则返回 true，否则返回 false
         */
        public static boolean matches(Map<String,BsonValue> doc, BsonDocument filter) {
            for (String key : filter.keySet()) {
                BsonValue filterValue = filter.get(key);
                switch (key) {
                    case "$and":
                        if (!handleAnd(doc, filterValue.asArray())) {
                            return false;
                        }
                        break;
                    case "$or":
                        if (!handleOr(doc, filterValue.asArray())) {
                            return false;
                        }
                        break;
                    default:
                        BsonValue docValue = doc.get(key);
                        if (!fieldMatches(docValue, filterValue)) {
                            return false;
                        }
                }
            }
            return true;
        }

        /**
         * 处理 $and 操作符
         */
        private static boolean handleAnd(Map<String,BsonValue> doc, BsonArray conditions) {
            for (BsonValue condition : conditions) {
                if (!matches(doc, condition.asDocument())) {
                    return false;
                }
            }
            return true;
        }

        /**
         * 处理 $or 操作符
         */
        private static boolean handleOr(Map<String,BsonValue> doc, BsonArray conditions) {
            for (BsonValue condition : conditions) {
                if (matches(doc, condition.asDocument())) {
                    return true;
                }
            }
            return false;
        }

        /**
         * 检查字段是否匹配过滤条件
         *
         * @param docValue    文档中的字段值
         * @param filterValue 过滤条件的值
         * @return 如果匹配则返回 true
         */
        private static boolean fieldMatches(BsonValue docValue, BsonValue filterValue) {
            if (filterValue.isDocument()) {
                BsonDocument conditionDoc = filterValue.asDocument();
                for (String operator : conditionDoc.keySet()) {
                    BsonValue operatorValue = conditionDoc.get(operator);
                    if (!applyOperator(docValue, operator, operatorValue)) {
                        return false;
                    }
                }
                return true;
            } else {
                // 隐式的 $eq 操作
                return compare(docValue, filterValue) == 0;
            }
        }

        /**
         * 应用具体的操作符
         */
        private static boolean applyOperator(BsonValue docValue, String operator, BsonValue operatorValue) {
            // 如果文档中字段不存在 (docValue == null)，大部分操作符都应该返回 false
            if (docValue == null) {
                return operator.equals("$ne") || operator.equals("$nin");
            }

            switch (operator) {
                case "$eq":
                    return compare(docValue, operatorValue) == 0;
                case "$ne":
                    return compare(docValue, operatorValue) != 0;
                case "$gt":
                    return compare(docValue, operatorValue) > 0;
                case "$gte":
                    return compare(docValue, operatorValue) >= 0;
                case "$lt":
                    return compare(docValue, operatorValue) < 0;
                case "$lte":
                    return compare(docValue, operatorValue) <= 0;
                case "$in":
                    return operatorValue.asArray().stream().anyMatch(val -> compare(docValue, val) == 0);
                case "$nin":
                    return operatorValue.asArray().stream().noneMatch(val -> compare(docValue, val) == 0);
                case "$regex":
                    return handleRegex(docValue, operatorValue);
                default:
                    // 不支持的操作符，可以抛出异常或返回 false
                    return false;
            }
        }

        /**
         * 处理正则表达式匹配
         */
        private static boolean handleRegex(BsonValue docValue, BsonValue regexValue) {
            if (!docValue.isString()) {
                return false;
            }
            String docString = docValue.asString().getValue();
            Pattern pattern = Pattern.compile(regexValue.asString().getValue());
            return pattern.matcher(docString).find();
        }

        /**
         * 通用的比较方法
         *
         * @return 0 for equal, > 0 for docValue > filterValue, < 0 for docValue < filterValue
         */
        @SuppressWarnings({"rawtypes", "unchecked"})
        private static int compare(BsonValue docValue, BsonValue filterValue) {
            if (docValue == null && filterValue == null) return 0;
            if (docValue == null) return -1;
            if (filterValue == null) return 1;
            if (docValue.getBsonType() != filterValue.getBsonType()) {
                // 对于数字类型，可以进行转换后比较
                if (docValue.isNumber() && filterValue.isNumber()) {
                    Number num1 = getNumberFromBsonValue(docValue);
                    Number num2 = getNumberFromBsonValue(filterValue);
                    return Double.compare(num1.doubleValue(), num2.doubleValue());
                }
                // 类型不同且无法比较，则认为不相等
                return -1;
            }

            // 类型相同
            switch (docValue.getBsonType()) {
                case INT32:
                    return Integer.compare(docValue.asInt32().getValue(), filterValue.asInt32().getValue());
                case INT64:
                    return Long.compare(docValue.asInt64().getValue(), filterValue.asInt64().getValue());
                case DOUBLE:
                    return Double.compare(docValue.asDouble().getValue(), filterValue.asDouble().getValue());
                case STRING:
                    return docValue.asString().getValue().compareTo(filterValue.asString().getValue());
                case DATE_TIME:
                    return Long.compare(docValue.asDateTime().getValue(), filterValue.asDateTime().getValue());
                case BOOLEAN:
                    return Boolean.compare(docValue.asBoolean().getValue(), filterValue.asBoolean().getValue());
                case OBJECT_ID:
                    return docValue.asObjectId().getValue().compareTo(filterValue.asObjectId().getValue());
                // 对于其他类型，我们只支持判等
                default:
                    return docValue.equals(filterValue) ? 0 : -1;
            }
        }

        private static Number getNumberFromBsonValue(BsonValue bsonValue) {
            if (bsonValue.isInt32()) {
                return bsonValue.asInt32().getValue();
            } else if (bsonValue.isInt64()) {
                return bsonValue.asInt64().getValue();
            } else if (bsonValue.isDouble()) {
                return bsonValue.asDouble().getValue();
            } else if (bsonValue.isDecimal128()) {
                return bsonValue.asDecimal128().getValue().bigDecimalValue();
            }
            throw new IllegalArgumentException("Unsupported number type: " + bsonValue.getBsonType());
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

}


