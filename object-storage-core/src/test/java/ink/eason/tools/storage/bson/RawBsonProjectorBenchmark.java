package ink.eason.tools.storage.bson; // 请替换为你的包名

import org.bson.BsonArray;
import org.bson.BsonBinary;
import org.bson.BsonBinaryReader;
import org.bson.BsonBinarySubType;
import org.bson.BsonBinaryWriter;
import org.bson.BsonBoolean;
import org.bson.BsonDateTime;
import org.bson.BsonDecimal128;
import org.bson.BsonDocument;
import org.bson.BsonDouble;
import org.bson.BsonInt32;
import org.bson.BsonInt64;
import org.bson.BsonJavaScript;
import org.bson.BsonJavaScriptWithScope;
import org.bson.BsonMaxKey;
import org.bson.BsonMinKey;
import org.bson.BsonNull;
import org.bson.BsonObjectId;
import org.bson.BsonRegularExpression;
import org.bson.BsonString;
import org.bson.BsonSymbol;
import org.bson.BsonTimestamp;
import org.bson.BsonUndefined;
import org.bson.ByteBuf;
import org.bson.ByteBufNIO;
import org.bson.RawBsonDocument;
import org.bson.codecs.BsonDocumentCodec;
import org.bson.codecs.DecoderContext;
import org.bson.codecs.EncoderContext;
import org.bson.io.BasicOutputBuffer;
import org.bson.io.ByteBufferBsonInput;
import org.bson.types.Decimal128;
import org.bson.types.ObjectId;
import org.openjdk.jmh.annotations.Benchmark;
import org.openjdk.jmh.annotations.BenchmarkMode;
import org.openjdk.jmh.annotations.Fork;
import org.openjdk.jmh.annotations.Level;
import org.openjdk.jmh.annotations.Measurement;
import org.openjdk.jmh.annotations.Mode;
import org.openjdk.jmh.annotations.OutputTimeUnit;
import org.openjdk.jmh.annotations.Scope;
import org.openjdk.jmh.annotations.Setup;
import org.openjdk.jmh.annotations.State;
import org.openjdk.jmh.annotations.Warmup;
import org.openjdk.jmh.infra.Blackhole;
import org.openjdk.jmh.runner.Runner;
import org.openjdk.jmh.runner.RunnerException;
import org.openjdk.jmh.runner.options.Options;
import org.openjdk.jmh.runner.options.OptionsBuilder;

import java.nio.ByteBuffer;
import java.nio.ByteOrder;
import java.util.Arrays;
import java.util.Date;
import java.util.HashSet;
import java.util.Set;
import java.util.UUID;
import java.util.concurrent.TimeUnit;
import java.util.regex.Pattern;

// 配置JMH参数
@BenchmarkMode(Mode.AverageTime)
@OutputTimeUnit(TimeUnit.NANOSECONDS)
@State(Scope.Benchmark)
@Warmup(iterations = 5, time = 1)
@Measurement(iterations = 5, time = 1)
@Fork(1)
public class RawBsonProjectorBenchmark {

    // ================== 测试数据和配置 ==================

    private RawBsonDocument rawDocument;
    private ByteBuffer inputByteBuffer;

    private Set<String> projectionFields;
    private BsonDocument filter;

    private RawBsonProjector projectorInstance;

    // 使用标准的Codec进行编解码
    private final BsonDocumentCodec documentCodec = new BsonDocumentCodec();

    @Setup
    public void setup() {
        // 1. 创建一份复杂的BSON文档
        BsonDocument complexDoc = createComplexBsonDocument();

        // 2. 将其序列化为字节 (使用Codec)
        BasicOutputBuffer outputBuffer = new BasicOutputBuffer();
        try (BsonBinaryWriter writer = new BsonBinaryWriter(outputBuffer)) {
            documentCodec.encode(writer, complexDoc, EncoderContext.builder().build());
        }
        byte[] bsonBytes = outputBuffer.toByteArray();
        this.rawDocument = new RawBsonDocument(bsonBytes);
        this.inputByteBuffer = ByteBuffer.wrap(bsonBytes).order(ByteOrder.LITTLE_ENDIAN);

        // 3. 定义投影字段 (Inclusive)
        this.projectionFields = new HashSet<>(Arrays.asList(
                "_id",
                "stringField",
                "nestedDoc.boolField",
                "arrayOfDocs.0.key",
                "deeplyNested.level1Array.0.level2Doc.level3Array",
                "timestampField"
        ));

        // 4. 定义一个能够匹配成功的过滤器
        this.filter = new BsonDocument("$and", new BsonArray(Arrays.asList(
                new BsonDocument("int64Field", new BsonDocument("$gt", new BsonInt64(1000L))),
                new BsonDocument("nestedDoc.stringField", new BsonDocument("$regex", new BsonString("^Nested")))
        )));

        // 5. 创建一个用于实例方法测试的Projector
        this.projectorInstance = new RawBsonProjector(
                this.projectionFields,
                RawBsonProjector.ProjectionMode.INCLUSIVE,
                false,
                null
        );
    }

    @Setup(Level.Invocation)
    public void resetBuffer() {
        inputByteBuffer.rewind();
    }


    // ================== 基准测试方法 ==================

    /**
     * 场景1: 基准测试 - 从ByteBuffer完整解析为BsonDocument (使用Codec)
     */
    @Benchmark
    public void baseline_fullDeserialization(Blackhole bh) {
        ByteBuf byteBuffer = new ByteBufNIO(rawDocument.getByteBuffer().asNIO().slice().order(ByteOrder.LITTLE_ENDIAN));
        try (BsonBinaryReader bsonReader = new BsonBinaryReader(new ByteBufferBsonInput(byteBuffer))) {
            BsonDocument raw = new BsonDocumentCodec().decode(bsonReader, DecoderContext.builder().build());
            BsonDocument doc = new BsonDocument();
            doc.append("_id", raw.get("_id"));
            doc.append("stringField", raw.get("stringField"));
            doc.append("nestedDoc.boolField", raw.getDocument("nestedDoc").get("boolField"));
            doc.append("arrayOfDocs.0.key", raw.getArray("arrayOfDocs").getFirst().asDocument().get("key"));
            doc.append("deeplyNested.level1Array.0.level2Doc.level3Array", raw.getDocument("deeplyNested").getArray("level1Array").getFirst().asDocument().getDocument("level2Doc").getArray("level3Array"));
            doc.append("timestampField", raw.get("timestampField"));
            bh.consume(doc);
        }
    }

    /**
     * 场景2: 仅投影 (Inclusive)，不进行过滤 (静态方法)
     */
    @Benchmark
    public void projectionOnly_static(Blackhole bh) {
        ByteBuffer result = RawBsonProjector.project(
                inputByteBuffer,
                false,
                projectionFields,
                RawBsonProjector.ProjectionMode.INCLUSIVE,
                null
        );
        bh.consume(result);
    }

    /**
     * 场景3: 投影 (Inclusive) + 过滤 (静态方法)
     */
    @Benchmark
    public void projectionAndFilter_static(Blackhole bh) {
        ByteBuffer result = RawBsonProjector.project(
                inputByteBuffer,
                false,
                projectionFields,
                RawBsonProjector.ProjectionMode.INCLUSIVE,
                filter
        );
        bh.consume(result);
    }

    /**
     * 场景4: 投影 (Inclusive) + 过滤 (实例方法)
     */
    @Benchmark
    public void projectionAndFilter_instance(Blackhole bh) {
        ByteBuffer result = projectorInstance.project(inputByteBuffer.slice().order(ByteOrder.LITTLE_ENDIAN));
        bh.consume(result);
    }


    // ================== 辅助方法：创建复杂BSON文档 ==================

    private static BsonDocument createComplexBsonDocument() {
        BsonDocument doc = new BsonDocument();

        // 常见数据类型
        doc.put("_id", new BsonObjectId(new ObjectId()));
        doc.put("stringField", new BsonString("Hello BSON World! This is a reasonably long string to test performance."));
        doc.put("int32Field", new BsonInt32(12345));
        doc.put("int64Field", new BsonInt64(Long.MAX_VALUE / 2));
        doc.put("doubleField", new BsonDouble(3.1415926535));
        doc.put("boolField", new BsonBoolean(true));
        doc.put("dateField", new BsonDateTime(new Date().getTime()));
        doc.put("nullField", new BsonNull());

        // 特殊数据类型
        // **修正**: 正确处理UUID到BsonBinary的转换
        UUID uuid = UUID.randomUUID();
        byte[] uuidBytes = new byte[16];
        ByteBuffer bb = ByteBuffer.wrap(uuidBytes);
        bb.putLong(uuid.getMostSignificantBits());
        bb.putLong(uuid.getLeastSignificantBits());
        doc.put("binaryField", new BsonBinary(BsonBinarySubType.UUID_STANDARD, uuidBytes));

        doc.put("undefinedField", new BsonUndefined());
        doc.put("timestampField", new BsonTimestamp((int) (System.currentTimeMillis() / 1000), 1));

        // **修正**: 正确处理Pattern到BsonRegularExpression的转换
        Pattern pattern = Pattern.compile("^test.*", Pattern.CASE_INSENSITIVE);
        doc.put("regexField", new BsonRegularExpression(pattern.pattern(), flagsToString(pattern.flags())));

        doc.put("decimal128Field", new BsonDecimal128(Decimal128.parse("12345.67890")));
        doc.put("minKeyField", new BsonMinKey());
        doc.put("maxKeyField", new BsonMaxKey());

        // 代码类型
        doc.put("javascriptField", new BsonJavaScript("function() { return 1; }"));
        doc.put("javascriptWithScopeField", new BsonJavaScriptWithScope("function(x) { return x; }", new BsonDocument("x", new BsonInt32(10))));
        doc.put("symbolField", new BsonSymbol("mySymbol"));

        // 嵌套结构
        BsonDocument nestedDoc = new BsonDocument("stringField", new BsonString("Nested String"))
                .append("intField", new BsonInt32(99))
                .append("boolField", new BsonBoolean(false));
        doc.put("nestedDoc", nestedDoc);

        BsonArray simpleArray = new BsonArray();
        for (int i = 0; i < 50; i++) simpleArray.add(new BsonInt32(i));
        doc.put("simpleArray", simpleArray);

        BsonArray arrayOfDocs = new BsonArray();
        for (int i = 0; i < 20; i++) {
            arrayOfDocs.add(new BsonDocument("key", new BsonString("value_" + i)).append("index", new BsonInt32(i)));
        }
        doc.put("arrayOfDocs", arrayOfDocs);

        BsonDocument deeplyNested = new BsonDocument();
        BsonArray level1Array = new BsonArray();
        for (int i = 0; i < 5; i++) {
            BsonArray level3Array = new BsonArray();
            for (int j = 0; j < 10; j++) level3Array.add(new BsonString("item_" + i + "_" + j));
            BsonDocument level2Doc = new BsonDocument("level3Array", level3Array).append("id", new BsonObjectId());
            level1Array.add(new BsonDocument("level2Doc", level2Doc));
        }
        deeplyNested.put("level1Array", level1Array);
        doc.put("deeplyNested", deeplyNested);

        return doc;
    }

    private static String flagsToString(int flags) {
        StringBuilder sb = new StringBuilder();
        if ((flags & Pattern.CASE_INSENSITIVE) != 0) sb.append('i');
        if ((flags & Pattern.MULTILINE) != 0) sb.append('m');
        if ((flags & Pattern.DOTALL) != 0) sb.append('s');
        if ((flags & Pattern.UNICODE_CASE) != 0) sb.append('u');
        if ((flags & Pattern.COMMENTS) != 0) sb.append('x');
        // 其他BSON支持的flag可以继续添加
        return sb.toString();
    }

    // 主方法，用于直接运行此测试类
    public static void main(String[] args) throws RunnerException {
        Options opt = new OptionsBuilder()
                .include(RawBsonProjectorBenchmark.class.getSimpleName())
                .addProfiler("gc")
                .build();
        new Runner(opt).run();
    }
}
