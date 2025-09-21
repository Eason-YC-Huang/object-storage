package ink.eason.tools.storage.bson;

import ink.eason.tools.storage.bson.RawBsonDocumentProjector.ProjectionMode;
import org.bson.RawBsonDocument;
import org.openjdk.jmh.annotations.*;
import org.openjdk.jmh.infra.Blackhole;
import org.openjdk.jmh.runner.Runner;
import org.openjdk.jmh.runner.RunnerException;
import org.openjdk.jmh.runner.options.Options;
import org.openjdk.jmh.runner.options.OptionsBuilder;

import java.io.IOException;
import java.io.InputStream;
import java.nio.ByteBuffer;
import java.util.Set;
import java.util.concurrent.TimeUnit;

@BenchmarkMode(Mode.Throughput)
@OutputTimeUnit(TimeUnit.SECONDS)
@State(Scope.Thread)
@Warmup(iterations = 5, time = 2, timeUnit = TimeUnit.SECONDS)
@Measurement(iterations = 5, time = 2, timeUnit = TimeUnit.SECONDS)
@Fork(value = 1, jvmArgs = {"-Xms2G", "-Xmx2G"})
public class ProjectorBenchmark2 {

    // 状态类 1: 作为“黄金”数据源，在整个Benchmark生命周期内只加载一次，且不可变。
    @State(Scope.Benchmark)
    public static class DataSource {
        byte[] goldenBytes; // 存储原始的、不可变的字节数组
        Set<String> highSelectivityProjection;

        @Setup(Level.Trial)
        public void setup() throws IOException {
             this.goldenBytes = RawBsonDocument.parse("""
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
            """).getByteBuffer().array();

            this.highSelectivityProjection = Set.of("_id", "version", "price", "binaryData", "creationDate", "lastModified", "regex", "jsCodeWithScope", "symbol");
        }
    }

    // 状态类 2: 为每一次`@Benchmark`调用准备可变的执行数据
    @State(Scope.Thread)
    public static class ExecutionPlan {
        private ByteBuffer workingBuffer; // 每次调用前都会被重置为一个全新的副本

        // 关键所在：此方法在每次调用@Benchmark方法前执行
        // JMH会保证其执行时间不计入最终测量结果
        @Setup(Level.Invocation)
        public void setup(DataSource ds) {
            // 从黄金数据源克隆一份全新的字节数组
            byte[] freshBytes = ds.goldenBytes.clone();
            // 包装成ByteBuffer，作为本次测试的输入
            this.workingBuffer = ByteBuffer.wrap(freshBytes);
        }
    }


    // --- Projector 实例 ---
    private final RawBsonDocumentProjector inPlaceProjector = new RawBsonDocumentProjector(ProjectionMode.IN_PLACE);
    private final RawBsonDocumentProjector newBufferProjector = new RawBsonDocumentProjector(ProjectionMode.NEW_BUFFER);


    @Benchmark
    public void highSelectivity_InPlace(ExecutionPlan plan, DataSource ds, Blackhole blackhole) {
        // plan.workingBuffer 是一个全新的、干净的数据副本
        ByteBuffer result = inPlaceProjector.project(plan.workingBuffer, ds.highSelectivityProjection);
        blackhole.consume(result);
    }

    @Benchmark
    public void highSelectivity_NewBuffer(ExecutionPlan plan, DataSource ds, Blackhole blackhole) {
        // plan.workingBuffer 同样是一个全新的副本，保证了对比的公平性
        ByteBuffer result = newBufferProjector.project(plan.workingBuffer, ds.highSelectivityProjection);
        blackhole.consume(result);
    }

    public static void main(String[] args) throws RunnerException {
        Options opt = new OptionsBuilder()
                .include(ProjectorBenchmark2.class.getSimpleName())
                .build();

        new Runner(opt).run();
    }
}
