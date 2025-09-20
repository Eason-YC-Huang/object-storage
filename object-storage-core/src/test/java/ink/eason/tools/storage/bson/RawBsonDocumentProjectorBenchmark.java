package ink.eason.tools.storage.bson;

import org.bson.RawBsonDocument;
import org.openjdk.jmh.annotations.*;
import org.openjdk.jmh.infra.Blackhole;
import org.openjdk.jmh.runner.Runner;
import org.openjdk.jmh.runner.RunnerException;
import org.openjdk.jmh.runner.options.Options;
import org.openjdk.jmh.runner.options.OptionsBuilder;

import java.util.Set;
import java.util.concurrent.TimeUnit;

// Sets the default benchmark mode to Throughput (operations per second)
@BenchmarkMode(Mode.Throughput)
// Sets the default output time unit
@OutputTimeUnit(TimeUnit.SECONDS)
// Sets the default warm-up iterations
@Warmup(iterations = 5, time = 1)
// Sets the default measurement iterations
@Measurement(iterations = 5, time = 1)
// Runs each benchmark in a separate forked JVM process for isolation
@Fork(1)
// All threads in a benchmark run share the same state instance
@State(Scope.Benchmark)
public class RawBsonDocumentProjectorBenchmark {

    // @State defines the data to be used in the benchmark.
    // JMH ensures that the setup of this state is not part of the measured time.
    private RawBsonDocumentProjector projector;
    private RawBsonDocument sourceDoc;
    private Set<String> projectionSet;

    @Setup
    public void setup() {
        // Initialize the projector
        projector = new RawBsonDocumentProjector();

        // Use the same complex document from your main method as the test data
        sourceDoc = RawBsonDocument.parse("""
                {
                  "user": { "name": "Alice", "age": 30 },
                  "address": { "city": "New York", "zip": 10001 },
                  "hobbies": ["reading", "traveling"],
                  "workExperience": [
                    { "company": "ABC Corp", "position": "Developer", "years": 5, "skills": ["Java", "Python"] },
                    { "company": "XYZ Corp", "position": "Manager", "years": 3 }
                  ]
                }
                """);

        // The set of fields to project
        projectionSet = Set.of(
                "user.notExists",
                "address.city",
                "hobbies",
                "workExperience[0].company",
                "workExperience[1].company",
                "workExperience[0].skills"
        );
    }

    // @Benchmark marks this as the method to be measured.
    @Benchmark
    public void testProjection(Blackhole blackhole) {
        // The operation we want to measure
        RawBsonDocument output = projector.project(sourceDoc, projectionSet);

        // A Blackhole is used to consume the result of the benchmark.
        // This prevents the JIT compiler from performing Dead Code Elimination,
        // which would happen if the result 'output' was not used.
        blackhole.consume(output);
    }

    // A main method to conveniently run the benchmark from your IDE
    public static void main(String[] args) throws RunnerException {
        Options opt = new OptionsBuilder()
                .include(RawBsonDocumentProjectorBenchmark.class.getSimpleName())
                .build();

        new Runner(opt).run();
    }
}
