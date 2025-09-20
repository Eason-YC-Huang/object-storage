package ink.eason.tools.storage.bson;


import org.bson.ByteBuf;
import org.bson.ByteBufNIO;
import org.bson.io.OutputBuffer;
import org.bson.types.ObjectId;

import java.io.IOException;
import java.io.OutputStream;
import java.nio.Buffer;
import java.nio.ByteBuffer;
import java.util.Arrays;
import java.util.Collections;
import java.util.List;

import static java.lang.String.format;
import static java.nio.ByteOrder.LITTLE_ENDIAN;

/**
 * A BSON output stream that stores the output in a single, un-pooled byte array.
 */
public class BsonOutputByteBuffer extends OutputBuffer {

    /**
     * This ByteBuffer allows us to write ObjectIDs without allocating a temporary array per object, and enables us
     * to leverage JVM intrinsics for writing little-endian numeric values.
     */
    private ByteBuffer buffer;

    /**
     * Construct an instance with a default initial byte array size.
     */
    public BsonOutputByteBuffer() {
        this(1024);
    }

    /**
     * Construct an instance with the specified initial byte array size.
     *
     * @param initialSize the initial size of the byte array
     */
    public BsonOutputByteBuffer(final int initialSize) {
        // Allocate heap buffer to ensure we can access underlying array
        this.buffer = ByteBuffer.allocate(initialSize).order(LITTLE_ENDIAN);
    }

    public BsonOutputByteBuffer(final ByteBuffer buffer) {
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

        throw new IllegalStateException("The output is closed");
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
