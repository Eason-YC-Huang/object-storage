package ink.eason.tools.storage.bson;

import org.bson.AbstractBsonWriter;
import org.bson.BsonBinaryWriter;
import org.bson.BsonBinaryWriterSettings;
import org.bson.BsonWriterSettings;
import org.bson.FieldNameValidator;
import org.bson.io.BsonOutput;

public class MyBsonBinaryWriter extends BsonBinaryWriter {

    private BsonOutput bsonOutput;

    public MyBsonBinaryWriter(BsonOutput bsonOutput, FieldNameValidator validator) {
        super(bsonOutput, validator);
        this.bsonOutput = bsonOutput;
    }

    public MyBsonBinaryWriter(BsonOutput bsonOutput) {
        super(bsonOutput);
        this.bsonOutput = bsonOutput;
    }

    public MyBsonBinaryWriter(BsonWriterSettings settings, BsonBinaryWriterSettings binaryWriterSettings, BsonOutput bsonOutput) {
        super(settings, binaryWriterSettings, bsonOutput);
        this.bsonOutput = bsonOutput;
    }

    public MyBsonBinaryWriter(BsonWriterSettings settings, BsonBinaryWriterSettings binaryWriterSettings, BsonOutput bsonOutput, FieldNameValidator validator) {
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
        private final int position;

        /**
         * Creates a new instance storing the current position of the {@link org.bson.io.BsonOutput}.
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
