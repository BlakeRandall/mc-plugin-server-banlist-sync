package dev.example.bansync;

import java.util.Date;

import org.bson.BsonDocument;
import org.bson.BsonDocumentReader;
import org.bson.BsonDocumentWriter;
import org.bson.codecs.DecoderContext;
import org.bson.codecs.EncoderContext;
import static org.bson.codecs.configuration.CodecRegistries.fromProviders;
import static org.bson.codecs.configuration.CodecRegistries.fromRegistries;
import org.bson.codecs.configuration.CodecRegistry;
import org.bson.codecs.pojo.PojoCodecProvider;
import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertFalse;
import static org.junit.jupiter.api.Assertions.assertNull;
import org.junit.jupiter.api.Test;

import com.mongodb.MongoClientSettings;

class SharedBanEntryTest {

    private final CodecRegistry codecRegistry = fromRegistries(
            MongoClientSettings.getDefaultCodecRegistry(),
            fromProviders(
                    PojoCodecProvider.builder()
                            .automatic(true)
                            .build()
            )
    );

    @Test
    void defaultsToInactiveWithNoOptionalValues() {
        SharedBanEntry entry = new SharedBanEntry();

        assertFalse(entry.isActive());
        assertEquals(0L, entry.getRevision());
        assertNull(entry.getExpiresAt());
        assertNull(entry.getUpdatedAt());
    }

    @Test
    void mongodbPojoCodecPreservesAllBanFields() {
        Date expiresAt = new Date(1_800_000_000_000L);
        Date updatedAt = new Date(1_700_000_000_000L);
        SharedBanEntry original = new SharedBanEntry();
        original.setKey("PLAYER:123e4567-e89b-12d3-a456-426614174000");
        original.setType("PLAYER");
        original.setTarget("ExamplePlayer");
        original.setUuid("123e4567-e89b-12d3-a456-426614174000");
        original.setReason("Rule violation");
        original.setSource("Moderator");
        original.setExpiresAt(expiresAt);
        original.setActive(true);
        original.setChangedBy("server-1");
        original.setChangeSource("local-change");
        original.setUpdatedAt(updatedAt);
        original.setRevision(7L);

        BsonDocument document = new BsonDocument();
        codecRegistry.get(SharedBanEntry.class).encode(
                new BsonDocumentWriter(document),
                original,
                EncoderContext.builder().build()
        );

        SharedBanEntry decoded = codecRegistry.get(SharedBanEntry.class).decode(
                new BsonDocumentReader(document),
                DecoderContext.builder().build()
        );

        assertEquals(original.getKey(), decoded.getKey());
        assertEquals(original.getType(), decoded.getType());
        assertEquals(original.getTarget(), decoded.getTarget());
        assertEquals(original.getUuid(), decoded.getUuid());
        assertEquals(original.getReason(), decoded.getReason());
        assertEquals(original.getSource(), decoded.getSource());
        assertEquals(expiresAt, decoded.getExpiresAt());
        assertEquals(original.isActive(), decoded.isActive());
        assertEquals(original.getChangedBy(), decoded.getChangedBy());
        assertEquals(original.getChangeSource(), decoded.getChangeSource());
        assertEquals(updatedAt, decoded.getUpdatedAt());
        assertEquals(original.getRevision(), decoded.getRevision());
    }
}
