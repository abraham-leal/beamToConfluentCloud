

import io.confluent.kafka.schemaregistry.client.CachedSchemaRegistryClient;
import io.confluent.kafka.schemaregistry.client.SchemaMetadata;
import io.confluent.kafka.schemaregistry.client.SchemaRegistryClient;
import io.confluent.kafka.schemaregistry.client.rest.exceptions.RestClientException;
import io.confluent.kafka.serializers.KafkaAvroSerializer;
import org.apache.avro.Schema;
import org.apache.beam.sdk.annotations.Experimental;
import org.apache.beam.sdk.annotations.Experimental.Kind;
import org.apache.beam.sdk.coders.AvroCoder;
import org.apache.beam.sdk.coders.Coder;
import org.apache.beam.sdk.coders.CoderRegistry;
import org.apache.beam.sdk.transforms.SerializableFunction;
import org.apache.beam.vendor.guava.v26_0_jre.com.google.common.annotations.VisibleForTesting;
import org.apache.beam.vendor.guava.v26_0_jre.com.google.common.collect.ImmutableMap;
import org.apache.kafka.common.serialization.Serializer;

import javax.annotation.Nullable;
import java.io.IOException;
import java.util.Map;

import static org.apache.beam.vendor.guava.v26_0_jre.com.google.common.base.Preconditions.checkArgument;

@Experimental(Kind.SOURCE_SINK)
public class ConfluentSchemaRegistrySerializerProvider<T> implements SerializerProvider<T> {
    private final SerializableFunction<Void, SchemaRegistryClient> schemaRegistryClientProviderFn;
    private final String schemaRegistryUrl;
    private final String subject;
    private final @Nullable Integer version;

    @VisibleForTesting
    ConfluentSchemaRegistrySerializerProvider(
            SerializableFunction<Void, SchemaRegistryClient> schemaRegistryClientProviderFn,
            String schemaRegistryUrl,
            String subject,
            @Nullable Integer version) {
        checkArgument(
                schemaRegistryClientProviderFn != null,
                "You should provide a schemaRegistryClientProviderFn.");
        checkArgument(schemaRegistryUrl != null, "You should provide a schemaRegistryUrl.");
        checkArgument(subject != null, "You should provide a subject to fetch the schema from.");
        this.schemaRegistryClientProviderFn = schemaRegistryClientProviderFn;
        this.schemaRegistryUrl = schemaRegistryUrl;
        this.subject = subject;
        this.version = version;
    }

    public static <T> ConfluentSchemaRegistrySerializerProvider<T> of(
            String schemaRegistryUrl, String subject) {
        return of(schemaRegistryUrl, subject, null);
    }

    public static <T> ConfluentSchemaRegistrySerializerProvider<T> of(
            String schemaRegistryUrl, String subject, @Nullable Integer version) {
        return new ConfluentSchemaRegistrySerializerProvider(
                (SerializableFunction<Void, SchemaRegistryClient>)
                        input -> new CachedSchemaRegistryClient(schemaRegistryUrl, Integer.MAX_VALUE),
                schemaRegistryUrl,
                subject,
                version);
    }

    @Override
    public Serializer<T> getSerializer(Map<String, ?> configs, boolean isKey) {
        ImmutableMap<String, Object> csrConfig =
                ImmutableMap.<String, Object>builder()
                        .putAll(configs)
                        .build();
        Serializer<T> serializer = (Serializer<T>) new KafkaAvroSerializer(getSchemaRegistryClient());
        serializer.configure(csrConfig, isKey);
        return serializer;
    }

    @Override
    public Coder<T> getCoder(CoderRegistry coderRegistry) {
        final Schema avroSchema = new Schema.Parser().parse(getSchemaMetadata().getSchema());
        return (Coder<T>) AvroCoder.of(avroSchema);
    }

    private SchemaMetadata getSchemaMetadata() {
        try {
            return (version == null)
                    ? getSchemaRegistryClient().getLatestSchemaMetadata(subject)
                    : getSchemaRegistryClient().getSchemaMetadata(subject, version);
        } catch (IOException | RestClientException e) {
            throw new RuntimeException("Unable to get latest schema metadata for subject: " + subject, e);
        }
    }

    private SchemaRegistryClient getSchemaRegistryClient() {
        return this.schemaRegistryClientProviderFn.apply(null);
    }
}
