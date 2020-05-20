import org.apache.beam.sdk.annotations.Experimental;
import org.apache.beam.sdk.coders.Coder;
import org.apache.beam.sdk.coders.CoderRegistry;
import org.apache.kafka.common.serialization.Serializer;

import java.io.Serializable;
import java.util.Map;

@Experimental(Experimental.Kind.SOURCE_SINK)
interface SerializerProvider<T> extends Serializable {
    Serializer<T> getSerializer(Map<String, ?> configs, boolean isKey);

    Coder<T> getCoder(CoderRegistry coderRegistry);
}
