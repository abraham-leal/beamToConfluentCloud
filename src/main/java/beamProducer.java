import examples.customerSubscription;
import io.confluent.kafka.serializers.AbstractKafkaAvroSerDeConfig;
import org.apache.beam.sdk.Pipeline;
import org.apache.beam.sdk.io.kafka.KafkaIO;
import org.apache.beam.sdk.options.PipelineOptions;
import org.apache.beam.sdk.options.PipelineOptionsFactory;
import org.apache.beam.sdk.transforms.Create;
import org.apache.beam.sdk.values.PCollection;
import org.apache.kafka.common.serialization.Serializer;

import java.util.ArrayList;
import java.util.HashMap;
import java.util.List;
import java.util.Map;

public class beamProducer {

    public static void main(String[] args) {
        PipelineOptions options = PipelineOptionsFactory.create();
        Pipeline p = Pipeline.create(options);

        ConfluentSchemaRegistrySerializerProvider<customerSubscription> sampleSerializer =
                ConfluentSchemaRegistrySerializerProvider.of("<CCLOUD_SR_URL>","mySubject");

        // sample data
        List<customerSubscription> kvs = new ArrayList<>();
        kvs.add(customerSubscription.newBuilder().setCountry("2").setCustomer("2").setSubscription(2).build());
        kvs.add(customerSubscription.newBuilder().setCountry("3").setCustomer("3").setSubscription(3).build());
        kvs.add(customerSubscription.newBuilder().setCountry("4").setCustomer("4").setSubscription(4).build());
        kvs.add(customerSubscription.newBuilder().setCountry("5").setCustomer("5").setSubscription(5).build());
        kvs.add(customerSubscription.newBuilder().setCountry("6").setCustomer("6").setSubscription(6).build());


        PCollection<customerSubscription> input = p
                .apply(Create.of(kvs));

        input.apply(KafkaIO.<Void, customerSubscription>write()
                .withBootstrapServers("pkc-4yyd6.us-east1.gcp.confluent.cloud:9092")
                .withTopic("beamTopic")
                .withProducerConfigUpdates(getSRConfigs())
                .withValueSerializer((Class<? extends Serializer<customerSubscription>>) sampleSerializer.getSerializer(getSRConfigs(),false).getClass())
                .values());

        p.run().waitUntilFinish();
    }

    public static Map<String,Object> getSRConfigs (){
        HashMap<String, Object> SRconfig = new HashMap<>();
        SRconfig.put(AbstractKafkaAvroSerDeConfig.SCHEMA_REGISTRY_URL_CONFIG,"<CCLOUD_SR_URL>");
        SRconfig.put(AbstractKafkaAvroSerDeConfig.USER_INFO_CONFIG,"<CCLOUD_SR_API_KEY>:<CCLOUD_SR_API_SECRET>");
        SRconfig.put(AbstractKafkaAvroSerDeConfig.BASIC_AUTH_CREDENTIALS_SOURCE,"USER_INFO");
        SRconfig.put("security.protocol","SASL_SSL");
        SRconfig.put("sasl.mechanism","PLAIN");
        SRconfig.put("ssl.endpoint.identification.algorithm","https");
        SRconfig.put("sasl.jaas.config","org.apache.kafka.common.security.plain.PlainLoginModule " +
                "required username=\"<CCLOUD_API_KEY>\" " +
                "password=\"<CCLOUD_API_SECRET>\";");
        return SRconfig;
    }
}
