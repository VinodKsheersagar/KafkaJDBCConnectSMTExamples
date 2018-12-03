package custom.kafka.connect;

import org.apache.kafka.common.cache.Cache;
import org.apache.kafka.common.cache.LRUCache;
import org.apache.kafka.common.cache.SynchronizedCache;
import org.apache.kafka.common.config.ConfigDef;
import org.apache.kafka.connect.connector.ConnectRecord;
import org.apache.kafka.connect.data.Schema;
import org.apache.kafka.connect.data.Field;

import org.apache.kafka.connect.data.Struct;
import org.apache.kafka.connect.transforms.Transformation;
//import org.apache.kafka.connect.transforms.util.NonEmptyListValidator;
import org.apache.kafka.connect.transforms.util.SimpleConfig;


//import java.util.List;
import java.util.Map;




public class ReassignPartition<R extends ConnectRecord<R>> implements Transformation<R> {

    public static final String OVERVIEW_DOC = "Repartition Strategy.";

    public static final String FIELDS_CONFIG = "partitions";

    public static final ConfigDef CONFIG_DEF = new ConfigDef()
            .define(FIELDS_CONFIG, ConfigDef.Type.INT, new Integer(1),  ConfigDef.Importance.MEDIUM,
                    "Number of Partitions to be used.");

    // private static final String PURPOSE = "Assign Partition using Record data some field value";

    // private List<String> fields;

    private Cache<Schema, Schema> valueToKeySchemaCache;

    @Override
    public void configure(Map<String, ?> configs) {
        //final SimpleConfig simpleConfig = new SimpleConfig(CONFIG_DEF, configs);
        //final String class_no= simpleConfig.getString(FIELDS_CONFIG);
        // int partitions = config.getList(FIELDS_CONFIG);
        //valueToKeySchemaCache = new SynchronizedCache<>(new LRUCache<Schema, Schema>(16));
    }

    @Override
    public R apply(R record) {
        //  if (record.valueSchema() == null) {
        return applySchemaless(record);
        //} else {
        //  return applyWithSchema(record);
        //}
    }

    private R applySchemaless(R record) {

        Integer assignedPartition=0;
        char startChar;
        /*-------------------------------*/


        String s = (String) record.key();

        final Struct valueStruct = (Struct) record.value();

        //Field last_name;

        System.out.println("this message is from reassign Partition lastName :  "+valueStruct.get("last_name"));





        int result = Integer.parseInt(s);

        //startChar = (ss.toCharArray())[0];

        if (result >=0 && result <= 7)
            assignedPartition=0;
        else if (result >=8 && result <=9 )
            assignedPartition=1;
        else if(result >=10 && result <= 12)
            assignedPartition=2;
        else
            assignedPartition=3;


        /*-------------------------------*/

        return record.newRecord(record.topic(), assignedPartition, null, record.key(), record.valueSchema(), record.value(), record.timestamp());
    }
/*
    private R applyWithSchema(R record) {
        final Struct value = requireStruct(record.value(), PURPOSE);

        Schema keySchema = valueToKeySchemaCache.get(value.schema());
        if (keySchema == null) {
            final SchemaBuilder keySchemaBuilder = SchemaBuilder.struct();
            for (String field : fields) {
                final Schema fieldSchema = value.schema().field(field).schema();
                keySchemaBuilder.field(field, fieldSchema);
            }
            keySchema = keySchemaBuilder.build();
            valueToKeySchemaCache.put(value.schema(), keySchema);
        }

        final Struct key = new Struct(keySchema);
        for (String field : fields) {
            key.put(field, value.get(field));
        }

        return record.newRecord(record.topic(), record.kafkaPartition(), keySchema, key, value.schema(), value, record.timestamp());
    } */

    @Override
    public ConfigDef config() {
        return CONFIG_DEF;
    }

    @Override
    public void close() {
        valueToKeySchemaCache = null;
    }

}