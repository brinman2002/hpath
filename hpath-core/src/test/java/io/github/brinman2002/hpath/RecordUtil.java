package io.github.brinman2002.hpath;

import java.io.IOException;
import java.io.InputStream;
import java.util.ArrayList;
import java.util.Iterator;
import java.util.List;

import org.apache.avro.Schema;
import org.apache.avro.generic.GenericData;
import org.apache.crunch.PCollection;
import org.apache.crunch.impl.mem.MemPipeline;
import org.apache.crunch.types.avro.Avros;

import com.google.common.collect.Iterators;

public class RecordUtil {

    public static GenericData.Record record(final String name, final int age, final String gender, final boolean useAddress) throws Exception {

        final Schema schema = getSchema();
        final GenericData.Record record = new GenericData.Record(schema);
        record.put("name", name);
        record.put("age", age);
        record.put("gender", gender);

        final GenericData.Record address = new GenericData.Record(schema.getField("address").schema().getTypes().get(1));
        address.put("street", "something");
        address.put("city", "metal metal land");
        if (useAddress) {
            record.put("address", address);
        }

        return record;
    }

    public static PCollection<GenericData.Record> collectionOf(final GenericData.Record... records) throws IOException {
        return MemPipeline.typedCollectionOf(Avros.generics(getSchema()), records);
    }

    public static <T> List<T> iteratorAsList(final Iterator<T> i) {
        final List<T> list = new ArrayList<T>();
        Iterators.addAll(list, i);
        return list;
    }

    public static Schema getSchema() throws IOException {
        final InputStream stream = RecordUtil.class.getResourceAsStream("/test.avsc");
        final Schema schema = new Schema.Parser().parse(stream);
        return schema;
    }

    public static Schema getAddressSchema() throws IOException {
        return getSchema().getField("address").schema().getTypes().get(1);
    }
}
