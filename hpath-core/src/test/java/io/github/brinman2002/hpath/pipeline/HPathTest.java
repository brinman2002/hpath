package io.github.brinman2002.hpath.pipeline;

import static io.github.brinman2002.hpath.RecordUtil.collectionOf;
import static io.github.brinman2002.hpath.RecordUtil.record;
import static org.junit.Assert.assertEquals;
import io.github.brinman2002.hpath.RecordUtil;

import java.io.IOException;
import java.util.Collection;

import org.apache.avro.generic.GenericData.Record;
import org.apache.crunch.PCollection;
import org.apache.crunch.types.avro.Avros;
import org.junit.Test;

/**
 * Tests for {@link HPath}
 *
 * @author brandon
 *
 */
public class HPathTest {

    @Test
    public void simplestQuery() throws IOException, Exception {
        final PCollection<Record> pCollection = collectionOf(record("Brandon", 37, "male", false), record("Dave Brockie", 50, "male", true));

        final PCollection<Record> queryResult = HPath.query(pCollection, ".[name = 'Dave Brockie']", Avros.generics(RecordUtil.getSchema()));

        final Iterable<Record> materialized = queryResult.materialize();
        // TODO assertions

        for (final Record r : materialized) {
            System.out.println("-----------------");
            System.out.println(r);
        }
    }

    @Test
    public void stringResult() throws Exception {
        final PCollection<Record> pCollection = collectionOf(record("Brandon", 37, "male", false), record("Dave Brockie", 50, "male", true));

        final PCollection<String> queryResult = HPath.query(pCollection, "//name", Avros.strings());

        final Collection<String> materialized = queryResult.asCollection().getValue();
        assertEquals(2, materialized.size());
        // TODO assertions
    }

    /**
     * TODO this test demonstrates that we don't handle outer level expressions
     * right at all
     *
     * @throws Exception
     */
    @Test(expected = IllegalArgumentException.class)
    public void aggregationFunction() throws Exception {
        final PCollection<Record> pCollection = collectionOf(record("Brandon", 37, "male", false), record("Dave Brockie", 50, "male", true));

        HPath.query(pCollection, "count(.)", Avros.doubles());

    }

    @Test
    public void address() throws Exception {
        final PCollection<Record> pCollection = collectionOf(record("Brandon", 37, "male", false), record("Dave Brockie", 50, "male", true));

        // The type system in MemPipeline lets us cheat on the PType. TODO as
        // this work matures, call this properly.
        final PCollection<Double> queryResult = HPath.query(pCollection, "address[street='something']", Avros.doubles());

        final Collection<? extends Object> materialized = queryResult.asCollection().getValue();
        // TODO assertions
        for (final Object r : materialized) {
            System.out.println("-----------------");
            System.out.println(r);
        }
    }

}
