package io.github.brinman2002.hpath.pipeline;

import static io.github.brinman2002.hpath.RecordUtil.collectionOf;
import static io.github.brinman2002.hpath.RecordUtil.record;
import static org.junit.Assert.assertEquals;
import io.github.brinman2002.hpath.RecordUtil;

import java.io.IOException;
import java.util.ArrayList;
import java.util.List;

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

        final List<Record> materialized = new ArrayList<Record>(queryResult.asCollection().getValue());

        assertEquals(1, materialized.size());
        assertEquals(record("Dave Brockie", 50, "male", true), materialized.get(0));
    }

    @Test
    public void stringResult() throws Exception {
        final PCollection<Record> pCollection = collectionOf(record("Brandon", 37, "male", false), record("Dave Brockie", 50, "male", true));

        final PCollection<String> queryResult = HPath.query(pCollection, "//name", Avros.strings());

        final List<String> materialized = new ArrayList<String>(queryResult.asCollection().getValue());
        assertEquals(2, materialized.size());
        assertEquals("Brandon", materialized.get(0));
        assertEquals("Dave Brockie", materialized.get(1));

    }

    @Test
    public void doubleResult() throws Exception {
        final PCollection<Record> pCollection = collectionOf(record("Brandon", 37, "male", false), record("Dave Brockie", 50, "male", true));

        final PCollection<Integer> queryResult = HPath.query(pCollection, "//age", Avros.ints());

        final List<Integer> materialized = new ArrayList<Integer>(queryResult.asCollection().getValue());

        assertEquals(2, materialized.size());
        assertEquals((Integer) 37, materialized.get(0));
        assertEquals((Integer) 50, materialized.get(1));

    }

    /**
     * Throw exception on unsupported query.
     *
     * @throws Exception
     */
    @Test(expected = IllegalArgumentException.class)
    public void function() throws Exception {
        final PCollection<Record> pCollection = collectionOf(record("Brandon", 37, "male", false), record("Dave Brockie", 50, "male", true));

        HPath.query(pCollection, "count(.)", Avros.doubles());
    }

    /**
     * Throw exception on unsupported query.
     *
     * @throws Exception
     */
    @Test(expected = IllegalArgumentException.class)
    public void operation() throws Exception {
        final PCollection<Record> pCollection = collectionOf(record("Brandon", 37, "male", false), record("Dave Brockie", 50, "male", true));

        HPath.query(pCollection, "1 + 1", Avros.doubles());
    }

    @Test
    public void address() throws Exception {
        final PCollection<Record> pCollection = collectionOf(record("Brandon", 37, "male", false), record("Dave Brockie", 50, "male", true));

        final Record brockie = (Record) record("Dave Brockie", 50, "male", true).get("address");

        final PCollection<Record> queryResult = HPath.query(pCollection, "address[street='something']", Avros.generics(brockie.getSchema()));

        final List<Record> materialized = new ArrayList<Record>(queryResult.asCollection().getValue());

        assertEquals(1, materialized.size());
        assertEquals(brockie, materialized.get(0));
    }
}
