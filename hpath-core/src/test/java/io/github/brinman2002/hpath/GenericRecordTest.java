package io.github.brinman2002.hpath;

import static io.github.brinman2002.hpath.RecordUtil.iteratorAsList;
import static io.github.brinman2002.hpath.RecordUtil.record;
import static org.junit.Assert.assertEquals;

import java.util.Arrays;
import java.util.List;

import org.apache.avro.generic.GenericData;
import org.apache.commons.jxpath.JXPathContext;
import org.apache.commons.jxpath.ri.JXPathContextReferenceImpl;
import org.junit.BeforeClass;
import org.junit.Test;

/**
 * Tests of the node pointer factory and node pointer specific to GenericRecord.
 *
 * @author brandon
 *
 */
public class GenericRecordTest {

    @BeforeClass
    public static void register() {
        JXPathContextReferenceImpl.addNodePointerFactory(new GenericRecordNodePointerFactory());
    }

    @SuppressWarnings("unchecked")
    @Test
    public void basicXPath() throws Exception {

        final JXPathContext context = JXPathContext.newContext(Arrays.asList(record("Brandon", 37, "male", false),
                record("Dave Brockie", 50, "male", true)));

        final List<GenericData.Record> result = iteratorAsList(context.iterate(".[name='Brandon']"));
        assertEquals(1, result.size());
        assertEquals("Brandon", result.get(0).get("name"));
    }

    @Test
    public void outerFunctionXPath() throws Exception {

        final JXPathContext context = JXPathContext.newContext(Arrays.asList(record("Brandon", 37, "male", false),
                record("Dave Brockie", 50, "male", true)));

        @SuppressWarnings("unchecked")
        final List<Double> result = iteratorAsList(context.iterate("count(.[gender='male']) + count(.[name='Brandon'])"));
        assertEquals(1, result.size());
        assertEquals((Double) 3.0, result.get(0));
    }

    @Test
    public void querySubtree() throws Exception {
        final JXPathContext context = JXPathContext.newContext(Arrays.asList(record("Brandon", 37, "male", false),
                record("Dave Brockie", 50, "male", true)));

        @SuppressWarnings("unchecked")
        final List<String> results = iteratorAsList(context.iterate("//name"));
        assertEquals(2, results.size());
        assertEquals("Brandon", results.get(0));
        assertEquals("Dave Brockie", results.get(1));
    }

}
