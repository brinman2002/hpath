package io.github.brinman2002.hpath.pipeline;

import io.github.brinman2002.hpath.GenericRecordNodePointerFactory;

import java.util.Iterator;

import org.apache.avro.generic.GenericData;
import org.apache.avro.generic.GenericData.Record;
import org.apache.avro.util.Utf8;
import org.apache.commons.jxpath.JXPathContext;
import org.apache.commons.jxpath.ri.ExpressionHelper;
import org.apache.commons.jxpath.ri.JXPathCompiledExpression;
import org.apache.commons.jxpath.ri.JXPathContextReferenceImpl;
import org.apache.commons.jxpath.ri.compiler.CoreFunction;
import org.apache.commons.jxpath.ri.compiler.CoreOperation;
import org.apache.commons.jxpath.ri.compiler.Expression;
import org.apache.commons.jxpath.ri.compiler.LocationPath;
import org.apache.commons.jxpath.ri.model.NodePointerFactory;
import org.apache.crunch.DoFn;
import org.apache.crunch.Emitter;
import org.apache.crunch.PCollection;
import org.apache.crunch.types.PType;

/**
 * Utility functions for performing XPath queries over PCollections of data.
 * This is an evolving class and queries that don't perform correctly or at all
 * now may be well supported at a future date.
 *
 * @author brandon
 *
 */
public class HPath {

    static {
        initializeNodePointerFactory();
    }

    static synchronized void initializeNodePointerFactory() {
        final NodePointerFactory[] factories = JXPathContextReferenceImpl.getNodePointerFactories();
        if (factories != null) {
            for (final NodePointerFactory factory : factories) {
                if (factory instanceof GenericRecordNodePointerFactory) {
                    return;
                }
            }
        }
        JXPathContextReferenceImpl.addNodePointerFactory(new GenericRecordNodePointerFactory());
    }

    /**
     * Perform an XPath query over the PCollection. Queries are limited in that
     * the outer-most part of the query must be a location path. XPath
     * operations and functions may be used within the query.
     *
     * @param data
     *            The data that is being queried.
     * @param query
     *            The XPath query. Not all queries are supported; see
     *            implementation for details.
     * @param type
     *            The expected PType of the returned result.
     * @return PCollection representing the result of the query.
     */
    public static <T> PCollection<T> query(final PCollection<GenericData.Record> data, final String query, final PType<T> type) {

        final JXPathCompiledExpression jxExpression = (JXPathCompiledExpression) JXPathContext.compile(query);
        final Expression expression = ExpressionHelper.getExpression(jxExpression);

        if (expression instanceof LocationPath) {
            return data.parallelDo(new XPathDoFn<T>(query), type);
        }

        if (expression instanceof CoreOperation || expression instanceof CoreFunction) {
            throw new IllegalArgumentException("Illegal query: XPath operations and functions not currently supported");
        }

        throw new IllegalStateException("Illegal query: Was not a location, operation or function, instead was instance of " + expression.getClass());
    }

    /**
     * DoFn that performs the XPath query on a single instance of a
     * {@link GenericData.Record}.
     *
     * @author brandon
     *
     * @param <T>
     */
    static class XPathDoFn<T> extends DoFn<GenericData.Record, T> {
        /**
         *
         */
        private static final long serialVersionUID = -9009001375502914981L;

        final private String xPath;

        public XPathDoFn(final String xPath) {
            this.xPath = xPath;
        }

        @Override
        public void initialize() {
            super.initialize();
            initializeNodePointerFactory();
        }

        @SuppressWarnings("unchecked")
        @Override
        public void process(final Record input, final Emitter<T> emitter) {
            final JXPathContext context = JXPathContext.newContext(input);
            final Iterator<T> iterator = context.iterate(xPath);
            while (iterator.hasNext()) {
                final T o = iterator.next();
                if (o instanceof Utf8) {
                    /**
                     * Manually convert the internal 'Utf8' class into a regular
                     * string. This is a bit ugly but there doesn't seem to be a
                     * good way around it.
                     */
                    emitter.emit((T) o.toString());
                } else {
                    emitter.emit(o);
                }
            }
        }

    }
}
