package io.github.brinman2002.hpath;

import java.util.Locale;

import org.apache.avro.generic.GenericData;
import org.apache.avro.generic.GenericData.Record;
import org.apache.commons.jxpath.ri.Compiler;
import org.apache.commons.jxpath.ri.QName;
import org.apache.commons.jxpath.ri.compiler.NodeNameTest;
import org.apache.commons.jxpath.ri.compiler.NodeTest;
import org.apache.commons.jxpath.ri.compiler.NodeTypeTest;
import org.apache.commons.jxpath.ri.model.NodeIterator;
import org.apache.commons.jxpath.ri.model.NodePointer;
import org.apache.commons.jxpath.util.ValueUtils;

/**
 * {@link NodePointer} for an Avro generic record.
 *
 * @author brandon
 *
 */
public class GenericRecordNodePointer extends NodePointer {

    private static final long serialVersionUID = -306108560333191945L;

    private GenericData.Record value;

    private final QName qname;

    /**
     * Create instance.
     *
     * @param parent
     *            Parent object
     * @param qname
     *            Object QName
     * @param record
     *            The actual data that the NodePointer represents
     */
    public GenericRecordNodePointer(final NodePointer parent, final QName qname, final GenericData.Record record) {
        super(parent);
        this.qname = qname;
        this.value = record;
    }

    /**
     * Create instance.
     *
     * @param qname
     *            Object QName
     * @param record
     *            The actual data that the NodePointer represents
     * @param locale
     *            Locale instance to pass to base class constructor
     */
    public GenericRecordNodePointer(final QName qname, final GenericData.Record record, final Locale locale) {
        super(null, locale);
        this.qname = qname;
        this.value = record;
    }

    @SuppressWarnings({ "unchecked", "rawtypes" })
    @Override
    public int compareChildNodePointers(final NodePointer pointer1, final NodePointer pointer2) {

        /*
         * Null check the pointers. Technically, if we really follow the base
         * class Javadoc and use "Java comparison conventions", we could throw
         * NullPointerException, but we won't.
         */

        if (pointer1 == null && pointer2 == null) {
            return 0;
        }

        if (pointer1 == null) {
            return 1;
        }
        if (pointer2 == null) {
            return -1;
        }

        // If the two pointer objects don't have the same class, compare based
        // on the class name.
        if (pointer1.getClass() != pointer2.getClass()) {
            return pointer1.getClass().getName().compareTo(pointer2.getClass().getName());
        }

        final Object obj1 = pointer1.getBaseValue();
        final Object obj2 = pointer2.getBaseValue();

        // Null check the values.

        if (obj1 == null && obj2 == null) {
            return 0;
        }

        if (obj1 == null) {
            return 1;
        }
        if (obj2 == null) {
            return -1;
        }
        // This will catch two GenericData.Record instances and use their built
        // in compareTo(), in addition to any other sets of classes that
        // also implement Comparable.

        if (obj1 instanceof Comparable && obj2 instanceof Comparable) {
            try {
                return ((Comparable) obj1).compareTo(obj2);
            } catch (final ClassCastException e) {
                // If obj1's class doesn't support comparing itself to obj2,
                // we'll keep going.
            }
        }

        // When they aren't Comparable, sort GenericData.Record instances before
        // other classes.
        if (obj1 instanceof GenericData.Record && !(obj2 instanceof GenericData.Record)) {
            return -1;
        }
        if (!(obj1 instanceof GenericData.Record) && obj2 instanceof GenericData.Record) {
            return 1;
        }

        // At this point, we have two objects that neither are
        // GenericData.Record, and we've made every reasonable attempt to
        // compare them.

        // We could probably throw ClassCastException per Comparable's javadoc
        // and call it good, but in the interest of supporting ordering whenever
        // possible (and since JXPath doesn't provide unordered behavior if this
        // does throw), even when it may be inconsistent or just plain weird,
        // take one last approach.
        return obj1.toString().compareTo(obj2.toString());

    }

    @Override
    public Object getBaseValue() {
        return value;
    }

    @Override
    public GenericData.Record getImmediateNode() {
        return value;
    }

    @Override
    public int getLength() {
        return 1;
    }

    @Override
    public QName getName() {
        return qname;
    }

    @Override
    public boolean isCollection() {
        return ValueUtils.isCollection(value);
    }

    @Override
    public boolean isLeaf() {
        return false;
    }

    @Override
    public void setValue(final Object value) {
        if (!(value instanceof GenericData.Record)) {
            throw new IllegalArgumentException();
        }
        this.value = (Record) value;
    }

    @Override
    public NodeIterator childIterator(final NodeTest test, final boolean reverse, final NodePointer startWith) {
        return new GenericRecordIterator(this, test, reverse, startWith);
    }

    @Override
    public boolean testNode(final NodeTest nodeTest) {
        if (nodeTest == null) {
            return true;
        }

        if (value == null) {
            return false;
        }

        if (nodeTest instanceof NodeNameTest) {
            return doNodeNameTest((NodeNameTest) nodeTest);
        }

        if (nodeTest instanceof NodeTypeTest) {
            return Compiler.NODE_TYPE_NODE == ((NodeTypeTest) nodeTest).getNodeType();
        }

        return false;
    }

    private boolean doNodeNameTest(final NodeNameTest nodeNameTest) {
        if (nodeNameTest.isWildcard()) {
            return true;
        }
        if (nodeNameTest.getNodeName() == null) {
            return false;
        }
        return nodeNameTest.getNodeName().equals(qname);
    }

}
