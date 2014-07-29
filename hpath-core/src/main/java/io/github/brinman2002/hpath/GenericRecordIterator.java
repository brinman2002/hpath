package io.github.brinman2002.hpath;

import java.util.ArrayList;
import java.util.Iterator;
import java.util.List;

import org.apache.avro.Schema.Field;
import org.apache.avro.generic.GenericData;
import org.apache.commons.jxpath.ri.QName;
import org.apache.commons.jxpath.ri.compiler.NodeTest;
import org.apache.commons.jxpath.ri.model.NodeIterator;
import org.apache.commons.jxpath.ri.model.NodePointer;
import org.apache.commons.jxpath.util.ValueUtils;

/**
 * {@link NodeIterator} that iterates over the fields of an Avro generic record
 * instance.
 *
 * @author brandon
 *
 */
public class GenericRecordIterator implements NodeIterator {

    private int pos = 0;

    private final List<NodePointer> childs = new ArrayList<NodePointer>();

    public GenericRecordIterator(final GenericRecordNodePointer pointer, final NodeTest test, final boolean reverse, final NodePointer startWith) {
        initialize(pointer, test, reverse, startWith);
    }

    void initialize(final GenericRecordNodePointer pointer, final NodeTest test, final boolean reverse, final NodePointer startWith) {
        final GenericData.Record record = pointer.getImmediateNode();

        for (final Field field : record.getSchema().getFields()) {
            final String name = field.name();
            final QName qName = new QName(name);
            final Object value = record.get(name);
            // If the value is not a collection, iterate will return an iterator
            // with that single element.
            final Iterator<?> iterator = ValueUtils.iterate(value);
            while (iterator.hasNext()) {
                final NodePointer child = createChild(pointer, qName, iterator.next());
                if ((child != null) && child.testNode(test)) {
                    if (reverse) {
                        childs.add(0, child);
                    } else {
                        childs.add(child);
                    }
                }
            }
        }
    }

    private NodePointer createChild(final GenericRecordNodePointer pointer, final QName qName, final Object value) {
        return NodePointer.newChildNodePointer(pointer, qName, value);
    }

    @Override
    public int getPosition() {
        return pos;
    }

    @Override
    public boolean setPosition(final int position) {
        if (!isValid(position)) {
            return false;
        }
        pos = position;
        return true;
    }

    @Override
    public NodePointer getNodePointer() {
        if (!isValid(pos)) {
            return null;
        }
        return childs.get(pos - 1);
    }

    private boolean isValid(final int position) {
        return position > 0 && position <= childs.size();
    }

}
