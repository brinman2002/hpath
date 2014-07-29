package io.github.brinman2002.hpath;

import java.util.Locale;

import org.apache.avro.generic.GenericData;
import org.apache.commons.jxpath.ri.QName;
import org.apache.commons.jxpath.ri.model.NodePointer;
import org.apache.commons.jxpath.ri.model.NodePointerFactory;

/**
 * Factory for {@link GenericRecordNodePointer}
 *
 * @author brandon
 *
 */
public class GenericRecordNodePointerFactory implements NodePointerFactory {

    @Override
    public NodePointer createNodePointer(final QName arg0, final Object object, final Locale arg2) {
        if (!(object instanceof GenericData.Record)) {
            return null;
        }
        return new GenericRecordNodePointer(arg0, (GenericData.Record) object, arg2);
    }

    @Override
    public NodePointer createNodePointer(final NodePointer nodePointer, final QName qName, final Object object) {
        if (!(object instanceof GenericData.Record)) {
            return null;
        }
        return new GenericRecordNodePointer(nodePointer, qName, (GenericData.Record) object);
    }

    @Override
    public int getOrder() {
        return 1;
    }

}
