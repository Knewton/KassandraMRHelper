package com.knewton.mapreduce;

import org.apache.cassandra.db.CounterCell;
import org.apache.cassandra.db.DeletedCell;
import org.apache.cassandra.db.OnDiskAtom;
import org.apache.cassandra.db.RangeTombstone;
import org.apache.hadoop.io.Writable;
import org.apache.hadoop.io.WritableComparable;
import org.apache.hadoop.mapreduce.Mapper;

import java.io.IOException;
import java.nio.ByteBuffer;

/**
 * Parent class of all SSTable mappers. You shouldn't be inheriting from this class. Instead, you
 * should look at its subclasses as a starting point.
 *
 * @author Giannis Neokleous
 *
 * @param <CASS_VALUE>
 *            The type of Columns/Cells to care about. If you're inheriting from this class and
 *            specify this value, this mapper will skip any types of cells that are not assignable.
 *            For example if the type is {@link CounterCell}, then any other Cells that are not of
 *            this type will be skipped.
 * @param <K1>
 *            The incoming key type
 * @param <V1>
 *            The incoming value type
 * @param <K2>
 *            The outgoing key type
 * @param <V2>
 *            The outgoing value type
 */
public abstract class AbstractSSTableMapper<CASS_VALUE extends OnDiskAtom,
                                            K1, V1,
                                            K2 extends WritableComparable<?>, V2 extends Writable>
        extends Mapper<ByteBuffer, CASS_VALUE, K2, V2> {

    private ByteBuffer key;
    private CASS_VALUE value;
    private boolean skipDeletedAtoms;
    private final Class<CASS_VALUE> cassandraAtomType;

    public AbstractSSTableMapper(Class<CASS_VALUE> cassandraAtomType) {
        skipDeletedAtoms = false;
        this.cassandraAtomType = cassandraAtomType;
    }

    /**
     * {@inheritDoc}
     */
    @Override
    public void map(ByteBuffer key, CASS_VALUE atom, Context context) throws IOException,
            InterruptedException {

        // skip deleted atoms if necessary
        if (skipDeletedAtoms && (atom instanceof DeletedCell || atom instanceof RangeTombstone)) {
            return;
        }

        // skip types we don't care about
        if (!cassandraAtomType.isAssignableFrom(atom.getClass())) {
            return;
        }

        K1 mapKey = getMapperKey(key, context);
        if (mapKey == null) {
            return;
        }

        V1 mapValue = getMapperValue(atom, context);
        if (mapValue == null) {
            return;
        }

        this.value = atom;
        this.key = key;
        performMapTask(mapKey, mapValue, context);
    }

    /**
     * This should be defined in the child class to output any key/value pairs that need to go to a
     * reducer.
     */
    public abstract void performMapTask(K1 key, V1 value, Context context) throws IOException,
            InterruptedException;

    /**
     * Get the mapper specific key that would make the sstable row key into something more
     * meaningful.
     *
     * @return Mapper key of type <code>K1</code>
     */
    protected abstract K1 getMapperKey(ByteBuffer key, Context context);

    /**
     * Get the mapper specific value that would make an SSTable atom into something more
     * meaningful.
     *
     * @return Mapper value of type <code>V1</code>
     */
    protected abstract V1 getMapperValue(CASS_VALUE atom, Context context);

    /**
     * Any modifications to this row key bytebuffer should be rewinded. Make sure you know what
     * you're doing if you call this.
     *
     * @return The original <code>ByteBuffer</code> representing the row key.
     */
    protected ByteBuffer getRowKey() {
        return key;
    }

    /**
     * Any modifications to the atom's name <code>ByteBuffer</code> or the atom's value
     * <code>ByteBuffer</code> needs to be rewinded. Make sure you know what you're doing if you
     * call this.
     *
     * @return The current cassandra value.
     */
    protected CASS_VALUE getValue() {
        return value;
    }

    /**
     * @param skipDeletedAtoms
     *            When set to true atoms marked for deletion will be ignored.
     */
    public void setSkipDeletedAtoms(boolean skipDeletedAtoms) {
        this.skipDeletedAtoms = skipDeletedAtoms;
    }

    /**
     * @return True if deleted atoms will be skipped false otherwise.
     */
    public boolean isSkipDeletedAtoms() {
        return skipDeletedAtoms;
    }
}
