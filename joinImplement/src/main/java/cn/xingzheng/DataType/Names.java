package cn.xingzheng.DataType;

import org.apache.flink.api.java.tuple.Tuple;
import scala.Array;

import java.util.ArrayList;

public class Names extends Tuple {

    public ArrayList<Name> names ;

    public Names() {
        names = new ArrayList<>();
    }

    /**
     * Sets the field at the specified position.
     *
     * @param value The value to be assigned to the field at the specified position.
     * @param pos   The position of the field, zero indexed.
     * @throws IndexOutOfBoundsException Thrown, if the position is negative, or equal to, or larger than the number of fields.
     */
    @Override
    public <Name> void setField(Name value, int pos) {
        names.set(pos, (cn.xingzheng.DataType.Name) value);
    }

    public Names(ArrayList<Name> names) {
        this.names = names;
    }


    /**
     * Gets the field at the specified position.
     *
     * @param pos The position of the field, zero indexed.
     * @return The field at the specified position.
     * @throws IndexOutOfBoundsException Thrown, if the position is negative, or equal to, or larger than the number of fields.
     */
    @Override
    public Name getField(int pos) {
        return names.get(pos);
    }


    /**
     * Gets the number of field in the tuple (the tuple arity).
     *
     * @return The number of fields in the tuple.
     */
    @Override
    public int getArity() {
        return names.size();
    }

    /**
     * Shallow tuple copy.
     *
     * @return A new Tuple with the same fields as this.
     */
    @Override
    public Names copy() {
        Names copy = new Names(this.names);
        return copy;
    }
}
