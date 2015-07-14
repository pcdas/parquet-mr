package parquet.filter2.predicate.iotas.index;

/**
 * Checks for membership in a sorted queue. The values has to be checked in an ascending fashion.
 * Created by abennett on 16/7/15.
 */
public class SortedIntQueue {

    private final SortedIntIterator values;
    private boolean hasValue;
    private int currentValue;

    public SortedIntQueue(SortedIntIterator values) {
        this.values = values;
        this.hasValue = false;
        this.currentValue = 0;
    }

    public void init() {
        if (values.hasNext()) {
            hasValue = true;
            currentValue = values.nextDedupe();
        }

    }

    public boolean checkAndPop(int value) {
        if (!hasValue) {
            return false;
        }
        if (value < currentValue) {
            return false;
        } else if (value == currentValue) {
            return true;
        } else {
            while (hasValue && value > currentValue) {
                readNext();
            }
            return checkAndPop(value);
        }

    }

    private void readNext() {
        if (values.hasNext()) {
            currentValue = values.nextDedupe();
        } else {
            hasValue = false;
        }
    }

}
