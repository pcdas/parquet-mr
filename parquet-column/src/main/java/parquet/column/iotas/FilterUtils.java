package parquet.column.iotas;

import parquet.filter2.predicate.FilterPredicate;
import parquet.filter2.predicate.Operators;

import java.util.ArrayList;
import java.util.List;

/**
 * Created by abennett on 24/6/15.
 */
public class FilterUtils {

    public static List<FilterPredicate> getLeafNodes(FilterPredicate root) {
        List<FilterPredicate> filterList = new ArrayList<FilterPredicate>();
        filterList = unwrap(root, filterList);
        return filterList;
    }

    private static List<FilterPredicate> unwrap(FilterPredicate filter, List<FilterPredicate> filterList) {
        if (filter == null) {
            return filterList;
        }
        if (filter instanceof Operators.And) {
            filterList = unwrap(((Operators.And) filter).getLeft(), filterList);
            filterList = unwrap(((Operators.And) filter).getRight(), filterList);
        } else if (filter instanceof Operators.Or) {
            filterList = unwrap(((Operators.Or) filter).getLeft(), filterList);
            filterList = unwrap(((Operators.Or) filter).getRight(), filterList);
        } else if (filter instanceof Operators.Not) {
            filterList = unwrap(((Operators.Not) filter).getPredicate(), filterList);
        } else
            filterList.add(filter);
        return filterList;
    }
}
