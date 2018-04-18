package org.apache.carbondata.datamap.lucene;

import org.apache.carbondata.core.metadata.datatype.DataType;
import org.apache.carbondata.core.scan.expression.Expression;
import org.apache.lucene.search.Query;

public interface LuceneQuery
    public Query getQuery(Expression expression);
}
