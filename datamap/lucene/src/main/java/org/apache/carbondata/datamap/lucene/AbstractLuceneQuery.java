package org.apache.carbondata.datamap.lucene;

import org.apache.carbondata.core.scan.expression.ColumnExpression;
import org.apache.carbondata.core.scan.expression.Expression;
import org.apache.lucene.search.Query;

import java.util.List;

public abstract class AbstractLuceneQuery implements LuceneQuery {
    List <Expression> columns = null;
    List <Expression> values = null;

    abstract Query newQuery();

    public Query getQuery(Expression[] expressions) {

        return null;
    }

    public Query getTwoParametersQuery(Expression expression) {
        assert expression.getChildren().size() == 2;
        Expression expr1 = expression.getChildren().get(0);
        Expression expr2 = expression.getChildren().get(1);
        Expression column = null;
        Expression value = null;
        if (expr1 instanceof ColumnExpression) {
            column = expr1;
            value = expr2;
        } else {
            column = expr2;
            value = expr1;
        }
        return getQuery(new Expression[]{column,value});
    }

    public Query getThreeParametersQuery(Expression expression){
        assert expression.getChildren().size()==2;
        Expression expr1 = expression.getChildren().get(0);
        Expression expr2 = expression.getChildren().get(1);
        Expression expr3 = expression.getChildren().get(2);
        
    }
}
