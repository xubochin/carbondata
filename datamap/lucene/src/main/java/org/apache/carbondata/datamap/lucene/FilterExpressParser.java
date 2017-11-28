package org.apache.carbondata.datamap.lucene;

import org.apache.carbondata.core.metadata.datatype.DataType;
import org.apache.carbondata.core.metadata.datatype.DataTypes;
import org.apache.carbondata.core.scan.expression.ColumnExpression;
import org.apache.carbondata.core.scan.expression.Expression;
import org.apache.carbondata.core.scan.expression.ExpressionResult;
import org.apache.carbondata.core.scan.expression.conditional.EqualToExpression;
import org.apache.carbondata.core.scan.expression.exception.FilterIllegalMemberException;
import org.apache.carbondata.core.scan.expression.exception.FilterUnsupportedException;
import org.apache.carbondata.core.scan.expression.logical.AndExpression;
import org.apache.carbondata.core.scan.expression.logical.OrExpression;
import org.apache.carbondata.core.scan.filter.resolver.FilterResolverIntf;
import org.apache.lucene.analysis.Analyzer;
import org.apache.lucene.analysis.standard.StandardAnalyzer;
import org.apache.lucene.document.DoublePoint;
import org.apache.lucene.document.LongPoint;
import org.apache.lucene.queryparser.classic.ParseException;
import org.apache.lucene.queryparser.classic.QueryParser;
import org.apache.lucene.search.BooleanClause;
import org.apache.lucene.search.Query;
import org.apache.lucene.document.IntPoint;
import org.apache.lucene.document.LongPoint;

import java.util.ArrayList;
import java.util.List;

public class FilterExpressParser extends QueryParser implements ExpressTreeVisitor {
    private int conj = CONJ_NONE;
    private int mods = MOD_NONE;
    static final int CONJ_NONE = 0;
    static final int CONJ_AND = 1;
    static final int CONJ_OR = 2;
    static final int MOD_NONE = 0;
    static final int MOD_NOT = 10;
    static final int MOD_REQ = 11;
    private List <BooleanClause> clauses = null;


    public FilterExpressParser(String field, Analyzer analyzer) {
        super(field == null ? new String() : field, analyzer);
    }


    public Query parserFilterExpr(FilterResolverIntf filterExp) throws ParseException {
        clauses = new ArrayList <BooleanClause>();

        visit(filterExp.getFilterExpression());

        if (clauses.size() == 1) {
            return clauses.get(0).getQuery();
        } else {
            return getBooleanQuery(clauses);
        }
    }

    public static Query transformFilterExpress(FilterResolverIntf filterExp) {
        FilterExpressParser parser = new FilterExpressParser(null, new StandardAnalyzer());
        Query query = null;
        try {
            query = parser.parserFilterExpr(filterExp);
        } catch (ParseException e) {
            e.printStackTrace();
        }

        return query;
    }

    public void visit(Expression expression) {
        switch (expression.getFilterExpressionType()) {
            case AND: {
                AndExpression andExpression = (AndExpression) expression;
                Expression left = andExpression.getLeft();
                Expression right = andExpression.getRight();
                visit(left);
                conj = CONJ_AND;
                mods = MOD_NONE;
                visit(right);
            }
            break;
            case OR: {
                OrExpression orExpression = (OrExpression) expression;
                Expression left = orExpression.getLeft();
                Expression right = orExpression.getRight();
                visit(left);
                conj = CONJ_OR;
                mods = MOD_NONE;
                visit(right);
            }
            break;
            case EQUALS: {
                mods = MOD_REQ;
                try {
                    createEqualsNode(expression);
                } catch (ParseException e) {
                    e.printStackTrace();
                } catch (FilterIllegalMemberException e) {
                    e.printStackTrace();
                } catch (FilterUnsupportedException e) {
                    e.printStackTrace();
                }
            }
            break;
            case NOT_EQUALS: {
                mods = MOD_NOT;
                try {
                    createEqualsNode(expression);
                } catch (ParseException e) {
                    e.printStackTrace();
                } catch (FilterIllegalMemberException e) {
                    e.printStackTrace();
                } catch (FilterUnsupportedException e) {
                    e.printStackTrace();
                }
                break;
            }
            case LESSTHAN:
                break;
            case LESSTHAN_EQUALTO:
                break;
            case GREATERTHAN:
                break;
            case GREATERTHAN_EQUALTO:
                break;
            case ADD:
                break;
            case SUBSTRACT:
                break;
            case DIVIDE:
                break;
            case MULTIPLY:
                break;
            case IN:
                break;
            case LIST:
                break;
            case NOT_IN:
                break;
            case LITERAL:
                break;
            case RANGE:
                break;
            case FALSE:
                break;
            case TRUE:
                break;
            case NOT:
            case UNKNOWN:
            default:
        }
    }

    private void createEqualsNode(Expression expression)
            throws ParseException, FilterIllegalMemberException, FilterUnsupportedException {
        ColumnExpression column = null;
        Expression value = null;
        Query query = null;
        EqualToExpression equalExpression = (EqualToExpression) expression;
        Expression left = equalExpression.getLeft();
        Expression right = equalExpression.getRight();
        if (left instanceof ColumnExpression) {
            column = (ColumnExpression) left;
            value = right;
        } else if (right instanceof ColumnExpression) {
            column = (ColumnExpression) right;
            value = left;
        } else {
            //TODO:how to deal with
        }
        DataType type = column.getDataType();
        ExpressionResult res = value.evaluate(null);
        Query query1 = null;
        if (type == DataTypes.STRING) {
            //TODO: not find equal search in lucene, use term query replace it
            query1 = new QueryParser(column.getColumnName(), this.getAnalyzer()).parse(value.getString());
            addClause(clauses, conj, mods, query1);
        } else if (type == DataTypes.BYTE
                || type == DataTypes.SHORT
                || type == DataTypes.LONG) {
            query1 = LongPoint.newSetQuery(column.getColumnName(), res.getLong());
        } else if (type == DataTypes.DOUBLE) {
            query1 = DoublePoint.newSetQuery(column.getColumnName(), res.getDouble());
        } else if (type == DataTypes.TIMESTAMP) {
            //TODO: how to do?
        }
        else{
            throw new RuntimeException("not supported data type" + type.getName());
        }
        addClause(clauses, conj, mods, query1);
    }

    public void visitErrorNode(Expression expression) {

    }
}