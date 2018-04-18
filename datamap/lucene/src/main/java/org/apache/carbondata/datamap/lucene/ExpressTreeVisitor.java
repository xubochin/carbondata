package org.apache.carbondata.datamap.lucene;

import org.apache.carbondata.core.scan.expression.Expression;
import org.apache.carbondata.core.scan.expression.exception.FilterIllegalMemberException;
import org.apache.carbondata.core.scan.expression.exception.FilterUnsupportedException;
import org.apache.lucene.queryparser.classic.ParseException;

public interface ExpressTreeVisitor {
    void visit(Expression expression) throws ParseException, FilterIllegalMemberException, FilterUnsupportedException;
//    void visitChildren(Expression expression);
//    void visitTerminalNode(Expression expression);
    void visitErrorNode(Expression expression);
}
