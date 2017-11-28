package org.apache.carbondata.datamap.lucene;

import org.apache.carbondata.core.scan.expression.Expression;

public interface ExpressTreeVisitor {
    void visit(Expression expression);
//    void visitChildren(Expression expression);
//    void visitTerminalNode(Expression expression);
    void visitErrorNode(Expression expression);
}
