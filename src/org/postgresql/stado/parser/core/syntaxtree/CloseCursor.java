//
// Generated by JTB 1.2.2
//

package org.postgresql.stado.parser.core.syntaxtree;

/**
 * Grammar production:
 * f0 -> <CLOSE_>
 * f1 -> ( Identifier(prn) | <ALL_> )
 */
public class CloseCursor implements Node {
   public NodeToken f0;
   public NodeChoice f1;

   public CloseCursor(NodeToken n0, NodeChoice n1) {
      f0 = n0;
      f1 = n1;
   }

   public CloseCursor(NodeChoice n0) {
      f0 = new NodeToken("CLOSE");
      f1 = n0;
   }

   public void accept(org.postgresql.stado.parser.core.visitor.Visitor v) {
      v.visit(this);
   }
   public Object accept(org.postgresql.stado.parser.core.visitor.ObjectVisitor v, Object argu) {
      return v.visit(this,argu);
   }
}
