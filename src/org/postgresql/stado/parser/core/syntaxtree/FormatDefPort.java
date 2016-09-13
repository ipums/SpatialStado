//
// Generated by JTB 1.2.2
//

package org.postgresql.stado.parser.core.syntaxtree;

/**
 * Grammar production:
 * f0 -> <PORT_>
 * f1 -> [ "=" ]
 * f2 -> <INT_LITERAL>
 */
public class FormatDefPort implements Node {
   public NodeToken f0;
   public NodeOptional f1;
   public NodeToken f2;

   public FormatDefPort(NodeToken n0, NodeOptional n1, NodeToken n2) {
      f0 = n0;
      f1 = n1;
      f2 = n2;
   }

   public FormatDefPort(NodeOptional n0, NodeToken n1) {
      f0 = new NodeToken("PORT");
      f1 = n0;
      f2 = n1;
   }

   public void accept(org.postgresql.stado.parser.core.visitor.Visitor v) {
      v.visit(this);
   }
   public Object accept(org.postgresql.stado.parser.core.visitor.ObjectVisitor v, Object argu) {
      return v.visit(this,argu);
   }
}

