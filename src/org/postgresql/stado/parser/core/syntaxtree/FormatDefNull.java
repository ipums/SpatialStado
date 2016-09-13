//
// Generated by JTB 1.2.2
//

package org.postgresql.stado.parser.core.syntaxtree;

/**
 * Grammar production:
 * f0 -> <NULL_>
 * f1 -> [ <AS_> ]
 * f2 -> <STRING_LITERAL>
 */
public class FormatDefNull implements Node {
   public NodeToken f0;
   public NodeOptional f1;
   public NodeToken f2;

   public FormatDefNull(NodeToken n0, NodeOptional n1, NodeToken n2) {
      f0 = n0;
      f1 = n1;
      f2 = n2;
   }

   public FormatDefNull(NodeOptional n0) {
      f0 = new NodeToken("NULL");
      f1 = n0;
      f2 = new NodeToken("'");
   }

   public void accept(org.postgresql.stado.parser.core.visitor.Visitor v) {
      v.visit(this);
   }
   public Object accept(org.postgresql.stado.parser.core.visitor.ObjectVisitor v, Object argu) {
      return v.visit(this,argu);
   }
}

