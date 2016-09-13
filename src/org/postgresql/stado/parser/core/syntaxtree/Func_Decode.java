//
// Generated by JTB 1.2.2
//

package org.postgresql.stado.parser.core.syntaxtree;

/**
 * Grammar production:
 * f0 -> <DECODE_>
 * f1 -> <PARENTHESIS_START_>
 * f2 -> SQLArgument(prn)
 * f3 -> ","
 * f4 -> SQLArgument(prn)
 * f5 -> ( "," SQLArgument(prn) )*
 * f6 -> <PARENTHESIS_CLOSE_>
 */
public class Func_Decode implements Node {
   public NodeToken f0;
   public NodeToken f1;
   public SQLArgument f2;
   public NodeToken f3;
   public SQLArgument f4;
   public NodeListOptional f5;
   public NodeToken f6;

   public Func_Decode(NodeToken n0, NodeToken n1, SQLArgument n2, NodeToken n3, SQLArgument n4, NodeListOptional n5, NodeToken n6) {
      f0 = n0;
      f1 = n1;
      f2 = n2;
      f3 = n3;
      f4 = n4;
      f5 = n5;
      f6 = n6;
   }

   public Func_Decode(SQLArgument n0, SQLArgument n1, NodeListOptional n2) {
      f0 = new NodeToken("DECODE");
      f1 = new NodeToken("(");
      f2 = n0;
      f3 = new NodeToken(",");
      f4 = n1;
      f5 = n2;
      f6 = new NodeToken(")");
   }

   public void accept(org.postgresql.stado.parser.core.visitor.Visitor v) {
      v.visit(this);
   }
   public Object accept(org.postgresql.stado.parser.core.visitor.ObjectVisitor v, Object argu) {
      return v.visit(this,argu);
   }
}

