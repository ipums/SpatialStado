//
// Generated by JTB 1.2.2
//

package org.postgresql.stado.parser.core.syntaxtree;

/**
 * Grammar production:
 * f0 -> <BOX2D_>
 */
public class Box2DDataType implements Node {
   public NodeToken f0;

   public Box2DDataType(NodeToken n0) {
      f0 = n0;
   }

   public Box2DDataType() {
      f0 = new NodeToken("BOX2D");
   }

   public void accept(org.postgresql.stado.parser.core.visitor.Visitor v) {
      v.visit(this);
   }
   public Object accept(org.postgresql.stado.parser.core.visitor.ObjectVisitor v, Object argu) {
      return v.visit(this,argu);
   }
}

