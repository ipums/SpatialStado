//
// Generated by JTB 1.2.2
//

package org.postgresql.stado.parser.core.syntaxtree;

/**
 * Grammar production:
 * f0 -> <GRANT_>
 * f1 -> PrivilegeList(prn)
 * f2 -> <ON_>
 * f3 -> [ <TABLE_> ]
 * f4 -> TableListForGrant(prn)
 * f5 -> <TO_>
 * f6 -> GranteeList(prn)
 */
public class Grant implements Node {
   public NodeToken f0;
   public PrivilegeList f1;
   public NodeToken f2;
   public NodeOptional f3;
   public TableListForGrant f4;
   public NodeToken f5;
   public GranteeList f6;

   public Grant(NodeToken n0, PrivilegeList n1, NodeToken n2, NodeOptional n3, TableListForGrant n4, NodeToken n5, GranteeList n6) {
      f0 = n0;
      f1 = n1;
      f2 = n2;
      f3 = n3;
      f4 = n4;
      f5 = n5;
      f6 = n6;
   }

   public Grant(PrivilegeList n0, NodeOptional n1, TableListForGrant n2, GranteeList n3) {
      f0 = new NodeToken("GRANT");
      f1 = n0;
      f2 = new NodeToken("ON");
      f3 = n1;
      f4 = n2;
      f5 = new NodeToken("TO");
      f6 = n3;
   }

   public void accept(org.postgresql.stado.parser.core.visitor.Visitor v) {
      v.visit(this);
   }
   public Object accept(org.postgresql.stado.parser.core.visitor.ObjectVisitor v, Object argu) {
      return v.visit(this,argu);
   }
}

