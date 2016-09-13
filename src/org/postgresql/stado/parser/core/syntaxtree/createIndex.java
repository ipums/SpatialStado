//
// Generated by JTB 1.2.2
//

package org.postgresql.stado.parser.core.syntaxtree;

/**
 * Grammar production:
 * f0 -> <CREATE_>
 * f1 -> [ <UNIQUE_> ]
 * f2 -> <INDEX_>
 * f3 -> Identifier(prn)
 * f4 -> <ON_>
 * f5 -> TableName(prn)
 * f6 -> [ <USING_> Identifier(prn) ]
 * f7 -> <PARENTHESIS_START_>
 * f8 -> columnListIndexSpec(prn)
 * f9 -> <PARENTHESIS_CLOSE_>
 * f10 -> [ tablespaceDef(prn) ]
 * f11 -> [ WhereClause(prn) ]
 */
public class createIndex implements Node {
   public NodeToken f0;
   public NodeOptional f1;
   public NodeToken f2;
   public Identifier f3;
   public NodeToken f4;
   public TableName f5;
   public NodeOptional f6;
   public NodeToken f7;
   public columnListIndexSpec f8;
   public NodeToken f9;
   public NodeOptional f10;
   public NodeOptional f11;

   public createIndex(NodeToken n0, NodeOptional n1, NodeToken n2, Identifier n3, NodeToken n4, TableName n5, NodeOptional n6, NodeToken n7, columnListIndexSpec n8, NodeToken n9, NodeOptional n10, NodeOptional n11) {
      f0 = n0;
      f1 = n1;
      f2 = n2;
      f3 = n3;
      f4 = n4;
      f5 = n5;
      f6 = n6;
      f7 = n7;
      f8 = n8;
      f9 = n9;
      f10 = n10;
      f11 = n11;
   }

   public createIndex(NodeOptional n0, Identifier n1, TableName n2, NodeOptional n3, columnListIndexSpec n4, NodeOptional n5, NodeOptional n6) {
      f0 = new NodeToken("CREATE");
      f1 = n0;
      f2 = new NodeToken("INDEX");
      f3 = n1;
      f4 = new NodeToken("ON");
      f5 = n2;
      f6 = n3;
      f7 = new NodeToken("(");
      f8 = n4;
      f9 = new NodeToken(")");
      f10 = n5;
      f11 = n6;
   }

   public void accept(org.postgresql.stado.parser.core.visitor.Visitor v) {
      v.visit(this);
   }
   public Object accept(org.postgresql.stado.parser.core.visitor.ObjectVisitor v, Object argu) {
      return v.visit(this,argu);
   }
}
