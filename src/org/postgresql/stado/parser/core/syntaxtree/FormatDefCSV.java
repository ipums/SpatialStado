//
// Generated by JTB 1.2.2
//

package org.postgresql.stado.parser.core.syntaxtree;

/**
 * Grammar production:
 * f0 -> <CSV_>
 * f1 -> ( <QUOTE_STRING_> [ <AS_> ] <STRING_LITERAL> | <ESCAPE_> [ <AS_> ] <STRING_LITERAL> | <FORCE_QUOTE_> ColumnNameList(prn) | <FORCE_NOT_NULL_> ColumnNameList(prn) )*
 */
public class FormatDefCSV implements Node {
   public NodeToken f0;
   public NodeListOptional f1;

   public FormatDefCSV(NodeToken n0, NodeListOptional n1) {
      f0 = n0;
      f1 = n1;
   }

   public FormatDefCSV(NodeListOptional n0) {
      f0 = new NodeToken("CSV");
      f1 = n0;
   }

   public void accept(org.postgresql.stado.parser.core.visitor.Visitor v) {
      v.visit(this);
   }
   public Object accept(org.postgresql.stado.parser.core.visitor.ObjectVisitor v, Object argu) {
      return v.visit(this,argu);
   }
}

