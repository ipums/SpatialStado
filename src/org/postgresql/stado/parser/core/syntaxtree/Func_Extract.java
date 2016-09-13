//
// Generated by JTB 1.2.2
//

package org.postgresql.stado.parser.core.syntaxtree;

/**
 * Grammar production:
 * f0 -> <EXTRACT_>
 * f1 -> <PARENTHESIS_START_>
 * f2 -> ( <YEAR_FROM> | <QUARTER_FROM> | <MONTH_FROM> | <WEEK_FROM> | <DAY_FROM> | <HOUR_FROM> | <MINUTE_FROM> | <SECOND_FROM> | <DOY_FROM> | <DOW_FROM> | <DECADE_FROM> | <CENTURY_FROM> | <MILLISECOND_FROM> | <MILLENNIUM_FROM> | <MICROSECONDS_FROM> | <EPOCH_FROM> )
 * f3 -> SQLArgument(prn)
 * f4 -> <PARENTHESIS_CLOSE_>
 */
public class Func_Extract implements Node {
   public NodeToken f0;
   public NodeToken f1;
   public NodeChoice f2;
   public SQLArgument f3;
   public NodeToken f4;

   public Func_Extract(NodeToken n0, NodeToken n1, NodeChoice n2, SQLArgument n3, NodeToken n4) {
      f0 = n0;
      f1 = n1;
      f2 = n2;
      f3 = n3;
      f4 = n4;
   }

   public Func_Extract(NodeChoice n0, SQLArgument n1) {
      f0 = new NodeToken("EXTRACT");
      f1 = new NodeToken("(");
      f2 = n0;
      f3 = n1;
      f4 = new NodeToken(")");
   }

   public void accept(org.postgresql.stado.parser.core.visitor.Visitor v) {
      v.visit(this);
   }
   public Object accept(org.postgresql.stado.parser.core.visitor.ObjectVisitor v, Object argu) {
      return v.visit(this,argu);
   }
}

