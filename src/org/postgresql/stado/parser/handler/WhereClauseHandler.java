/*****************************************************************************
 * Copyright (C) 2008 EnterpriseDB Corporation.
 * Copyright (C) 2011 Stado Global Development Group.
 * Copyright (c) 2016 Regents of the University of Minnesota
 *
 * This file is part of the Minnesota Population Center's Terra Populus project.
 * For copyright and licensing information, see the NOTICE and LICENSE files
 * in this project's top-level directory, and also online at:
 * https://github.com/mnpopcenter/stado
 *
 * This file is part of Stado.
 *
 * Stado is free software: you can redistribute it and/or modify
 * it under the terms of the GNU General Public License as published by
 * the Free Software Foundation, either version 3 of the License, or
 * (at your option) any later version.
 *
 * Stado is distributed in the hope that it will be useful,
 * but WITHOUT ANY WARRANTY; without even the implied warranty of
 * MERCHANTABILITY or FITNESS FOR A PARTICULAR PURPOSE.  See the
 * GNU General Public License for more details.
 *
 * You should have received a copy of the GNU General Public License
 * along with Stado.  If not, see <http://www.gnu.org/licenses/>.
 *
 * You can find Stado at http://www.stado.us
 *
 ****************************************************************************/
package org.postgresql.stado.parser.handler;

import java.util.ArrayList;
import java.util.HashSet;
import java.util.Iterator;
import java.util.List;
import java.util.Vector;

import org.postgresql.stado.exception.XDBServerException;
import org.postgresql.stado.metadata.SysColumn;
import org.postgresql.stado.metadata.SysTable;
import org.postgresql.stado.optimizer.AttributeColumn;
import org.postgresql.stado.optimizer.QueryCondition;
import org.postgresql.stado.optimizer.QueryTree;
import org.postgresql.stado.optimizer.RelationNode;
import org.postgresql.stado.optimizer.SqlExpression;
import org.postgresql.stado.parser.Command;
import org.postgresql.stado.parser.core.syntaxtree.WhereClause;
import org.postgresql.stado.parser.core.visitor.ObjectDepthFirst;


/**
 * The WhereClauseHandler is responsible to take care of anything that is
 * between the Where Clause and the Group by Clause.
 */
public class WhereClauseHandler extends ObjectDepthFirst {

    short outerCounter = 0;

    QueryTree aQueryTree;

    Command commandToExecute;

    SysTable targetTable;

    /**
     *
     * @param commandToExecute
     */
    public WhereClauseHandler(Command commandToExecute) {
        this.commandToExecute = commandToExecute;
    }
    /**
     * Get the target Sys table.
     *
     * @return SysTable
     * @param tableName
     * @throws XDBServerException if table not found.
     */
   public SysTable getTargetTable(String tableName) throws XDBServerException {
       if(targetTable == null) {
           targetTable = commandToExecute.getClientContext().getSysDatabase().getSysTable(tableName);
       }
       return targetTable;
   }
    /**
     * f0 -> <WHERE_> f1 -> SQLComplexExpression(prn)
     * @param n
     * @param argu
     * @return
     */
    @Override
    public Object visit(WhereClause n, Object argu) {
        // TODO: Dummy Call, revisit to see if it can be removed safely
        n.f0.accept(this, argu);
        QueryConditionHandler qch = new QueryConditionHandler(commandToExecute);

        n.f1.accept(qch, argu);

        aQueryTree = (QueryTree) argu;

        if(qch.isHandleLeftOuterNeeded()) {
            handleLeftOuter(qch);
        } else {
            //clear lists that are no longer needed
            qch.queryCondList.clear();
            qch.innerQueryCondList.clear();
        }

        // the condition list should only be filled with
        // main conditions and that to only if they have a join in
        // and of there sub conditions.

        // -- I will there fore add another variable to the query tree --
        // will have the where root condition.

        // aQueryTree.conditionList.addAll(allConditions);
        aQueryTree
                .setWhereRootCondition(optimizeConditions(qch.aRootCondition));
        return null;
    }

    /**
     * @Return table Names associated with left outer/inner join
     * @Parameters Vector<QueryCondition> queryCondList
     */
    private Vector<String> getTableNames(Vector<QueryCondition> queryCondList) {
        Vector<String> tableNames = new Vector<String>();
        for (QueryCondition rootCond : queryCondList) {
            for (QueryCondition exprCond : QueryCondition.getNodes(rootCond, QueryCondition.QC_SQLEXPR)) {
                for (SqlExpression columnExpr : SqlExpression.getNodes(exprCond.getExpr(), SqlExpression.SQLEX_COLUMN)) {
                    AttributeColumn column = columnExpr.getColumn();
                    RelationNode relNode = column.relationNode;
                    if (relNode != null) {
                        tableNames.add(relNode.getTableName());
                    } else if (column.getTableName() != null && column.getTableName().length() > 0) {
                        tableNames.add(column.getTableName());
                    } else if (column.getTableAlias() != null && column.getTableAlias().length() > 0) {
                        tableNames.add(aQueryTree.getTableNameOfAlias(column.getTableAlias()));
                    }
                }
            }
        }
        return tableNames;
    }
    /**
     * handling for left outer(+)
     */
    private void handleLeftOuter(Object argu) {
        aQueryTree.setOuterJoin(true);
        QueryConditionHandler qch = (QueryConditionHandler)argu;
        try {
            //get table names associated with left outer(+)
            Vector<String> tableNames = getTableNames(qch.queryCondList);
            for(int i=0; i<qch.queryCondList.size(); i++) {
                QueryCondition qc = qch.queryCondList.get(i);
                aQueryTree.getFromClauseConditions().add(qc);
                Vector<QueryCondition> expressions = QueryCondition.getNodes(qc, QueryCondition.QC_SQLEXPR);
                Vector<SqlExpression> vCondExpr = new Vector<SqlExpression>();
                for (QueryCondition qcExpr : expressions) {
                    vCondExpr.add(qcExpr.getExpr());
                }
                QueryTreeHandler.analyzeAndCompleteColInfoAndNodeInfo(vCondExpr.iterator(),
                        aQueryTree.getRelationNodeList(), null, QueryTreeHandler.CONDITION,
                        commandToExecute.getaQueryTreeTracker(),
                        this.commandToExecute.getClientContext().getSysDatabase());
                HashSet<RelationNode> parents = new HashSet<RelationNode>();
                //last relation will be emp in (dept.deptno = emp.deptno)
                RelationNode lastRelation = aQueryTree.getRelHandlerInfo().get(tableNames.get(i));

                for (RelationNode relNode : aQueryTree.getRelationNodeList()) {
                    if (relNode == lastRelation) {
                        continue;
                    }
                    for (QueryCondition qcExpr : expressions) {
                        if (qcExpr.getExpr().contains(relNode)) {
                            parents.add(relNode);
                            break;
                        }
                    }
                }
                lastRelation.addParentNodes(parents, true, ++outerCounter);
            }

            /////////////////////////////////////////////////////////////////////////////
            ///////now handle inner join conditions if there exists some/////////////////
            /////////////////////////////////////////////////////////////////////////////
            if(qch.isHandleInnerJoinNeeded()) {
                // get table names assoicated with inner join
                tableNames = getTableNames(qch.innerQueryCondList);
                for(int i=0; i<qch.innerQueryCondList.size(); i++) {
                    QueryCondition qc = qch.innerQueryCondList.get(i);
                    aQueryTree.getFromClauseConditions().add(qc);
                    Vector<QueryCondition> expressions = QueryCondition.getNodes(qc, QueryCondition.QC_SQLEXPR);
                    Vector<SqlExpression> vCondExpr = new Vector<SqlExpression>();
                    for (QueryCondition qcExpr : expressions) {
                        vCondExpr.add(qcExpr.getExpr());
                    }
                    QueryTreeHandler.analyzeAndCompleteColInfoAndNodeInfo(vCondExpr.iterator(),
                            aQueryTree.getRelationNodeList(), null, QueryTreeHandler.CONDITION,
                            commandToExecute.getaQueryTreeTracker(),
                            this.commandToExecute.getClientContext().getSysDatabase());
                    //last relation will be emp in (dept.deptno = emp.deptno)
                    RelationNode lastRelation = aQueryTree.getRelHandlerInfo().get(tableNames.get(i));

                    for (RelationNode relNode : aQueryTree.getRelationNodeList()) {
                        if (relNode == lastRelation) {
                            continue;
                        }
                        for (QueryCondition qcExpr : expressions) {
                            if (qcExpr.getExpr().contains(relNode)) {
                                lastRelation.addSiblingJoin(relNode);
                            }
                        }
                    }
                }
            }
        } finally {
            qch.innerQueryCondList.clear();
            qch.queryCondList.clear();
        }
    }

    private QueryCondition optimizeConditions(QueryCondition aQueryCondition) {
        aQueryCondition.rebuildCondString();
        List<QueryCondition> anOrConditions = getAllConditions(aQueryCondition, "OR");
        if (anOrConditions.size() == 1
                && anOrConditions.get(0) == aQueryCondition) {
            return aQueryCondition;
        }
        List<QueryCondition> aCommonConditions = findCommonConditions(anOrConditions, "AND");
        QueryCondition resultCondition = buildNewCondition(anOrConditions,
                aCommonConditions);
        resultCondition.rebuildCondString();
        return resultCondition;
    }

    private SqlExpression getNotConstantExpr(QueryCondition aCond) {
        if (aCond.getLeftCond().getExpr().getExprType() == SqlExpression.SQLEX_CONSTANT) {
            return aCond.getRightCond().getExpr();
        }
        return aCond.getLeftCond().getExpr();
    }

    private boolean hasSameNode(QueryCondition aCond1, QueryCondition aCond2) {
        if (aCond1 == null || aCond2 == null
                || aCond1.getCondType() != QueryCondition.QC_RELOP
                || aCond2.getCondType() != QueryCondition.QC_RELOP) {
            return false;
        }
        if (aCond1.getLeftCond().getCondType() != QueryCondition.QC_SQLEXPR
                || aCond1.getRightCond().getCondType() != QueryCondition.QC_SQLEXPR
                || aCond2.getLeftCond().getCondType() != QueryCondition.QC_SQLEXPR
                || aCond2.getRightCond().getCondType() != QueryCondition.QC_SQLEXPR) {
            return false;
        }
        if (aCond1.getLeftCond().getExpr().getExprType() == SqlExpression.SQLEX_CONSTANT
                || aCond1.getRightCond().getExpr().getExprType() == SqlExpression.SQLEX_CONSTANT) {
            if (aCond2.getLeftCond().getExpr().getExprType() == SqlExpression.SQLEX_CONSTANT
                    || aCond2.getRightCond().getExpr().getExprType() == SqlExpression.SQLEX_CONSTANT) {
                SqlExpression expr1 = getNotConstantExpr(aCond1);
                SqlExpression expr2 = getNotConstantExpr(aCond2);
                if (expr1.getColumn().getTableAlias().endsWith(
                        expr2.getColumn().getTableAlias())) {
                    return true;
                }
            }
        }
        return false;
    }

    private List<QueryCondition> remakeConditionsForCommonNode(QueryCondition aCond,
            QueryCondition aCond1) {
        List<QueryCondition> aResultList = new ArrayList<QueryCondition>();
        List<QueryCondition> aWorktList = new ArrayList<QueryCondition>();

        List<QueryCondition> anAndCond = getAllConditions(aCond, "AND");
        List<QueryCondition> anAndCond1 = getAllConditions(aCond1, "AND");
        if (anAndCond.size() != anAndCond.size() || anAndCond.size() == 0
                || anAndCond1.size() == 0) {
            return null;
        }

        List<QueryCondition> anNewAndCond = new ArrayList<QueryCondition>();

        for (int i = 0; i < anAndCond.size(); i++) {
            aWorktList.clear();
            QueryCondition qc = anAndCond.get(i);
            for (int j = 0; j < anAndCond1.size(); j++) {
                QueryCondition qc1 = anAndCond1.get(j);
                if (hasSameNode(qc, qc1)) {
                    aWorktList.clear();
                    aWorktList.add(qc);
                    aWorktList.add(qc1);
                    anAndCond1.set(j, null);
                    break;
                }
            }
            if (aWorktList.size() != 0) {
                anNewAndCond.add(createOperatorConditions(aWorktList, "OR"));
            } else {
                anNewAndCond.add(qc);

            }
        }
        for (Iterator<QueryCondition> iter = anNewAndCond.iterator(); iter.hasNext();) {
            if (iter.next() == null) {
                iter.remove();
            }
        }
        aResultList.add(createOperatorConditions(anNewAndCond, "AND"));
        for (Iterator<QueryCondition> iter = anAndCond1.iterator(); iter.hasNext();) {
            if (iter.next() == null) {
                iter.remove();
            }
        }
        if (anAndCond1.size() == 0) {
            aResultList.add(createOperatorConditions(anAndCond1, "AND"));
        } else {
            return null;
        }
        return aResultList;
    }

    private QueryCondition remakeConditionsForCommonNode(List<QueryCondition> aListConditions) {
        List<QueryCondition> aResultList = new ArrayList<QueryCondition>();
        if (aListConditions == null || aListConditions.size() != 2) {
            return null;
        }
        aResultList = remakeConditionsForCommonNode(aListConditions.get(0),
                aListConditions.get(1));
        if (aResultList == null) {
            return null;
        }
        for (Iterator<QueryCondition> iter = aResultList.iterator(); iter.hasNext();) {
            if (iter.next() == null) {
                iter.remove();
            }
        }
        if (aResultList.size() != 1) {
            return null;
        }
        return aResultList.get(0);
    }

    /**
     * @param anOrConditions
     * @param commonConditions
     * @return
     */
    private QueryCondition buildNewCondition(List<QueryCondition> anOrConditions,
            List<QueryCondition> aCommonConditions) {
        QueryCondition aResultCondition = new QueryCondition();
        QueryCondition anAndCondition = createOperatorConditions(
                aCommonConditions, "AND");
        List<QueryCondition> aRestConditions = removeCommonConditions(anOrConditions,
                aCommonConditions);
        QueryCondition anORCondition = createOperatorConditions(
                aRestConditions, "OR");
        QueryCondition aCommonNodeCondition = remakeConditionsForCommonNode(aRestConditions);

        List<QueryCondition> workVector = new ArrayList<QueryCondition>();
        workVector.add(anAndCondition);
        workVector.add(anORCondition);
        workVector.add(aCommonNodeCondition);

        aResultCondition = createOperatorConditions(workVector, "AND");

        return aResultCondition;
    }

    private List<QueryCondition> removeCommonConditions(List<QueryCondition> anOrConditions,
            List<QueryCondition> aCommonConditions) {
        List<QueryCondition> result = new ArrayList<QueryCondition>();
        for (QueryCondition qc : anOrConditions) {
            QueryCondition aNewqc = removeConditions(qc, aCommonConditions);
            if (aNewqc != null) {
                aNewqc.rebuildCondString();
                result.add(aNewqc);
            }
        }
        return result;
    }

    private QueryCondition removeConditions(QueryCondition aCondition,
            List<QueryCondition> aCommonConditions) {

        List<QueryCondition> anAndConditions = getAllConditions(aCondition, "AND");
        List<QueryCondition> aNewAndConditions = new ArrayList<QueryCondition>();
        for (QueryCondition anAndQc : anAndConditions) {
            boolean flag = false;
            for (QueryCondition aCommonQc : aCommonConditions) {
                if (isConditionsSimilar(anAndQc, aCommonQc)) {
                    flag = true;
                    break;
                }

            }
            if (!flag) {
                aNewAndConditions.add(anAndQc);
            }

        }

        QueryCondition anAndCondition = createOperatorConditions(
                aNewAndConditions, "AND");
        return anAndCondition;
    }

    private QueryCondition createOperatorConditions(List<QueryCondition> aCommonConditions,
            String op) {
        for (Iterator<QueryCondition> iter = aCommonConditions.iterator(); iter.hasNext();) {
            if (iter.next() == null) {
                iter.remove();
            }
        }
        QueryCondition qc = new QueryCondition();
        ArrayList<QueryCondition> aCopyCommonConditions = new ArrayList<QueryCondition>();
        aCopyCommonConditions.addAll(aCommonConditions);
        if (aCopyCommonConditions == null || aCopyCommonConditions.size() == 0) {
            return null;
        }
        if (aCopyCommonConditions.size() == 1) {
            if (aCopyCommonConditions.get(0) == null) {
                return null;
            }
            return aCopyCommonConditions.get(0);
        }
        if (aCopyCommonConditions.get(1) == null) {
            return null;
        }

        qc.setCondType(QueryCondition.QC_CONDITION);
        qc.setOperator(op);
        qc.setLeftCond(aCopyCommonConditions.get(0));
        aCopyCommonConditions.remove(0);
        qc.setRightCond(createOperatorConditions(aCopyCommonConditions, op));
        qc.rebuildCondString();
        return qc;

    }

    private List<QueryCondition> findCommonConditions(List<QueryCondition> anOrConditions, String op) {
        List<QueryCondition> result = getAllConditions(anOrConditions.get(0), op);
        for (int i = 1; i < anOrConditions.size(); i++) {
            QueryCondition qc = anOrConditions.get(i);
            result = findCommonConditions(result, getAllConditions(qc, op));
        }
        return result;
    }

    private List<QueryCondition> findCommonConditions(List<QueryCondition> aCond1, List<QueryCondition> aCond2) {
        List<QueryCondition> result = new ArrayList<QueryCondition>();
        for (QueryCondition qc1 : aCond1) {
            for (QueryCondition qc2 : aCond2) {
                if (isConditionsSimilar(qc1, qc2)) {
                    result.add(qc1);
                }
            }

        }

        return result;
    }

    private boolean isConditionsSimilar(QueryCondition qc1, QueryCondition qc2) {
        String aStr1 = qc1.rebuildString().trim().toUpperCase();
        String aStr2 = qc2.rebuildString().trim().toUpperCase();
        if (aStr1.equals(aStr2)) {
            return true;
        }
        return false;
    }

    private List<QueryCondition> getAllConditions(QueryCondition aQueryCondition, String op) {
        ArrayList<QueryCondition> result = new ArrayList<QueryCondition>();
        if (aQueryCondition.getCondType() == QueryCondition.QC_CONDITION
                && aQueryCondition.getOperator().compareTo(op) == 0) {
            result.addAll(getAllConditions(aQueryCondition.getLeftCond(), op));
            result.addAll(getAllConditions(aQueryCondition.getRightCond(), op));
        } else {
            result.add(aQueryCondition);
        }

        return result;
    }

}
