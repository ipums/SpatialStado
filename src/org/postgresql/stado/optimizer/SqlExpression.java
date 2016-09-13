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
package org.postgresql.stado.optimizer;

import java.math.BigDecimal;
import java.sql.Date;
import java.sql.Time;
import java.sql.Timestamp;
import java.sql.Types;
import java.util.ArrayList;
import java.util.Collection;
import java.util.HashMap;
import java.util.HashSet;
import java.util.Hashtable;
import java.util.LinkedList;
import java.util.List;
import java.util.Map;
import java.util.StringTokenizer;
import java.util.Vector;

import org.postgresql.stado.common.util.ParseCmdLine;
import org.postgresql.stado.common.util.Property;
import org.postgresql.stado.common.util.Props;
import org.postgresql.stado.common.util.XLogger;
import org.postgresql.stado.engine.XDBSessionContext;
import org.postgresql.stado.exception.ColumnNotFoundException;
import org.postgresql.stado.exception.ErrorMessageRepository;
import org.postgresql.stado.exception.XDBServerException;
import org.postgresql.stado.metadata.SysColumn;
import org.postgresql.stado.metadata.SysDatabase;
import org.postgresql.stado.parser.ExprTypeHelper;
import org.postgresql.stado.parser.ExpressionType;
import org.postgresql.stado.parser.handler.CastTemplates;
import org.postgresql.stado.parser.handler.DataTypeHandler;
import org.postgresql.stado.parser.handler.IFunctionID;
import org.postgresql.stado.parser.handler.IdentifierHandler;

/**
 * 
 * SqlExpression holds SQL expression elements, whether a column, constant, or
 * compound expression. This should be changed in the future with a base
 * SqlExpression class and inherited subclasses based on the particular
 * expression type.
 * 
 * For leftExpr and rightExpr, it is intended to be processed left-depth first.
 * 
 */
public class SqlExpression implements IRebuildString {
	private static final XLogger logger = XLogger
			.getLogger(SqlExpression.class);

	/**
	 * for time and timestamps, precision for subseconds. Ingres does not
	 * support subseconds.
	 */
	private static final String subsecondBaseString = "000000000";

	private static final int CONSTANT_DOUBLE_SCALE_VALUE = 10;

	private static final int CONSTANT_PRECISION_DOUBLE_VALUE = 32;

	// Type Mapping constants (see mappedExpression)
	public static final int INTERNALMAPPING = 1;

	public static final int EXTERNALMAPPING = 2;

	public static final int ORIGINAL = 3;

	// enumerate types here
	public static final int SQLEX_CONSTANT = 2;

	public static final int SQLEX_COLUMN = 4;

	public static final int SQLEX_UNARY_EXPRESSION = 8;

	public static final int SQLEX_OPERATOR_EXPRESSION = 16;

	public static final int SQLEX_FUNCTION = 32;

	public static final int SQLEX_SUBQUERY = 64;

	public static final int SQLEX_CONDITION = 128;

	public static final int SQLEX_CASE = 256;

	public static final int SQLEX_LIST = 512;

	public static final int SQLEX_PARAMETER = 1024;

	public static final int SQLEX_COLUMNLIST = 512;

	// To get all SqlExpression
	public static final int SQLEX_ALL = SQLEX_CONSTANT | SQLEX_COLUMN
			| SQLEX_UNARY_EXPRESSION | SQLEX_OPERATOR_EXPRESSION
			| SQLEX_FUNCTION | SQLEX_CONDITION | SQLEX_CASE | SQLEX_COLUMNLIST
			| SQLEX_LIST;

	/** Which SQLEX_* type this is */
	private int exprType;

	private ExpressionType exprDataType;

	/** String representation of expression */
	private String exprString = "";
	
	//sray
	/** String representation of original expression */
	private String exprStringOrigTableAndCol = null;

	/** This is set after the expression have been produced */
	private QueryTree belongsToTree;

	/**
	 * This is used in case the top level expression is aliased for purposes in
	 * subquerying, or display
	 */
	private String alias = "";

	/** Another member added here for help with handling aggregates */
	private String aggAlias = "";

	/** Additional alias used in outer handling */
	private String outerAlias = "";

	/** This is for labelling in the final ResultSet */
	private String projectionLabel = null;

	/** For constants */
	private String constantValue = "";

	/** For columns */
	private AttributeColumn column;

	/** For complex expressions */
	private SqlExpression leftExpr;

	private SqlExpression rightExpr;

	/** can be NOT */
	private String unaryOperator = "";

	/**
	 * This is for support of having query conditions inside sql expression
	 * allowing for a*b + ( A ==B ) there fore if A = 1, B= 1 and a = 1 , b =1
	 * The resultant must be 3.
	 */
	private QueryCondition aQueryCondition = null;

	/**
	 * this is required in context of !!, ||/, |/ and @ unary operators where
	 * operand may have sign e.g. !! -4 or !!(-4)
	 */
	private String operandSign = "";

	/** +, *, -, etc. */
	private String operator = "";

	/** For mapped expression - reference to another SqlExpression */
	private int mapped = ORIGINAL;

	private SqlExpression mappedExpression = null;

	// For prepared statements
	/** track the parameter number */
	private int paramNumber;

	/** holds PreparedStatement parameter value */
	private String paramValue = null;

	/** For functions */
	private int functionId;

	/** Function name */
	private String functionName = "";

	/** Parameters that this function uses */
	private List<SqlExpression> functionParams = new ArrayList<SqlExpression>();

	private String argSeparator = ", ";

	private boolean needParenthesisInFunction = true;

	// For CAST
	private DataTypeHandler expTypeOfCast;

	// For aggregate functions
	/**
	 * if this expression is a distinct group, like count(distinct x)
	 */
	private boolean isDistinctGroupFunction = false;

	/**
	 * If we need to break this aggregate function into two steps where we just
	 * get column info on the first
	 */
	private boolean isDeferredGroup = false;

	private boolean isAllCountGroupFunction = false;

	/** Used to help with SELECT COUNT (DISTINCT x) ... */
	private boolean isDistinctExtraGroup = false;

	/** For CASE */
	private SCase caseConstruct = new SCase();

	/** For subquery */
	private QueryTree subqueryTree;

	// Misc
	// Do not rebuild expression string if this flag is set
	private boolean isTemporaryExpression = false;

	/**
	 * This variable is only valid when we have this expression as a subtree All
	 * subtrees are contained in a relation node as they represent a relation
	 */
	private RelationNode parentContainerNode;

	/**
	 * This is added here for convenience instead of creating a wrapper class
	 * for projection list items. It is used to indicate that the item was not
	 * originally in the list, but added because it appears in the order by or
	 * having clause
	 */
	private boolean isAdded = false;

	/**
	 * This vector was added to provide support for the list of elements for
	 * e.g. the list (1,2,3) - This is also an expression but of a diffrent
	 * nature we cannot do operation on this SQLExpression
	 */
	private List<SqlExpression> expressionList = new ArrayList<SqlExpression>();

	/**
	 * 
	 * 
	 * 
	 * @return
	 * 
	 */

	public QueryTree getBelongsToTree() {
		return belongsToTree;
	}

	
	//sray
	public  String getExprStringOrigTableAndCol() {
		
		if (exprStringOrigTableAndCol != null) {
			return exprStringOrigTableAndCol;
		}
		else {
			if (this.getExprType() != SQLEX_OPERATOR_EXPRESSION) {
		
				if (this.getExprType() == SQLEX_COLUMN) {
					AttributeColumn anAC = this.getColumn();
					
					// sray: for a case where all columns are named as the same in the query
					if (!anAC.getTableAlias().equals(""))  // when you have a self join, both table names are the same. so use table alias instead
						exprStringOrigTableAndCol = IdentifierHandler.quote(anAC
								.getTableAlias());
					else
						exprStringOrigTableAndCol = IdentifierHandler.quote(anAC
							.getTableName());
					
					if (this.getColumn().getTableName().length() > 0) {
						exprStringOrigTableAndCol += "."
								+ IdentifierHandler.quote(this
										.getColumn().columnName);		
					} 
				}
			}
			return exprStringOrigTableAndCol;
		}
		
	}
	
	
	/**
	 * 
	 * 
	 * 
	 * @param belongsToTree
	 * 
	 */

	public void setBelongsToTree(QueryTree belongsToTree) {
		this.belongsToTree = belongsToTree;
		// Also get the columns in this SQL Expression and set them to
		// this tree.
		if (this.subqueryTree != null) {
			// In this case we dont do anything
			// as while analyzing the sql expressions we have already
			// set the tree.
		} else {
			for (SqlExpression aSqlExpression : getNodes(this, SQLEX_COLUMN)) {
				aSqlExpression.getColumn().setMyParentTree(belongsToTree);
			}

		}
	}

	/**
	 * 
	 * Set the Expression to temporary
	 * 
	 * @param isTemp
	 * 
	 */

	public void setTempExpr(boolean isTemp) {
		isTemporaryExpression = isTemp;
	}

	/**
	 * 
	 * 
	 * 
	 * @return
	 * 
	 */

	public boolean isTempExpr() {
		return isTemporaryExpression;
	}

	/**
	 * 
	 * 
	 * 
	 * @return
	 * 
	 * @param sqlexpr
	 * 
	 * @param nodetype
	 * 
	 */

	public static Vector<SqlExpression> getNodes(SqlExpression sqlexpr,
			int nodetype) {
		return getNodes(sqlexpr, nodetype, null);
	}

	/**
	 * This will return us the list of column nodes which belong to this Sql
	 * Expression
	 * 
	 * 
	 * @return
	 * 
	 * @param sqlexpr
	 * 
	 * @param nodetype
	 * 
	 * @param hsVisited
	 * 
	 */

	private static Vector<SqlExpression> getNodes(SqlExpression sqlexpr,
			int nodetype, HashSet<SqlExpression> hsVisited) {

		if (hsVisited == null) {
			hsVisited = new HashSet<SqlExpression>();
		}

		// See if we have already processed this one
		if (hsVisited.contains(sqlexpr)) {
			// return an empty vector to avoid null exception
			return new Vector<SqlExpression>();
		}

		hsVisited.add(sqlexpr);

		// Get all Nodes of a particular type
		Vector<SqlExpression> columnnodes = new Vector<SqlExpression>();

		SqlExpression sqlr = sqlexpr.rightExpr;
		SqlExpression sqll = sqlexpr.leftExpr;

		/* Get all the nodes from the right */
		if (sqlr != null) {
			columnnodes.addAll(getNodes(sqlr, nodetype, hsVisited));
		}

		/* Get all the nodes from the left */
		if (sqll != null) {
			columnnodes.addAll(getNodes(sqll, nodetype, hsVisited));
		}

		/*
		 * Check if the expression we are evaluation is a tree - in which case
		 * we will assume that this is a sub tree and we now have to return not
		 * the actual expression columns but the projected columns
		 */
		if ((sqlexpr.getExprType() & SQLEX_SUBQUERY) > 0) {

			// We have 4 different types of subqueries
			// a. Scalar - Correlated
			// b. Scalar - non corelated
			// a.NonScalar - Correlated
			// b.NonScalar - Non Correlated
			// The parent container node is the feature of only nonscalar
			// queries
			// since the implementation of the scalar correlated is not yet
			// decided we are
			// not sure how that will be taken care off.

			// Comment End
			// If it is not a scalar query
			if ((sqlexpr.subqueryTree.getQueryType() & QueryTree.SCALAR) == 0) {
				for (SqlExpression aSqlExpression : sqlexpr.parentContainerNode
						.getProjectionList()) {
					if (hsVisited == null
							|| !hsVisited.contains(aSqlExpression)) {
						columnnodes.addAll(getNodes(aSqlExpression, nodetype,
								hsVisited));
					}
				}
			}

			if ((sqlexpr.subqueryTree.getQueryType() & QueryTree.CORRELATED) > 0
					&& sqlexpr.subqueryTree != null) {
				for (QueryCondition aQC : sqlexpr.subqueryTree
						.getConditionList()) {
					for (QueryCondition aQCSE : QueryCondition.getNodes(aQC,
							QueryCondition.QC_SQLEXPR)) {
						columnnodes.addAll(getNodes(aQCSE.getExpr(), nodetype,
								hsVisited));
					}
				}
			}
		}

		/*
		 * Check if the expression we are evaluting is a function -- in this
		 * case we will also explore the Function params for which we are
		 * looking
		 */
		if ((sqlexpr.getExprType() & SQLEX_FUNCTION) > 0) {
			for (SqlExpression aSqlExpression : sqlexpr.functionParams) {
				columnnodes
						.addAll(getNodes(aSqlExpression, nodetype, hsVisited));
			}
		}

		/*
		 * When we have the expression of type CASE
		 */
		if ((sqlexpr.getExprType() & SQLEX_CASE) > 0) {
			// - This should contain the SQL Expressions from both
			// query conditions as well as sql expressions
			for (SqlExpression aSqlExpression : sqlexpr.getCaseConstruct()
					.getSQLExpressions()) {
				columnnodes.addAll(SqlExpression.getNodes(aSqlExpression,
						nodetype, hsVisited));
			}
		}

		/* Check if this node is of type we are expecting */
		if ((sqlexpr.getExprType() & nodetype) > 0) {
			columnnodes.add(sqlexpr);
		}

		return columnnodes;
	}

	/**
	 * 
	 * Constructor
	 * 
	 * @param aQueryTree
	 * 
	 */

	public SqlExpression(QueryTree aQueryTree) {
		this.setExprType(SQLEX_SUBQUERY);
		this.subqueryTree = aQueryTree;
	}

	public SqlExpression() {

	}

	/**
	 * Create constant SqlExpression with specified value and data type
	 * 
	 * @param constantValue
	 * @param dataType
	 */
	public SqlExpression(String constantValue, ExpressionType dataType) {
		exprType = SQLEX_CONSTANT;
		this.constantValue = constantValue;
		exprString = constantValue;
		exprDataType = dataType;
	}

	/**
	 * Copy a SqlExpression to another. Note that it just copies references.
	 * 
	 * 
	 * @param orignal
	 * 
	 * @param overwrite
	 * 
	 * @return
	 * 
	 */

	public static SqlExpression copy(SqlExpression orignal,
			SqlExpression overwrite) {

		overwrite.exprType = orignal.exprType;
		overwrite.exprString = orignal.exprString;
		overwrite.unaryOperator = orignal.unaryOperator;
		overwrite.operator = orignal.operator;
		overwrite.operandSign = orignal.operandSign;
		overwrite.constantValue = orignal.constantValue;
		overwrite.column = orignal.column;
		overwrite.functionId = orignal.functionId;
		overwrite.functionName = orignal.functionName;
		overwrite.functionParams = orignal.functionParams;
		overwrite.expTypeOfCast = orignal.expTypeOfCast;
		overwrite.needParenthesisInFunction = orignal.needParenthesisInFunction;
		overwrite.argSeparator = orignal.argSeparator;
		overwrite.alias = orignal.alias;
		overwrite.outerAlias = orignal.outerAlias;
		overwrite.leftExpr = orignal.leftExpr;
		overwrite.rightExpr = orignal.rightExpr;
		overwrite.exprDataType = orignal.exprDataType;
		overwrite.aQueryCondition = orignal.aQueryCondition;
		overwrite.isDistinctGroupFunction = orignal.isDistinctGroupFunction;
		overwrite.isDeferredGroup = orignal.isDeferredGroup;
		overwrite.isAllCountGroupFunction = orignal.isAllCountGroupFunction;
		overwrite.caseConstruct = orignal.caseConstruct;
		overwrite.subqueryTree = orignal.subqueryTree;
		overwrite.projectionLabel = orignal.projectionLabel;
		overwrite.paramNumber = orignal.paramNumber;
		overwrite.paramValue = orignal.paramValue;
		return overwrite;
	}

	/**
	 * Returns a copy of this SqlExpression
	 * 
	 * @return
	 * 
	 */

	public SqlExpression copy() {
		return copy(this, new SqlExpression());
	}

	// Rebuild exprString
	public void rebuildExpression() {
		rebuildExpression(null);
	}

	// Rebuild exprString
	private void rebuildExpression(XDBSessionContext client) {
		//
		if (unaryOperator.equals("+")) {
			unaryOperator = "";
			this.exprString = rebuildExpression(this, client);
		} else if (unaryOperator.equals("")) {
			this.exprString = rebuildExpression(this, client);
		} else if (this.getExprType() == SQLEX_PARAMETER) {
			rebuildExpression(this, client);
		} else if (unaryOperator.equals("|/")) {
			this.exprString = "|/ " + operandSign
					+ rebuildExpression(this, client);
		} else if (unaryOperator.equals("||/")) {
			this.exprString = "||/ " + operandSign
					+ rebuildExpression(this, client);
		} else if (unaryOperator.equals("!")) {
			this.exprString = operandSign + rebuildExpression(this, client)
					+ " !";
		} else if (unaryOperator.equals("!!")) {
			this.exprString = "!! " + operandSign
					+ rebuildExpression(this, client);
		} else if (unaryOperator.equals("@")) {
			this.exprString = "@ " + operandSign
					+ rebuildExpression(this, client);
		} else if (unaryOperator.equals("~")) {
			this.exprString = "~ " + rebuildExpression(this, client);
		} else {
			this.exprString = "-" + rebuildExpression(this, client);
		}
	}
	
	//sray
	public void rebuildExpressionWithColumnAlias() {
		AttributeColumn anAC;
		String newExprString = "";
		if (this.getExprType() != SQLEX_OPERATOR_EXPRESSION) {
			if (this.getExprType() == SQLEX_COLUMN) {
				anAC = this.getColumn();

				if (anAC.relationNode != null) {
					if (anAC.relationNode.getCurrentTempTableName()
							.length() > 0) {
						newExprString = IdentifierHandler
								.quote(anAC.relationNode
										.getCurrentTempTableName());
					} else if (!anAC.relationNode.getAlias().equals("")) {
						newExprString = IdentifierHandler
								.quote(anAC.relationNode.getAlias());
					} else {
						if (anAC.getTableAlias().length() > 0) {
							newExprString = IdentifierHandler.quote(anAC
									.getTableAlias());
						} else {
							newExprString = IdentifierHandler.quote(anAC
									.getTableName());
						}
					}
				} else {
					if (anAC.getTableAlias() != null
							&& anAC.getTableAlias().length() > 0) {
						newExprString = IdentifierHandler.quote(anAC
								.getTableAlias());
					}
				}


				if (this.column.relationNode != null &&
						this.column.relationNode.getCurrentTempTableName().length() > 0 && this.alias != null &&
								this.alias.length() > 0) { 
						 newExprString +=  "." + this.alias; 
				 }

			}
		}
		this.exprString = newExprString;
	}
	

	/**
	 * Use recursion to get all of the elements
	 * 
	 * 
	 * @param aSqlExpression
	 * 
	 * @return
	 * 
	 */

	private static String rebuildExpression(SqlExpression aSqlExpression,
			XDBSessionContext client) {
		String rightExprString = "";
		String leftExprString = "";
		String newExprString = "";
		AttributeColumn anAC;

		if (aSqlExpression.isTemporaryExpression == false) {

			if (aSqlExpression.getExprType() != SQLEX_OPERATOR_EXPRESSION) {
				if (aSqlExpression.getExprType() == SQLEX_COLUMN) {
					anAC = aSqlExpression.getColumn();

					if (anAC.relationNode != null) {
						if (anAC.relationNode.getCurrentTempTableName()
								.length() > 0) {
							newExprString = IdentifierHandler
									.quote(anAC.relationNode
											.getCurrentTempTableName());
						} else if (!anAC.relationNode.getAlias().equals("")) {
							newExprString = IdentifierHandler
									.quote(anAC.relationNode.getAlias());
						} else {
							if (anAC.getTableAlias().length() > 0) {
								newExprString = IdentifierHandler.quote(anAC
										.getTableAlias());
							} else {
								newExprString = IdentifierHandler.quote(anAC
										.getTableName());
							}
						}
					} else {
						if (anAC.getTableAlias() != null
								&& anAC.getTableAlias().length() > 0) {
							newExprString = IdentifierHandler.quote(anAC
									.getTableAlias());
						}
					}

					// Use tempColumnAlias, if assigned
					if (aSqlExpression.getColumn().tempColumnAlias.length() > 0) {
						newExprString += "."
								+ IdentifierHandler.quote(aSqlExpression
										.getColumn().tempColumnAlias);
					} else if (aSqlExpression.getColumn().columnAlias != null
							&& aSqlExpression.getColumn().columnAlias.length() > 0) {

						if (aSqlExpression.getColumn().getTableName().length() > 0) {
							newExprString += "."
									+ IdentifierHandler.quote(aSqlExpression
											.getColumn().columnAlias);
						} else {
							newExprString += IdentifierHandler
									.quote(aSqlExpression.getColumn().columnAlias);
						}
					} else {
						/*
						// If we are using a temp table and the expression has
						// been aliased, use the alias
						// sray: was commented out. uncommented
						 if (aSqlExpression.column.relationNode != null &&
								 aSqlExpression.column.relationNode.getCurrentTempTableName().length() > 0 && aSqlExpression.alias != null &&
								 aSqlExpression.alias.length() > 0) { 
							 newExprString +=  "." + aSqlExpression.alias; 
						 } else
						*/ 
						if (aSqlExpression.getColumn().getTableName().length() > 0) {
							newExprString += "."
									+ IdentifierHandler.quote(aSqlExpression
											.getColumn().columnName);
						} else {
							if (newExprString == null
									|| newExprString.equals("")) {
								newExprString += IdentifierHandler
										.quote(aSqlExpression.getColumn().columnName);
							} else {
								newExprString += "."
										+ IdentifierHandler
												.quote(aSqlExpression
														.getColumn().columnName);
							}
						}
						
					}
					
					
					
				} else if (aSqlExpression.getExprType() == SQLEX_FUNCTION
						&& aSqlExpression.isTemporaryExpression == false) {
					// Special handling for some functions needs to be executed
					// on coordinator
					switch (aSqlExpression.functionId) {
					case IFunctionID.CURRENTDATE_ID:
						return "date '" + new Date(System.currentTimeMillis())
								+ "'";
					case IFunctionID.CURRENTTIME_ID:
						return "time '" + new Time(System.currentTimeMillis())
								+ "'";
					case IFunctionID.CURRENTTIMESTAMP_ID:
						return "timestamp '"
								+ new Timestamp(System.currentTimeMillis())
								+ "'";
					case IFunctionID.CURRENTDATABASE_ID:
						return client == null ? aSqlExpression.constantValue
								: "'"
										+ client.getSysDatabase().getDbname()
												.replaceAll("'", "''") + "'";
					case IFunctionID.CURRENTUSER_ID:
						return client == null ? aSqlExpression.constantValue
								: "'"
										+ client.getCurrentUser().getName()
												.replaceAll("'", "''") + "'";
					case IFunctionID.VERSION_ID:
						return "'"
								+ Props.DISPLAY_SERVER_VERSION.replaceAll("'",
										"''") + "'";
					}
					newExprString = Property.get("xdb.sqlfunction."
							+ aSqlExpression.functionName + ".template");
					if (newExprString == null) {
						newExprString = aSqlExpression.functionName;
						if (aSqlExpression.functionId == IFunctionID.CAST_ID) {
							int theFromType;

							SqlExpression theFuncParam = aSqlExpression.functionParams
									.get(0);
							theFuncParam.rebuildExpression();
							if (theFuncParam.getExprDataType() == null) {
								return theFuncParam.getConstantValue();
							}
							if (theFuncParam.getExprType() == SqlExpression.SQLEX_COLUMN) {
								theFromType = theFuncParam.getColumn().columnType.type;
							} else {
								theFromType = theFuncParam.getExprDataType().type;
							}

							newExprString = CastTemplates.getTemplate(
									theFromType,
									aSqlExpression.expTypeOfCast.getSqlType());
							if (newExprString == null
									|| newExprString.trim().equals("")) {
								throw new XDBServerException(
										"Can not cast those datatypes.");
							}
							HashMap<String, String> arguments = new HashMap<String, String>();
							int count = 0;
							for (SqlExpression sqlexpr : aSqlExpression.functionParams) {
								count++;
								sqlexpr.rebuildExpression();
								arguments.put("arg", sqlexpr.exprString);
								arguments.put("type",
										aSqlExpression.expTypeOfCast
												.getTypeString());
							}
							newExprString = ParseCmdLine.substitute(
									newExprString, arguments);

						} else if (aSqlExpression.functionId != IFunctionID.COUNT_STAR_ID) {
							if (aSqlExpression.needParenthesisInFunction) {
								newExprString = newExprString + "( ";
							} else {
								newExprString = newExprString + " ";
							}

							if (FunctionAnalysis
									.isGroupFunction(aSqlExpression.functionId)
									&& aSqlExpression.isDistinctGroupFunction) {
								newExprString = newExprString + "DISTINCT ";
							}

							if (FunctionAnalysis
									.isGroupFunction(aSqlExpression.functionId)
									&& aSqlExpression.isAllCountGroupFunction) {
								newExprString = newExprString + "ALL ";
							}

							int count = 0;
							for (SqlExpression sqlexpr : aSqlExpression.functionParams) {
								if (count != 0) {
									newExprString = newExprString
											+ aSqlExpression.getArgSeparator();
								}
								count = 1;
								sqlexpr.rebuildExpression();
								newExprString = newExprString
										+ sqlexpr.exprString;
							}

							if (aSqlExpression.needParenthesisInFunction) {
								newExprString = newExprString + ") ";
							} else {
								newExprString = newExprString + " ";
							}
						} else {
							if (newExprString == null) {
								newExprString = aSqlExpression.exprString;
							}
						}
					} else {
						HashMap<String, String> arguments = new HashMap<String, String>();
						int count = 0;
						for (SqlExpression sqlexpr : aSqlExpression.functionParams) {
							count++;
							sqlexpr.rebuildExpression();
							arguments.put("arg" + count, sqlexpr.exprString);
						}
						newExprString = ParseCmdLine.substitute(newExprString,
								arguments);
					}

				} else if (aSqlExpression.getExprType() == SqlExpression.SQLEX_CASE) {
					newExprString = aSqlExpression.getCaseConstruct()
							.rebuildString();
				} else if (aSqlExpression.getExprType() == SqlExpression.SQLEX_CONSTANT) {
					if (Props.XDB_STRIP_INTERVAL_QUOTES
							&& aSqlExpression.getConstantValue() != null
							&& aSqlExpression.getConstantValue().toLowerCase()
									.startsWith("interval")) {
						newExprString = aSqlExpression.getConstantValue()
								.replaceAll("'", "");
					} else {
						newExprString = aSqlExpression.getConstantValue();
					}
				} else if (aSqlExpression.getExprType() == SqlExpression.SQLEX_SUBQUERY) {
					newExprString = "("
							+ aSqlExpression.subqueryTree.rebuildString() + ")";
				} else if (aSqlExpression.getExprType() == SqlExpression.SQLEX_LIST) {
					boolean firstTime = true;
					for (SqlExpression aSqlExpressionItem : aSqlExpression.expressionList) {
						if (firstTime) {
							newExprString += "( "
									+ aSqlExpressionItem.rebuildString();
							firstTime = false;
						} else {
							newExprString += " , "
									+ aSqlExpressionItem.rebuildString();
						}

					}
					newExprString += " )";
				} else {
					newExprString = aSqlExpression.exprString;
				}
			} else {
				// Always do left side first
				if (aSqlExpression.leftExpr != null) {
					aSqlExpression.leftExpr.rebuildExpression();
					leftExprString = aSqlExpression.leftExpr.exprString;
					aSqlExpression.rightExpr.rebuildExpression();
					rightExprString = aSqlExpression.rightExpr.exprString;

					if (aSqlExpression.operator.compareTo("\\") == 0) {
						aSqlExpression.operator = "/";
					}

					newExprString = "(" + leftExprString + " "
							+ aSqlExpression.operator + " " + rightExprString
							+ ")";
				}
			}
		} else {
			// if a temporary column, quote it if not quoted
			if (aSqlExpression.exprType == SqlExpression.SQLEX_COLUMN
					&& aSqlExpression.exprString != null
					&& aSqlExpression.exprString.length() > 0
					&& !aSqlExpression.exprString.startsWith("\"")) {
				newExprString = IdentifierHandler
						.quote(aSqlExpression.exprString);
			} else {
				newExprString = aSqlExpression.exprString;
			}
		}

		return newExprString;
	}

	/**
	 * Checks to see if the expression is an aggregate expression. Note that it
	 * does not check child expressions for aggregates.
	 * 
	 * @return whether or not expression is an aggregate function
	 */
	public boolean isAggregateExpression() {
		if (getExprType() == SQLEX_FUNCTION) {
			switch (functionId) {
			case IFunctionID.SUM_ID:
			case IFunctionID.AVG_ID:
			case IFunctionID.COUNT_STAR_ID:
			case IFunctionID.COUNT_ID:
			case IFunctionID.MAX_ID:
			case IFunctionID.MIN_ID:
			case IFunctionID.BITOR_ID:
			case IFunctionID.BITAND_ID:
			case IFunctionID.BOOLOR_ID:
			case IFunctionID.BOOLAND_ID:
			case IFunctionID.EVERY_ID:
			case IFunctionID.STDEV_ID:
			case IFunctionID.STDEVPOP_ID:
			case IFunctionID.STDEVSAMP_ID:
			case IFunctionID.VARIANCE_ID:
			case IFunctionID.VARIANCEPOP_ID:
			case IFunctionID.VARIANCESAMP_ID:
			case IFunctionID.REGRCOUNT_ID:
			case IFunctionID.REGRSXX_ID:
			case IFunctionID.REGRSYY_ID:
			case IFunctionID.REGRSXY_ID:
			case IFunctionID.COVARPOP_ID:
			case IFunctionID.CORR_ID:
			case IFunctionID.COVARSAMP_ID:
			case IFunctionID.REGRSLOPE_ID:
			case IFunctionID.REGRINTERCEPT_ID:
			case IFunctionID.REGRR2_ID:
			case IFunctionID.REGRAVX_ID:
			case IFunctionID.REGRAVY_ID:
			case IFunctionID.ST_EXTENT_ID:
			case IFunctionID.ST_EXTENT3D_ID: 
			case IFunctionID.ST_COLLECT_AGG_ID:
				return true;
			default:
				return false;
			}
		}
		return false;
	}

	/**
	 * Checks to see if aggregates are used in this expression The
	 * Optimizer/Planner uses this to handle the final SELECTs
	 * 
	 * @return
	 * 
	 */

	public boolean containsAggregates() {
		for (SqlExpression expr : SqlExpression.getNodes(this, SQLEX_FUNCTION)) {
			if (expr.isAggregateExpression()) {
				return true;
			}
		}
		return false;
	}

	/**
	 * 
	 * 
	 * 
	 * @param prepend
	 * 
	 * @return
	 * 
	 */
	public String toString(String prepend) {
		return prepend + " " + exprString;
	}

	//sray
	public String toString() {
		return exprString;
	}
	/**
	 * 
	 * 
	 * 
	 * @param colname
	 * 
	 * @param tableName
	 * 
	 * @param tableAlias
	 * 
	 * @param columnAlias
	 * 
	 * @param Operator
	 * 
	 * @return
	 * 
	 */

	public static SqlExpression getSqlColumnExpression(String colname,
			String tableName, String tableAlias, String columnAlias,
			String Operator)

	{ // Create a new Expression
		SqlExpression sqlexpr = new SqlExpression();
		// Assign Column Name to the exprString --
		sqlexpr.exprString = colname;
		// ExprType SQLEX_COLUMN
		sqlexpr.setExprType(SqlExpression.SQLEX_COLUMN);
		// Create a new Column --
		sqlexpr.setColumn(new AttributeColumn());
		// Assign Values to the four fields
		sqlexpr.getColumn().columnName = colname;
		sqlexpr.setAlias(sqlexpr.getColumn().columnName);
		sqlexpr.getColumn().setTableName(tableName);
		if (Operator == null || Operator.equals("")) {
			sqlexpr.operator = "+";
		} else {
			sqlexpr.operator = Operator;
		}
		if (columnAlias == null || columnAlias.equals("")) {
			sqlexpr.getColumn().columnAlias = colname;
		} else {
			sqlexpr.getColumn().columnAlias = columnAlias;
		}
		if (tableAlias == null || tableAlias.equals("")) {
			sqlexpr.getColumn().setTableAlias(tableName);
		} else {
			sqlexpr.getColumn().setTableAlias(tableAlias);
		}
		sqlexpr.setAlias(sqlexpr.getColumn().columnAlias);
		sqlexpr.outerAlias = sqlexpr.getAlias();

		return sqlexpr;
	}

	/**
	 * 
	 * This function is responsible for finding the type of column contained in
	 * a particular expression.
	 * 
	 * 
	 * 
	 * @return ExpressionType
	 * 
	 * @param expr
	 * 
	 * @param column
	 * 
	 * @throws XDBServerException
	 * 
	 * @throws ColumnNotFoundException
	 * 
	 */

	public ExpressionType setExpressionResultType(SqlExpression expr,
			SysColumn column) {

		expr.setExprDataType(new ExpressionType());
		expr.getExprDataType().setExpressionType(column.getColType(),
				column.getColLength(), column.getColPrecision(),
				column.getColScale());
		return expr.getExprDataType();
	}

	/**
	 * 
	 * This function is responsible for finding the type of expression a
	 * 
	 * particular expression is.
	 * 
	 * 
	 * 
	 * @return
	 * 
	 * @param database
	 * 
	 * @param expr
	 * 
	 * @throws XDBServerException
	 * 
	 * @throws ColumnNotFoundException
	 * 
	 */

	public static ExpressionType setExpressionResultType(SqlExpression expr,
			SysDatabase database) throws XDBServerException,
			ColumnNotFoundException {
		if (expr.getExprDataType() != null && expr.getExprDataType().type != 0) {
			return expr.getExprDataType();
		}

		if (expr.getExprType() == SqlExpression.SQLEX_PARAMETER) {
			// expr.exprDataType.type may be 0 in this case
			return expr.getExprDataType();
		} else if (expr.getExprType() == SqlExpression.SQLEX_SUBQUERY) {

			// In this case we will need to find out the expression type that
			// this SQL query will return and that will be the expression type
			return expr.subqueryTree.getProjectionList().get(0)
					.getExprDataType();
		}

		if (expr.getExprType() == SqlExpression.SQLEX_CONDITION)

		{
			// Here we are sure that the SQLEX_CONDITION will give us a boolean
			// value
			ExpressionType exprT = new ExpressionType();
			exprT.setExpressionType(ExpressionType.BOOLEAN_TYPE, 0, 0, 0);
			return exprT;
		}

		if (expr.getExprType() == SqlExpression.SQLEX_COLUMNLIST) {
			// In case the Sql Expression is a columnlist
		}

		if (expr.getExprType() == SqlExpression.SQLEX_CONSTANT) {
			// In this case we will have to check the actual constant value
			// whether it is a string , a numeric, or decimal or a varchar -
			int dataType = expr.getConstantDataType();
			ExpressionType exprT = new ExpressionType();
			exprT.setExpressionType(dataType,
					expr.getConstantValue() == null ? -1 : expr
							.getConstantValue().length(),
					CONSTANT_PRECISION_DOUBLE_VALUE,
					CONSTANT_DOUBLE_SCALE_VALUE);
			expr.setExprDataType(exprT);
			return exprT;
		}

		if (expr.getExprType() == SqlExpression.SQLEX_FUNCTION) {
			if (expr.functionId == IFunctionID.CAST_ID) {
				expr.functionParams.get(0).rebuildExpression();
				SqlExpression.setExpressionResultType(
						expr.functionParams.get(0), database);
				expr.setExprDataType(new ExpressionType());
				expr.exprDataType.type = expr.expTypeOfCast.getSqlType();
				expr.exprDataType.length = expr.expTypeOfCast.getLength();
				expr.exprDataType.precision = expr.expTypeOfCast.getPrecision();
				expr.exprDataType.scale = expr.expTypeOfCast.getScale();
				if ((expr.exprDataType.type == Types.CHAR || expr.exprDataType.type == Types.VARCHAR)
						&& expr.exprDataType.length == -1) {
					expr.exprDataType.length = 1024;
				}
				return expr.exprDataType;
			} else {
				// Each function will give a specific value, - CAST operation
				// will also be treated as a function
				for (SqlExpression afunctionParameter : expr.functionParams) {
					afunctionParameter.setExprDataType(SqlExpression
							.setExpressionResultType(afunctionParameter,
									database));
				}
				ExpressionType exprT = expr.getFunctionOutputValue(database);
				expr.setExprDataType(exprT);
				return exprT;
			}
		}

		if (expr.getExprType() == SqlExpression.SQLEX_UNARY_EXPRESSION) {
			// assumes that the left one will actually be filled.
			ExpressionType exprDatatypeL = setExpressionResultType(
					expr.leftExpr, database);

			expr.setExprDataType(exprDatatypeL);
			return expr.getExprDataType();
		}

		if (expr.getExprType() == SqlExpression.SQLEX_COLUMN) {
			// If it is orphan
			if ((expr.getColumn().columnGenre & AttributeColumn.ORPHAN) == AttributeColumn.ORPHAN) {
				if (expr.getColumn().relationNode == null) {
					return null;
				}
			}

			AttributeColumn colAttrib = expr.getColumn();

			// This will find the column Type and Set it for this particular
			// column
			if (expr.mapped == SqlExpression.INTERNALMAPPING) {
				expr.setExprDataType(SqlExpression.setExpressionResultType(
						expr.mappedExpression, database));
			} else {
				expr.setExprDataType(colAttrib.getColumnType(database));
			}

			return expr.getExprDataType();
		}

		else if (expr.getExprType() == SqlExpression.SQLEX_OPERATOR_EXPRESSION) {
			try {
				ExpressionType exprDataTypeL = setExpressionResultType(
						expr.leftExpr, database);

				ExpressionType exprDataTypeR = setExpressionResultType(
						expr.rightExpr, database);

				ExprTypeHelper aExprTypeHelper = new ExprTypeHelper();

				aExprTypeHelper.leftExprType = exprDataTypeL;

				aExprTypeHelper.righExprType = exprDataTypeR;

				aExprTypeHelper.Operator = expr.operator;

				expr.setExprDataType(exprDataTypeL
						.GetExpressionType(aExprTypeHelper));
			} catch (XDBServerException ex) {
				String errorMessage = ErrorMessageRepository.EXPRESSION_TYPE_UNDETERMINED;
				throw new XDBServerException(
						errorMessage + expr.rebuildString(),
						ex,
						ErrorMessageRepository.EXPRESSION_TYPE_UNDETERMINED_CODE);
			}
			return expr.getExprDataType();
		} else if (expr.getExprType() == SqlExpression.SQLEX_CASE) {
			// 1. For each SQL expression in case construct call the -
			// setExpressionDataType
			// function. All the expression in the result data type should be
			// same
			expr.setExprDataType(expr.getCaseConstruct().setDataType(database));
			return expr.getExprDataType();
		} else if (expr.getExprType() == SqlExpression.SQLEX_LIST) {
			SqlExpression aSqlExpression = expr.expressionList.get(0);
			ExpressionType exprType = SqlExpression.setExpressionResultType(
					aSqlExpression, database);
			// Check to make sure all are of the same type
			for (SqlExpression aSqlExpressionItem : expr.expressionList) {
				ExpressionType exprItemType = SqlExpression
						.setExpressionResultType(aSqlExpressionItem, database);
				if (exprItemType.type != exprType.type) {
					throw new XDBServerException("The Expression "
							+ aSqlExpression.rebuildString() + " and "
							+ aSqlExpressionItem.rebuildString()
							+ " are not of the same type");
				}
			}
			return exprType;
		} else {
			throw new XDBServerException(
					ErrorMessageRepository.INVALID_DATATYPE + "( "
							+ expr.getExprType() + " , " + expr.exprString
							+ " ) ", 0,
					ErrorMessageRepository.INVALID_DATATYPE_CODE);
		}
	}

	/**
	 * 
	 * 
	 * 
	 * @return
	 * 
	 */

	int getConstantDataType() {
		// Determine if this is a constant data Type
		if (getExprType() == SqlExpression.SQLEX_CONSTANT) {
			if (constantValue == null) {
				return ExpressionType.NULL_TYPE;
			}

			// First we differentiate between - Numeric and Non _ Numeric
			// If the peice of data is non- numeric we automatically assign it
			// the value string
			// else we need to check if it is DOUBLE OR If It is Numeric
			// boolean isNumeric = false;
			try {
				Double.parseDouble(constantValue);
				return ExpressionType.DOUBLEPRECISION_TYPE;
			} catch (NumberFormatException ex) {
				// check to make sure that we are not dealing with
				// null
				// isNumeric = false;
				// if (constantValue.equalsIgnoreCase("null"))
				// if (constantValue == null)
				// {
				// return ExpressionType.NULL_TYPE;
				// }
				return ExpressionType.VARCHAR_TYPE;
			}
		}

		// Some illegal value
		return -1;
	}

	/**
	 * 
	 * Is this expression NULL?
	 * 
	 * @return
	 * 
	 */
	public boolean isNullConstant() {
		if (exprType == SqlExpression.SQLEX_CONSTANT) {
			if (constantValue == null) {
				return true;
			}
		}
		return false;
	}

	/**
	 * 
	 * 
	 * 
	 * @param database
	 * 
	 * @throws org.postgresql.stado.exception.ColumnNotFoundException
	 * 
	 * @return
	 * 
	 */

	ExpressionType getFunctionOutputValue(SysDatabase database)
			throws ColumnNotFoundException, XDBServerException {
		// Let us be positive and create an expression type which will be set
		// later
		ExpressionType exprType = new ExpressionType();
		// Check if there is a valid Function ID - use a switch case statement
		// here
		switch (this.functionId) {

		case IFunctionID.TIMEOFDAY_ID:
			exprType = FunctionAnalysis.analyzeTimeOfDayParameter(this);
			break;
		case IFunctionID.ISFINITE_ID:
			exprType = FunctionAnalysis.analyzeIsFiniteParameter(this);
			break;
		case IFunctionID.EXTRACT_ID:
			exprType = FunctionAnalysis.analyzeExtractParameter(this);
			break;
		case IFunctionID.DATEPART_ID:
			exprType = FunctionAnalysis.analyzeDatePartParameter(this);
			break;
		case IFunctionID.AGE_ID:
			exprType = FunctionAnalysis.analyzeAgeParameter(this);
			break;
		case IFunctionID.DATETRUNC_ID:
			exprType = FunctionAnalysis.analyzeDateTruncParameter(this);
			break;
		case IFunctionID.YEAR_ID:
		case IFunctionID.MONTH_ID:
		case IFunctionID.DAYOFMONTH_ID:
		case IFunctionID.DAYOFWEEK_ID:
		case IFunctionID.DAYOFYEAR_ID:
		case IFunctionID.WEEKOFYEAR_ID:
		case IFunctionID.DAY_ID:
			if (functionParams.get(0).getExprType() == SQLEX_CONSTANT) {
				functionParams.get(0)
						.setConstantValue(
								normalizeDate(functionParams.get(0)
										.getConstantValue()));
			}
			exprType = FunctionAnalysis
					.analyzeDayOfMonth_DayOfWeek_DayOfYear_Month_Year_WeekOfYear_Parameter(
							this, database);
			break;
		case IFunctionID.MINUTE_ID:
		case IFunctionID.SECOND_ID:
		case IFunctionID.HOUR_ID:
			if (functionParams.get(0).getExprType() == SQLEX_CONSTANT) {
				functionParams.get(0)
						.setConstantValue(
								normalizeTime(functionParams.get(0)
										.getConstantValue()));
			}
			exprType = FunctionAnalysis.analyzeHour_Min_SecParameter(this,
					database);
			break;

		case IFunctionID.ASCII_ID:
		case IFunctionID.LENGTH_ID:
			exprType = FunctionAnalysis.analyzeLengthParameter(this);
			break;
		case IFunctionID.ADDDATE_ID:
		case IFunctionID.SUBDATE_ID:
			exprType = FunctionAnalysis.analyzeAddDate_SubDateParameter(this,
					database);
			break;
		case IFunctionID.ADDTIME_ID:
		case IFunctionID.SUBTIME_ID:
			exprType = FunctionAnalysis.analyzeAddTime_SubTimeParameter(this,
					database);
			break;
		case IFunctionID.DATE_ID:
			if (functionParams.get(0).getExprType() == SQLEX_CONSTANT) {
				functionParams.get(0)
						.setConstantValue(
								normalizeDate(functionParams.get(0)
										.getConstantValue()));
			}
			exprType = FunctionAnalysis.analyzeDateParameter(this, database);
			break;
		case IFunctionID.TODATE_ID:
		case IFunctionID.TOTIMESTAMP_ID:
			exprType = FunctionAnalysis.analyzeToDateParameter(this, database);
			break;
		case IFunctionID.DATEDIFF_ID:
			for (int i = 0; i < 2; i++) {
				if (functionParams.get(i).getExprType() == SQLEX_CONSTANT) {
					try {
						functionParams.get(i).setConstantValue(
								normalizeDate(functionParams.get(i)
										.getConstantValue()));
					} catch (Exception e) {
						functionParams.get(i).setConstantValue(
								normalizeTimeStamp(functionParams.get(i)
										.getConstantValue()));
					}
				}
			}
			exprType = FunctionAnalysis.analyzeDateDiff(this, database);
			break;
		case IFunctionID.DAYNAME_ID:
		case IFunctionID.MONTHNAME_ID:
			exprType = FunctionAnalysis.analyzeDayName_MonthName_Parameter(
					this, database);
			break;
		case IFunctionID.TIMESTAMP_ID:
			if (functionParams.get(0).getExprType() == SQLEX_CONSTANT) {
				functionParams.get(0).setConstantValue(
						normalizeTimeStamp(functionParams.get(0)
								.getConstantValue()));
			}
			exprType = FunctionAnalysis.analyzeTimeStamp(this, database);
			break;

		case IFunctionID.TIME_ID:
			if (functionParams.get(0).getExprType() == SQLEX_CONSTANT) {
				functionParams.get(0)
						.setConstantValue(
								normalizeTime(functionParams.get(0)
										.getConstantValue()));
			}
			exprType = FunctionAnalysis.analyzeTime(this, database);
			break;

		case IFunctionID.CEIL_ID:
		case IFunctionID.FLOOR_ID:
		case IFunctionID.SIGN_ID:
			exprType = FunctionAnalysis.analyzeCeil_Floor_Sign(this, database);
			break;
		case IFunctionID.EXP_ID:
		case IFunctionID.LN_ID:
		case IFunctionID.POWER_ID:
		case IFunctionID.ASIN_ID:
		case IFunctionID.ATAN_ID:
		case IFunctionID.COS_ID:
		case IFunctionID.COT_ID:
		case IFunctionID.DEGREES_ID:
		case IFunctionID.RADIANS_ID:
		case IFunctionID.SIN_ID:
		case IFunctionID.TAN_ID:
		case IFunctionID.ACOS_ID:
		case IFunctionID.LOG10_ID:
		case IFunctionID.SQRT_ID:
		case IFunctionID.COSH_ID:
		case IFunctionID.LOG_ID:

			exprType = FunctionAnalysis
					.analyzeExp_LN_POWER_ASIN_ATAN_COS_COT_DEGREES_RADIANS_SIN_TAN_ACOS_LOG10_SQRT_COSH(
							this, database);
			break;

		case IFunctionID.TRUNC_ID:
			exprType = FunctionAnalysis.analyzeTrunc(this, database);
			break;

		case IFunctionID.FLOAT_ID:
		case IFunctionID.ATAN2_ID:
			exprType = FunctionAnalysis.analyzeLog_Float_ATAN2(this, database);
			break;
		case IFunctionID.ROUND_ID:
			exprType = FunctionAnalysis.analyzeRound(this, database);
			break;
		case IFunctionID.PI_ID:
		case IFunctionID.RANDOM_ID:
		case IFunctionID.CBRT_ID:

			exprType = FunctionAnalysis.analyzePI(this);
			break;
		/* Function signature : Input : Char , OutPut : Char */
		case IFunctionID.UPPER_ID:
		case IFunctionID.LOWER_ID:
		case IFunctionID.SOUNDEX_ID:
		case IFunctionID.INITCAP_ID:
			exprType = FunctionAnalysis
					.analyzeAscii_Upper_Lower_Soundex_InitCap(this, database);
			break;
		case IFunctionID.MAPCHAR_ID:
			exprType = FunctionAnalysis.analyzeMapchar(this, database);
			break;
		case IFunctionID.LFILL_ID:
			exprType = FunctionAnalysis.analyzeLfill(this, database);
			break;
		case IFunctionID.CONCAT_ID:
			exprType = FunctionAnalysis.analyzeConcat(this, database);
			break;
		/* Function signature : Input : (Char,Char) , OutPut : Char */

		case IFunctionID.LTRIM_ID:
		case IFunctionID.RTRIM_ID:
		case IFunctionID.TRIM_ID:
			exprType = FunctionAnalysis.analyzeLtrim_Rtirm_Trim(this, database);
			break;

		/* Function signature : Input ( char,char ,int, int) Output : Int */
		case IFunctionID.INSTR_ID:
		case IFunctionID.INDEX_ID:
			exprType = FunctionAnalysis.analyzeIndex_InStr(this, database);
			break;

		/* Function signature : input : */
		case IFunctionID.LEFT_ID:
		case IFunctionID.RIGHT_ID:
			exprType = FunctionAnalysis.analyzeLeft_Right(this, database);
			// logger.error("Not implemeneted as Yet");
			break;

		/* Function Signature : (Char,Int,Char,Int ) Output :Char */
		case IFunctionID.LPAD_ID:
		case IFunctionID.RPAD_ID:
			exprType = FunctionAnalysis.analyzeLpad_Rpad(this, database);
			break;
		/* Function Signature : (char,char,char) Output : Char */
		case IFunctionID.REPLACE_ID:
			exprType = FunctionAnalysis.analyzeReplace(this, database);
			break;
		/* Function Signature : (char, char,int) Output : char */
		case IFunctionID.SUBSTR_ID:

			exprType = FunctionAnalysis.analyzeSubString(this, database);
			break;
		/* Aggregate Function - */
		case IFunctionID.AVG_ID:
			exprType = FunctionAnalysis.analyzeAverageParameter(this, database);
			break;
		case IFunctionID.COUNT_ID:
			exprType = FunctionAnalysis.analyzeCountParameter(this);
			break;
		case IFunctionID.MAX_ID:
		case IFunctionID.MIN_ID:
			exprType = FunctionAnalysis.analyzeMax_MinParameter(this, database);
			break;
		case IFunctionID.ABS_ID:
			exprType = FunctionAnalysis.analyzeAbsParameter(this, database);
			break;
		case IFunctionID.SUM_ID:
			exprType = FunctionAnalysis.analyzeSumParameter(this, database);
			break;
		case IFunctionID.COUNT_STAR_ID: // no semantic analysis required
			exprType.setExpressionType(ExpressionType.INT_TYPE, 10, 10, 0);
			for (SqlExpression sqlexpr : functionParams) {
				sqlexpr.setExprDataType(setExpressionResultType(sqlexpr,
						database));
			}
			break;
		// Misc Functions
		case IFunctionID.CURRENTDATABASE_ID:
		case IFunctionID.CURRENTUSER_ID:
		case IFunctionID.VERSION_ID:
			exprType = FunctionAnalysis.analyzeDatabase_Version_User(this);
			break;
		case IFunctionID.VALUE_ID:
			exprType = FunctionAnalysis.analyzeValue(this, database);
			break;
		case IFunctionID.GREATEST_ID:
		case IFunctionID.LEAST_ID:
			exprType = FunctionAnalysis.analyzeGreatest_Least(this, database);
			break;
		case IFunctionID.NUM_ID:
			exprType = FunctionAnalysis.analyzeNUM(this);
			break;
		case IFunctionID.STRPOS_ID:
			exprType = FunctionAnalysis.analyzeStrPos(this, database);
			break;
		case IFunctionID.BTRIM_ID:
		case IFunctionID.SPLIT_PART_ID:
		case IFunctionID.TRANSLATE_ID:
			exprType = FunctionAnalysis.analyzeBtrim(this, database);
			break;
		case IFunctionID.TO_ASCII_ID:
			exprType = FunctionAnalysis.analyzeToASCII(this, database);
			break;
		case IFunctionID.COALESCE_ID:
			exprType = FunctionAnalysis.analyzeCoalesce(this, database);
			break;
		case IFunctionID.CHR_ID:
			exprType = FunctionAnalysis.analyzeChr(this, database);
			break;
		case IFunctionID.CONVERT_ID:
			exprType = FunctionAnalysis.analyzeConvert(this, database);
			break;
		case IFunctionID.DECODE_ID:
			exprType = FunctionAnalysis.analyzeDecode(this);
			break;
		case IFunctionID.ENCODE_ID:
			exprType = FunctionAnalysis.analyzeEncode(this);
			break;
		case IFunctionID.PG_CLIENT_ENCODING_ID:
		case IFunctionID.MD5_ID:
			exprType = FunctionAnalysis.analyzeMd5(this, database);
			break;
		case IFunctionID.QUOTE_IDENT_ID:
		case IFunctionID.QUOTE_LITERAL_ID:
			exprType = FunctionAnalysis.analyzeQuote(this, database);
			break;
		case IFunctionID.REPEAT_ID:
			exprType = FunctionAnalysis.analyzeRepeat(this, database);
			break;
		case IFunctionID.TO_HEX_ID:
			exprType = FunctionAnalysis.analyzeToHex(this, database);
			break;
		case IFunctionID.WIDTH_BUCKET_ID:
		case IFunctionID.SETSEED_ID:
			exprType = FunctionAnalysis.analyzeSetSeed(this);
			break;
		case IFunctionID.MOD_ID:
			exprType = FunctionAnalysis.analyzeMod(this);
			break;
		case IFunctionID.VARIANCE_ID:
		case IFunctionID.STDEV_ID:
		case IFunctionID.STDEVPOP_ID:
		case IFunctionID.STDEVSAMP_ID:
		case IFunctionID.VARIANCEPOP_ID:
		case IFunctionID.VARIANCESAMP_ID:
			exprType = FunctionAnalysis.analyzeVarianceOrStddev(this);
			break;
		case IFunctionID.CORR_ID:
		case IFunctionID.COVARPOP_ID:
		case IFunctionID.COVARSAMP_ID:
		case IFunctionID.REGRAVX_ID:
		case IFunctionID.REGRAVY_ID:
		case IFunctionID.REGRINTERCEPT_ID:
		case IFunctionID.REGRSLOPE_ID:
		case IFunctionID.REGRR2_ID:
		case IFunctionID.REGRSXX_ID:
		case IFunctionID.REGRSXY_ID:
		case IFunctionID.REGRSYY_ID:
			exprType = FunctionAnalysis.analyzeCoRegFunc(this, database);
			break;
		case IFunctionID.REGRCOUNT_ID:
			exprType = FunctionAnalysis.analyzeRegrCount(this, database);
			break;
		case IFunctionID.NULLIF_ID:
			/* Function Signature : (value1,value2) Output : type of value1 */
			exprType = FunctionAnalysis.analyzeNullIf(this, database);
			break;
		case IFunctionID.SETBIT_ID:
		case IFunctionID.SETBYTE_ID:
			/* Function Signature : (varchar,int, int) Output : bytea */
			exprType = FunctionAnalysis.analyzeSetBitByte(this, database);
			break;
		case IFunctionID.GETBIT_ID:
		case IFunctionID.GETBYTE_ID:
			/* Function Signature : (varchar, int) Output : int */
			exprType = FunctionAnalysis.analyzeGetBitByte(this, database);
			break;
		case IFunctionID.TOCHAR_ID:
			/* Input:DATE/INT/DOUBLE/NUMERIC, VARCHAR Output: VARCHAR */
			exprType = FunctionAnalysis.analyzeToChar(this, database);
			break;
		case IFunctionID.TONUMBER_ID:
			/* Input:VARCHAR, VARCHAR Output: NUMERIC */
			exprType = FunctionAnalysis.analyzeToNumber(this, database);
			break;
		case IFunctionID.ADDMONTHS_ID:
			/* Input:TIMESTAMP, NUMERIC Output: TIMESTAMP */
			exprType = FunctionAnalysis.analyzeAddMonth(this, database);
			break;
		case IFunctionID.JUSTIFYDAYS_ID:
		case IFunctionID.JUSTIFYHOURS_ID:
		case IFunctionID.JUSTIFYINTERVAL_ID:
		case IFunctionID.LASTDAY_ID:
			/* Input:INTERVAL Output: INTERVAL */
			exprType = FunctionAnalysis.analyzeJustify(this, database);
			break;
		case IFunctionID.MONTHSBETWEEN_ID:
			/* Input:TIMESTAMP, TIMESTAMP Output: NUMERIC */
			exprType = FunctionAnalysis.analyzeMonthsBetween(this, database);
			break;
		case IFunctionID.NEXTDAY_ID:
			/* Input:TIMESTAMP, TEXT Output: TIMESTAMP */
			exprType = FunctionAnalysis.analyzeNextDay(this, database);
			break;
		case IFunctionID.REGEXPREPLACE_ID:
			/* Input:TEXT, TEXT, TEXT, [TEXT] Output: TEXT */
			exprType = FunctionAnalysis.analyzeRegexpReplace(this, database);
			break;
		case IFunctionID.BITAND_ID:
		case IFunctionID.BITOR_ID:
			/* Input: sqlExpr Output: type of sqlExpr */
			exprType = FunctionAnalysis.analyzeBitAnd(this, database);
			break;
		case IFunctionID.BOOLAND_ID:
		case IFunctionID.BOOLOR_ID:
		case IFunctionID.EVERY_ID:
			/* Input: BOOLEAN Output: BOOLEAN */
			exprType = FunctionAnalysis.analyzeBoolAnd(this, database);
			break;
		// case IFunctionID.CAST_ID:
		// exprType = FunctionAnalysis.analyzeCast(this);
		// break;
		case IFunctionID.CURRENTDATE_ID:
		case IFunctionID.CURRENTTIME_ID:
		case IFunctionID.CURRENTTIMESTAMP_ID:
			exprType = FunctionAnalysis.analyzeCurrDateTime(this);
			break;

		case IFunctionID.ABBREV_ID:
			exprType = FunctionAnalysis.analyzeAbbrev(this);
			break;
		case IFunctionID.BROADCAST_ID:
			exprType = FunctionAnalysis.analyzeBroadcast(this);
			break;
		case IFunctionID.FAMILY_ID:
			exprType = FunctionAnalysis.analyzeFamily(this);
			break;
		case IFunctionID.HOST_ID:
			exprType = FunctionAnalysis.analyzeHost(this);
			break;
		case IFunctionID.HOSTMASK_ID:
			exprType = FunctionAnalysis.analyzeHostmask(this);
			break;
		case IFunctionID.MASKLEN_ID:
			exprType = FunctionAnalysis.analyzeMasklen(this);
			break;
		case IFunctionID.NETMASK_ID:
			exprType = FunctionAnalysis.analyzeNetmask(this);
			break;
		case IFunctionID.NETWORK_ID:
			exprType = FunctionAnalysis.analyzeNetwork(this);
			break;
		case IFunctionID.SET_MASKLEN_ID:
			exprType = FunctionAnalysis.analyzeSet_Masklen(this);
			break;
		case IFunctionID.TEXT_ID:
			exprType = FunctionAnalysis.analyzeText(this);
			break;
		case IFunctionID.ST_ASTEXT_ID:
			exprType = SpatialFunctionAnalysis.analyzeST_AsText(this, database);
			break;
		case IFunctionID.ST_TRANSFORM_ID:
			exprType = SpatialFunctionAnalysis.analyzeST_Transform(this,
					database);
			break;
		case IFunctionID.ST_DISTANCE_ID:
			exprType = SpatialFunctionAnalysis.analyzeST_Distance(this,
					database);
			break;
		case IFunctionID.ST_DWITHIN_ID:
			exprType = SpatialFunctionAnalysis
					.analyzeST_DWithin(this, database);
			break;
		case IFunctionID.ST_LENGTH_ID:
			exprType = SpatialFunctionAnalysis.analyzeST_Length(this, database);
			break;
		case IFunctionID.ST_AREA_ID:
			exprType = SpatialFunctionAnalysis.analyzeST_Area(this, database);
			break;
		case IFunctionID.ST_ASBINARY_ID:
			exprType = SpatialFunctionAnalysis.analyzeST_AsBinary(this,
					database);
			break;
		case IFunctionID.ST_BOUNDARY_ID:
			exprType = SpatialFunctionAnalysis.analyzeST_Boundary(this,
					database);
			break;
		case IFunctionID.ST_BUFFER_ID:
			exprType = SpatialFunctionAnalysis.analyzeST_Buffer(this, database);
			break;
		case IFunctionID.ST_CENTROID_ID:
			exprType = SpatialFunctionAnalysis.analyzeST_Centroid(this,
					database);
			break;

		case IFunctionID.ST_ADDMEASURE_ID:
			exprType = SpatialFunctionAnalysis.analyzeST_ADDMEASURE(this,
					database);
			break;
		case IFunctionID.ST_ADDPOINT_ID:
			exprType = SpatialFunctionAnalysis.analyzeST_ADDPOINT(this,
					database);
			break;
		case IFunctionID.ST_AFFINE_ID:
			exprType = SpatialFunctionAnalysis.analyzeST_AFFINE(this, database);
			break;
		case IFunctionID.ST_AREA2D_ID:
			exprType = SpatialFunctionAnalysis.analyzeST_AREA2D(this, database);
			break;
		case IFunctionID.ST_ASEWKB_ID:
			exprType = SpatialFunctionAnalysis.analyzeST_ASEWKB(this, database);
			break;
		case IFunctionID.ST_ASEWKT_ID:
			exprType = SpatialFunctionAnalysis.analyzeST_ASEWKT(this, database);
			break;
		case IFunctionID.ST_ASGEOJSON_ID:
			exprType = SpatialFunctionAnalysis.analyzeST_ASGEOJSON(this,
					database);
			break;
		case IFunctionID.ST_ASGML_ID:
			exprType = SpatialFunctionAnalysis.analyzeST_ASGML(this, database);
			break;
		case IFunctionID.ST_ASHEXEWKB_ID:
			exprType = SpatialFunctionAnalysis.analyzeST_ASHEXEWKB(this,
					database);
			break;
		case IFunctionID.ST_ASKML_ID:
			exprType = SpatialFunctionAnalysis.analyzeST_ASKML(this, database);
			break;
		case IFunctionID.ST_ASSVG_ID:
			exprType = SpatialFunctionAnalysis.analyzeST_ASSVG(this, database);
			break;
		case IFunctionID.ST_AZIMUTH_ID:
			exprType = SpatialFunctionAnalysis
					.analyzeST_AZIMUTH(this, database);
			break;
		case IFunctionID.ST_BDMPOLYFROMTEXT_ID:
			exprType = SpatialFunctionAnalysis.analyzeST_BDMPOLYFROMTEXT(this,
					database);
			break;
		case IFunctionID.ST_BDPOLYFROMTEXT_ID:
			exprType = SpatialFunctionAnalysis.analyzeST_BDPOLYFROMTEXT(this,
					database);
			break;
		case IFunctionID.ST_BUILDAREA_ID:
			exprType = SpatialFunctionAnalysis.analyzeST_BUILDAREA(this,
					database);
			break;
		case IFunctionID.ST_BYTEA_ID:
			exprType = SpatialFunctionAnalysis.analyzeST_BYTEA(this, database);
			break;
		case IFunctionID.ST_CHIP_IN_ID:
			exprType = SpatialFunctionAnalysis
					.analyzeST_CHIP_IN(this, database);
			break;
		case IFunctionID.ST_CHIP_OUT_ID:
			exprType = SpatialFunctionAnalysis.analyzeST_CHIP_OUT(this,
					database);
			break;
		case IFunctionID.ST_CLOSESTPOINT_ID:
			exprType = SpatialFunctionAnalysis.analyzeST_CLOSESTPOINT(this,
					database);
			break;
		case IFunctionID.ST_COLLECTIONEXTRACT_ID:
			exprType = SpatialFunctionAnalysis.analyzeST_COLLECTIONEXTRACT(
					this, database);
			break;
		case IFunctionID.ST_COMBINE_BBOX_ID:
			exprType = SpatialFunctionAnalysis.analyzeST_COMBINE_BBOX(this,
					database);
			break;
		case IFunctionID.ST_COMPRESSION_ID:
			exprType = SpatialFunctionAnalysis.analyzeST_COMPRESSION(this,
					database);
			break;
		case IFunctionID.ST_CONTAINS_ID:
			exprType = SpatialFunctionAnalysis.analyzeST_CONTAINS(this,
					database);
			break;
		case IFunctionID.ST_CONTAINSPROPERLY_ID:
			exprType = SpatialFunctionAnalysis.analyzeST_CONTAINSPROPERLY(this,
					database);
			break;
		case IFunctionID.ST_CONVEXHULL_ID:
			exprType = SpatialFunctionAnalysis.analyzeST_CONVEXHULL(this,
					database);
			break;
		case IFunctionID.ST_COORDDIM_ID:
			exprType = SpatialFunctionAnalysis.analyzeST_COORDDIM(this,
					database);
			break;
		case IFunctionID.ST_COVEREDBY_ID:
			exprType = SpatialFunctionAnalysis.analyzeST_COVEREDBY(this,
					database);
			break;
		case IFunctionID.ST_COVERS_ID:
			exprType = SpatialFunctionAnalysis.analyzeST_COVERS(this, database);
			break;
		case IFunctionID.ST_CROSSES_ID:
			exprType = SpatialFunctionAnalysis
					.analyzeST_CROSSES(this, database);
			break;
		case IFunctionID.ST_CURVETOLINE_ID:
			exprType = SpatialFunctionAnalysis.analyzeST_CURVETOLINE(this,
					database);
			break;
		case IFunctionID.ST_DATATYPE_ID:
			exprType = SpatialFunctionAnalysis.analyzeST_DATATYPE(this,
					database);
			break;
		case IFunctionID.ST_DFULLYWITHIN_ID:
			exprType = SpatialFunctionAnalysis.analyzeST_DFULLYWITHIN(this,
					database);
			break;
		case IFunctionID.ST_DIFFERENCE_ID:
			exprType = SpatialFunctionAnalysis.analyzeST_DIFFERENCE(this,
					database);
			break;
		case IFunctionID.ST_DIMENSION_ID:
			exprType = SpatialFunctionAnalysis.analyzeST_DIMENSION(this,
					database);
			break;
		case IFunctionID.ST_DISJOINT_ID:
			exprType = SpatialFunctionAnalysis.analyzeST_DISJOINT(this,
					database);
			break;
		case IFunctionID.ST_DISTANCE_SPHERE_ID:
			exprType = SpatialFunctionAnalysis.analyzeST_DISTANCE_SPHERE(this,
					database);
			break;
		case IFunctionID.ST_DISTANCE_SPHEROID_ID:
			exprType = SpatialFunctionAnalysis.analyzeST_DISTANCE_SPHEROID(
					this, database);
			break;
		case IFunctionID.ST_ENDPOINT_ID:
			exprType = SpatialFunctionAnalysis.analyzeST_ENDPOINT(this,
					database);
			break;
		case IFunctionID.ST_ENVELOPE_ID:
			exprType = SpatialFunctionAnalysis.analyzeST_ENVELOPE(this,
					database);
			break;
		case IFunctionID.ST_EQUALS_ID:
			exprType = SpatialFunctionAnalysis.analyzeST_EQUALS(this, database);
			break;
		case IFunctionID.ST_EXTERIORRING_ID:
			exprType = SpatialFunctionAnalysis.analyzeST_EXTERIORRING(this,
					database);
			break;
		case IFunctionID.ST_FACTOR_ID:
			exprType = SpatialFunctionAnalysis.analyzeST_FACTOR(this, database);
			break;
		case IFunctionID.ST_FIND_EXTENT_ID:
			exprType = SpatialFunctionAnalysis.analyzeST_FIND_EXTENT(this,
					database);
			break;
		case IFunctionID.ST_FORCERHR_ID:
			exprType = SpatialFunctionAnalysis.analyzeST_FORCERHR(this,
					database);
			break;
		case IFunctionID.ST_FORCE_2D_ID:
			exprType = SpatialFunctionAnalysis.analyzeST_FORCE_2D(this,
					database);
			break;
		case IFunctionID.ST_FORCE_3D_ID:
			exprType = SpatialFunctionAnalysis.analyzeST_FORCE_3D(this,
					database);
			break;
		case IFunctionID.ST_FORCE_3DM_ID:
			exprType = SpatialFunctionAnalysis.analyzeST_FORCE_3DM(this,
					database);
			break;
		case IFunctionID.ST_FORCE_3DZ_ID:
			exprType = SpatialFunctionAnalysis.analyzeST_FORCE_3DZ(this,
					database);
			break;
		case IFunctionID.ST_FORCE_4D_ID:
			exprType = SpatialFunctionAnalysis.analyzeST_FORCE_4D(this,
					database);
			break;
		case IFunctionID.ST_FORCE_COLLECTION_ID:
			exprType = SpatialFunctionAnalysis.analyzeST_FORCE_COLLECTION(this,
					database);
			break;
		case IFunctionID.ST_GEOMETRYTYPE_ID:
			exprType = SpatialFunctionAnalysis.analyzeST_GEOMETRYTYPE(this,
					database);
			break;
		case IFunctionID.ST_GEOGFROMTEXT_ID:
			exprType = SpatialFunctionAnalysis.analyzeST_GEOGFROMTEXT(this,
					database);
			break;
		case IFunctionID.ST_GEOGFROMWKB_ID:
			exprType = SpatialFunctionAnalysis.analyzeST_GEOGFROMWKB(this,
					database);
			break;
		case IFunctionID.ST_GEOGRAPHYFROMTEXT_ID:
			exprType = SpatialFunctionAnalysis.analyzeST_GEOGRAPHYFROMTEXT(
					this, database);
			break;
		case IFunctionID.ST_GEOHASH_ID:
			exprType = SpatialFunctionAnalysis
					.analyzeST_GEOHASH(this, database);
			break;
		case IFunctionID.ST_GEOMCOLLFROMTEXT_ID:
			exprType = SpatialFunctionAnalysis.analyzeST_GEOMCOLLFROMTEXT(this,
					database);
			break;
		case IFunctionID.ST_GEOMCOLLFROMWKB_ID:
			exprType = SpatialFunctionAnalysis.analyzeST_GEOMCOLLFROMWKB(this,
					database);
			break;
		case IFunctionID.ST_GEOMFROMEWKB_ID:
			exprType = SpatialFunctionAnalysis.analyzeST_GEOMFROMEWKB(this,
					database);
			break;
		case IFunctionID.ST_GEOMFROMEWKT_ID:
			exprType = SpatialFunctionAnalysis.analyzeST_GEOMFROMEWKT(this,
					database);
			break;
		case IFunctionID.ST_GEOMFROMGML_ID:
			exprType = SpatialFunctionAnalysis.analyzeST_GEOMFROMGML(this,
					database);
			break;
		case IFunctionID.ST_GEOMFROMKML_ID:
			exprType = SpatialFunctionAnalysis.analyzeST_GEOMFROMKML(this,
					database);
			break;
		case IFunctionID.ST_GEOMFROMTEXT_ID:
			exprType = SpatialFunctionAnalysis.analyzeST_GEOMFROMTEXT(this,
					database);
			break;
		case IFunctionID.ST_GEOMFROMWKB_ID:
			exprType = SpatialFunctionAnalysis.analyzeST_GEOMFROMWKB(this,
					database);
			break;
		case IFunctionID.ST_GMLTOSQL_ID:
			exprType = SpatialFunctionAnalysis.analyzeST_GMLTOSQL(this,
					database);
			break;
		case IFunctionID.ST_HASARC_ID:
			exprType = SpatialFunctionAnalysis.analyzeST_HASARC(this, database);
			break;
		case IFunctionID.ST_HAUSDORFFDISTANCE_ID:
			exprType = SpatialFunctionAnalysis.analyzeST_HAUSDORFFDISTANCE(
					this, database);
			break;
		case IFunctionID.ST_HEIGHT_ID:
			exprType = SpatialFunctionAnalysis.analyzeST_HEIGHT(this, database);
			break;
		case IFunctionID.ST_INTERIORRINGN_ID:
			exprType = SpatialFunctionAnalysis.analyzeST_INTERIORRINGN(this,
					database);
			break;
		case IFunctionID.ST_INTERSECTION_ID:
			exprType = SpatialFunctionAnalysis.analyzeST_INTERSECTION(this,
					database);
			break;
		case IFunctionID.ST_INTERSECTS_ID:
			exprType = SpatialFunctionAnalysis.analyzeST_INTERSECTS(this,
					database);
			break;
		case IFunctionID.ST_ISCLOSED_ID:
			exprType = SpatialFunctionAnalysis.analyzeST_ISCLOSED(this,
					database);
			break;
		case IFunctionID.ST_ISEMPTY_ID:
			exprType = SpatialFunctionAnalysis
					.analyzeST_ISEMPTY(this, database);
			break;
		case IFunctionID.ST_ISRING_ID:
			exprType = SpatialFunctionAnalysis.analyzeST_ISRING(this, database);
			break;
		case IFunctionID.ST_ISSIMPLE_ID:
			exprType = SpatialFunctionAnalysis.analyzeST_ISSIMPLE(this,
					database);
			break;
		case IFunctionID.ST_ISVALID_ID:
			exprType = SpatialFunctionAnalysis
					.analyzeST_ISVALID(this, database);
			break;
		case IFunctionID.ST_ISVALIDREASON_ID:
			exprType = SpatialFunctionAnalysis.analyzeST_ISVALIDREASON(this,
					database);
			break;
		case IFunctionID.ST_LENGTH2D_ID:
			exprType = SpatialFunctionAnalysis.analyzeST_LENGTH2D(this,
					database);
			break;
		case IFunctionID.ST_LENGTH2D_SPHEROID_ID:
			exprType = SpatialFunctionAnalysis.analyzeST_LENGTH2D_SPHEROID(
					this, database);
			break;
		case IFunctionID.ST_LENGTH3D_ID:
			exprType = SpatialFunctionAnalysis.analyzeST_LENGTH3D(this,
					database);
			break;
		case IFunctionID.ST_LENGTH3D_SPHEROID_ID:
			exprType = SpatialFunctionAnalysis.analyzeST_LENGTH3D_SPHEROID(
					this, database);
			break;
		case IFunctionID.ST_LENGTH_SPHEROID_ID:
			exprType = SpatialFunctionAnalysis.analyzeST_LENGTH_SPHEROID(this,
					database);
			break;
		case IFunctionID.ST_LINECROSSINGDIRECTION_ID:
			exprType = SpatialFunctionAnalysis.analyzeST_LINECROSSINGDIRECTION(
					this, database);
			break;
		case IFunctionID.ST_LINEFROMMULTIPOINT_ID:
			exprType = SpatialFunctionAnalysis.analyzeST_LINEFROMMULTIPOINT(
					this, database);
			break;
		case IFunctionID.ST_LINEFROMTEXT_ID:
			exprType = SpatialFunctionAnalysis.analyzeST_LINEFROMTEXT(this,
					database);
			break;
		case IFunctionID.ST_LINEFROMWKB_ID:
			exprType = SpatialFunctionAnalysis.analyzeST_LINEFROMWKB(this,
					database);
			break;
		case IFunctionID.ST_LINEMERGE_ID:
			exprType = SpatialFunctionAnalysis.analyzeST_LINEMERGE(this,
					database);
			break;
		case IFunctionID.ST_LINESTRINGFROMWKB_ID:
			exprType = SpatialFunctionAnalysis.analyzeST_LINESTRINGFROMWKB(
					this, database);
			break;
		case IFunctionID.ST_LINETOCURVE_ID:
			exprType = SpatialFunctionAnalysis.analyzeST_LINETOCURVE(this,
					database);
			break;
		case IFunctionID.ST_LINE_INTERPOLATE_POINT_ID:
			exprType = SpatialFunctionAnalysis
					.analyzeST_LINE_INTERPOLATE_POINT(this, database);
			break;
		case IFunctionID.ST_LINE_LOCATE_POINT_ID:
			exprType = SpatialFunctionAnalysis.analyzeST_LINE_LOCATE_POINT(
					this, database);
			break;
		case IFunctionID.ST_LINE_SUBSTRING_ID:
			exprType = SpatialFunctionAnalysis.analyzeST_LINE_SUBSTRING(this,
					database);
			break;
		case IFunctionID.ST_LOCATEBETWEENELEVATIONS_ID:
			exprType = SpatialFunctionAnalysis
					.analyzeST_LOCATEBETWEENELEVATIONS(this, database);
			break;
		case IFunctionID.ST_LOCATE_ALONG_MEASURE_ID:
			exprType = SpatialFunctionAnalysis.analyzeST_LOCATE_ALONG_MEASURE(
					this, database);
			break;
		case IFunctionID.ST_LOCATE_BETWEEN_MEASURES_ID:
			exprType = SpatialFunctionAnalysis
					.analyzeST_LOCATE_BETWEEN_MEASURES(this, database);
			break;
		case IFunctionID.ST_LONGESTLINE_ID:
			exprType = SpatialFunctionAnalysis.analyzeST_LONGESTLINE(this,
					database);
			break;
		case IFunctionID.ST_M_ID:
			exprType = SpatialFunctionAnalysis.analyzeST_M(this, database);
			break;
		case IFunctionID.ST_MAKEENVELOPE_ID:
			exprType = SpatialFunctionAnalysis.analyzeST_MAKEENVELOPE(this,
					database);
			break;
		case IFunctionID.ST_MAKEPOINT_ID:
			exprType = SpatialFunctionAnalysis.analyzeST_MAKEPOINT(this,
					database);
			break;
		case IFunctionID.ST_MAKEPOINTM_ID:
			exprType = SpatialFunctionAnalysis.analyzeST_MAKEPOINTM(this,
					database);
			break;
		case IFunctionID.ST_MAKEPOLYGON_ID:
			exprType = SpatialFunctionAnalysis.analyzeST_MAKEPOLYGON(this,
					database);
			break;
		case IFunctionID.ST_MAXDISTANCE_ID:
			exprType = SpatialFunctionAnalysis.analyzeST_MAXDISTANCE(this,
					database);
			break;
		case IFunctionID.ST_MEMCOLLECT_ID:
			exprType = SpatialFunctionAnalysis.analyzeST_MEMCOLLECT(this,
					database);
			break;
		case IFunctionID.ST_MEM_SIZE_ID:
			exprType = SpatialFunctionAnalysis.analyzeST_MEM_SIZE(this,
					database);
			break;
		case IFunctionID.ST_MINIMUMBOUNDINGCIRCLE_ID:
			exprType = SpatialFunctionAnalysis.analyzeST_MINIMUMBOUNDINGCIRCLE(
					this, database);
			break;
		case IFunctionID.ST_MLINEFROMTEXT_ID:
			exprType = SpatialFunctionAnalysis.analyzeST_MLINEFROMTEXT(this,
					database);
			break;
		case IFunctionID.ST_MLINEFROMWKB_ID:
			exprType = SpatialFunctionAnalysis.analyzeST_MLINEFROMWKB(this,
					database);
			break;
		case IFunctionID.ST_MPOINTFROMTEXT_ID:
			exprType = SpatialFunctionAnalysis.analyzeST_MPOINTFROMTEXT(this,
					database);
			break;
		case IFunctionID.ST_MPOINTFROMWKB_ID:
			exprType = SpatialFunctionAnalysis.analyzeST_MPOINTFROMWKB(this,
					database);
			break;
		case IFunctionID.ST_MPOLYFROMTEXT_ID:
			exprType = SpatialFunctionAnalysis.analyzeST_MPOLYFROMTEXT(this,
					database);
			break;
		case IFunctionID.ST_MPOLYFROMWKB_ID:
			exprType = SpatialFunctionAnalysis.analyzeST_MPOLYFROMWKB(this,
					database);
			break;
		case IFunctionID.ST_MULTI_ID:
			exprType = SpatialFunctionAnalysis.analyzeST_MULTI(this, database);
			break;
		case IFunctionID.ST_MULTILINEFROMWKB_ID:
			exprType = SpatialFunctionAnalysis.analyzeST_MULTILINEFROMWKB(this,
					database);
			break;
		case IFunctionID.ST_MULTILINESTRINGFROMTEXT_ID:
			exprType = SpatialFunctionAnalysis
					.analyzeST_MULTILINESTRINGFROMTEXT(this, database);
			break;
		case IFunctionID.ST_MULTIPOINTFROMTEXT_ID:
			exprType = SpatialFunctionAnalysis.analyzeST_MULTIPOINTFROMTEXT(
					this, database);
			break;
		case IFunctionID.ST_MULTIPOINTFROMWKB_ID:
			exprType = SpatialFunctionAnalysis.analyzeST_MULTIPOINTFROMWKB(
					this, database);
			break;
		case IFunctionID.ST_MULTIPOLYFROMWKB_ID:
			exprType = SpatialFunctionAnalysis.analyzeST_MULTIPOLYFROMWKB(this,
					database);
			break;
		case IFunctionID.ST_MULTIPOLYGONFROMTEXT_ID:
			exprType = SpatialFunctionAnalysis.analyzeST_MULTIPOLYGONFROMTEXT(
					this, database);
			break;
		case IFunctionID.ST_NDIMS_ID:
			exprType = SpatialFunctionAnalysis.analyzeST_NDIMS(this, database);
			break;
		case IFunctionID.ST_NPOINTS_ID:
			exprType = SpatialFunctionAnalysis
					.analyzeST_NPOINTS(this, database);
			break;
		case IFunctionID.ST_NRINGS_ID:
			exprType = SpatialFunctionAnalysis.analyzeST_NRINGS(this, database);
			break;
		case IFunctionID.ST_NUMGEOMETRIES_ID:
			exprType = SpatialFunctionAnalysis.analyzeST_NUMGEOMETRIES(this,
					database);
			break;
		case IFunctionID.ST_NUMINTERIORRING_ID:
			exprType = SpatialFunctionAnalysis.analyzeST_NUMINTERIORRING(this,
					database);
			break;
		case IFunctionID.ST_NUMINTERIORRINGS_ID:
			exprType = SpatialFunctionAnalysis.analyzeST_NUMINTERIORRINGS(this,
					database);
			break;
		case IFunctionID.ST_NUMPOINTS_ID:
			exprType = SpatialFunctionAnalysis.analyzeST_NUMPOINTS(this,
					database);
			break;
		case IFunctionID.ST_ORDERINGEQUALS_ID:
			exprType = SpatialFunctionAnalysis.analyzeST_ORDERINGEQUALS(this,
					database);
			break;
		case IFunctionID.ST_OVERLAPS_ID:
			exprType = SpatialFunctionAnalysis.analyzeST_OVERLAPS(this,
					database);
			break;
		case IFunctionID.ST_PERIMETER_ID:
			exprType = SpatialFunctionAnalysis.analyzeST_PERIMETER(this,
					database);
			break;
		case IFunctionID.ST_PERIMETER2D_ID:
			exprType = SpatialFunctionAnalysis.analyzeST_PERIMETER2D(this,
					database);
			break;
		case IFunctionID.ST_PERIMETER3D_ID:
			exprType = SpatialFunctionAnalysis.analyzeST_PERIMETER3D(this,
					database);
			break;
		case IFunctionID.ST_POINT_ID:
			exprType = SpatialFunctionAnalysis.analyzeST_POINT(this, database);
			break;
		case IFunctionID.ST_POINTFROMTEXT_ID:
			exprType = SpatialFunctionAnalysis.analyzeST_POINTFROMTEXT(this,
					database);
			break;
		case IFunctionID.ST_POINTFROMWKB_ID:
			exprType = SpatialFunctionAnalysis.analyzeST_POINTFROMWKB(this,
					database);
			break;
		case IFunctionID.ST_POINTN_ID:
			exprType = SpatialFunctionAnalysis.analyzeST_POINTN(this, database);
			break;
		case IFunctionID.ST_POINTONSURFACE_ID:
			exprType = SpatialFunctionAnalysis.analyzeST_POINTONSURFACE(this,
					database);
			break;
		case IFunctionID.ST_POINT_INSIDE_CIRCLE_ID:
			exprType = SpatialFunctionAnalysis.analyzeST_POINT_INSIDE_CIRCLE(
					this, database);
			break;
		case IFunctionID.ST_POLYFROMTEXT_ID:
			exprType = SpatialFunctionAnalysis.analyzeST_POLYFROMTEXT(this,
					database);
			break;
		case IFunctionID.ST_POLYFROMWKB_ID:
			exprType = SpatialFunctionAnalysis.analyzeST_POLYFROMWKB(this,
					database);
			break;
		case IFunctionID.ST_POLYGON_ID:
			exprType = SpatialFunctionAnalysis
					.analyzeST_POLYGON(this, database);
			break;
		case IFunctionID.ST_POLYGONFROMTEXT_ID:
			exprType = SpatialFunctionAnalysis.analyzeST_POLYGONFROMTEXT(this,
					database);
			break;
		case IFunctionID.ST_POLYGONFROMWKB_ID:
			exprType = SpatialFunctionAnalysis.analyzeST_POLYGONFROMWKB(this,
					database);
			break;
		case IFunctionID.ST_POSTGIS_GIST_JOINSEL_ID:
			exprType = SpatialFunctionAnalysis.analyzeST_POSTGIS_GIST_JOINSEL(
					this, database);
			break;
		case IFunctionID.ST_POSTGIS_GIST_SEL_ID:
			exprType = SpatialFunctionAnalysis.analyzeST_POSTGIS_GIST_SEL(this,
					database);
			break;
		case IFunctionID.ST_RELATE_ID:
			exprType = SpatialFunctionAnalysis.analyzeST_RELATE(this, database);
			break;
		case IFunctionID.ST_REMOVEPOINT_ID:
			exprType = SpatialFunctionAnalysis.analyzeST_REMOVEPOINT(this,
					database);
			break;
		case IFunctionID.ST_REVERSE_ID:
			exprType = SpatialFunctionAnalysis
					.analyzeST_REVERSE(this, database);
			break;
		case IFunctionID.ST_ROTATE_ID:
			exprType = SpatialFunctionAnalysis.analyzeST_ROTATE(this, database);
			break;
		case IFunctionID.ST_ROTATEX_ID:
			exprType = SpatialFunctionAnalysis
					.analyzeST_ROTATEX(this, database);
			break;
		case IFunctionID.ST_ROTATEY_ID:
			exprType = SpatialFunctionAnalysis
					.analyzeST_ROTATEY(this, database);
			break;
		case IFunctionID.ST_ROTATEZ_ID:
			exprType = SpatialFunctionAnalysis
					.analyzeST_ROTATEZ(this, database);
			break;
		case IFunctionID.ST_SCALE_ID:
			exprType = SpatialFunctionAnalysis.analyzeST_SCALE(this, database);
			break;
		case IFunctionID.ST_SEGMENTIZE_ID:
			exprType = SpatialFunctionAnalysis.analyzeST_SEGMENTIZE(this,
					database);
			break;
		case IFunctionID.ST_SETFACTOR_ID:
			exprType = SpatialFunctionAnalysis.analyzeST_SETFACTOR(this,
					database);
			break;
		case IFunctionID.ST_SETPOINT_ID:
			exprType = SpatialFunctionAnalysis.analyzeST_SETPOINT(this, database);
			break;
		case IFunctionID.ST_SETSRID_ID:
			exprType = SpatialFunctionAnalysis
					.analyzeST_SETSRID(this, database);
			break;
		case IFunctionID.ST_SHIFT_LONGITUDE_ID:
			exprType = SpatialFunctionAnalysis.analyzeST_SHIFT_LONGITUDE(this,
					database);
			break;
		case IFunctionID.ST_SHORTESTLINE_ID:
			exprType = SpatialFunctionAnalysis.analyzeST_SHORTESTLINE(this,
					database);
			break;
		case IFunctionID.ST_SIMPLIFY_ID:
			exprType = SpatialFunctionAnalysis.analyzeST_SIMPLIFY(this, database);
			break;
		case IFunctionID.ST_SIMPLIFYPRESERVETOPOLOGY_ID:
			exprType = SpatialFunctionAnalysis
					.analyzeST_SIMPLIFYPRESERVETOPOLOGY(this, database);
			break;
		case IFunctionID.ST_SNAPTOGRID_ID:
			exprType = SpatialFunctionAnalysis.analyzeST_SNAPTOGRID(this,
					database);
			break;
		case IFunctionID.ST_SRID_ID:
			exprType = SpatialFunctionAnalysis.analyzeST_SRID(this, database);
			break;
		case IFunctionID.ST_STARTPOINT_ID:
			exprType = SpatialFunctionAnalysis.analyzeST_STARTPOINT(this,
					database);
			break;
		case IFunctionID.ST_SUMMARY_ID:
			exprType = SpatialFunctionAnalysis
					.analyzeST_SUMMARY(this, database);
			break;
		case IFunctionID.ST_SYMDIFFERENCE_ID:
			exprType = SpatialFunctionAnalysis.analyzeST_SYMDIFFERENCE(this,database);
			break;
		case IFunctionID.ST_SYMMETRICDIFFERENCE_ID:
			exprType = SpatialFunctionAnalysis.analyzeST_SYMMETRICDIFFERENCE(this, database);
			break;
		case IFunctionID.ST_TEXT_ID:
			exprType = SpatialFunctionAnalysis.analyzeST_TEXT(this, database);
			break;
		case IFunctionID.ST_TOUCHES_ID:
			exprType = SpatialFunctionAnalysis.analyzeST_TOUCHES(this, database);
			break;
		case IFunctionID.ST_TRANSLATE_ID:
			exprType = SpatialFunctionAnalysis.analyzeST_TRANSLATE(this, database);
			break;
		case IFunctionID.ST_TRANSSCALE_ID:
			exprType = SpatialFunctionAnalysis.analyzeST_TRANSSCALE(this, database);
			break;
		case IFunctionID.ST_WIDTH_ID:
			exprType = SpatialFunctionAnalysis.analyzeST_WIDTH(this, database);
			break;
		case IFunctionID.ST_WITHIN_ID:
			exprType = SpatialFunctionAnalysis.analyzeST_WITHIN(this, database);
			break;
		case IFunctionID.ST_WKBTOSQL_ID:
			exprType = SpatialFunctionAnalysis.analyzeST_WKBTOSQL(this,database);
			break;
		case IFunctionID.ST_WKTTOSQL_ID:
			exprType = SpatialFunctionAnalysis.analyzeST_WKTTOSQL(this, database);
			break;
		case IFunctionID.ST_X_ID:
			exprType = SpatialFunctionAnalysis.analyzeST_X(this, database);
			break;
		case IFunctionID.ST_Y_ID:
			exprType = SpatialFunctionAnalysis.analyzeST_Y(this, database);
			break;
		case IFunctionID.ST_Z_ID:
			exprType = SpatialFunctionAnalysis.analyzeST_Z(this, database);
			break;
		case IFunctionID.ST_ZMFLAG_ID:
			exprType = SpatialFunctionAnalysis.analyzeST_ZMFLAG(this, database);
			break;
		case IFunctionID.ST_BOX2D_ID:
			exprType = SpatialFunctionAnalysis.analyzeST_BOX2D(this, database);
			break;
		case IFunctionID.ST_BOX3D_ID:
			exprType = SpatialFunctionAnalysis.analyzeST_BOX3D(this, database);
			break;
		case IFunctionID.ST_GEOMETRY_ID:
			exprType = SpatialFunctionAnalysis.analyzeST_GEOMETRY(this, database);
			break;
		case IFunctionID.ST_GEOMETRYN_ID:
			exprType = SpatialFunctionAnalysis.analyzeST_GEOMETRYN(this, database);
			break;
		case IFunctionID.POSTGIS_DROPBBOX_ID:
			exprType = SpatialFunctionAnalysis.analyzePOSTGIS_DROPBBOX(this, database);
			break;
		case IFunctionID.ST_EXTENT_ID:
			exprType = SpatialFunctionAnalysis.analyzeST_EXTENT(this, database);
			break;
		case IFunctionID.ST_EXTENT3D_ID:
			exprType = SpatialFunctionAnalysis.analyzeST_EXTENT3D(this, database);
			break;
		case IFunctionID.ST_COLLECT_ID:
		case IFunctionID.ST_COLLECT_AGG_ID:
			exprType = SpatialFunctionAnalysis.analyzeST_COLLECT(this, database);
			break;

		default:
			try {
			exprType = FunctionAnalysis.analyzeCustom(this, database);
			} catch (Exception ex) {	
				throw new XDBServerException(this.functionName, ex);
			}
		}

		return exprType;
	}

	public String rebuildString(XDBSessionContext client) {
		rebuildExpression(client);
		return exprString;
	}

	/**
	 * 
	 * 
	 * 
	 * @return
	 * 
	 */
	public String rebuildString() {
		rebuildExpression();

		return exprString;
	}

	/**
	 * Checks to see if the expression only contains constants It or all of its
	 * children must have type SQLEX_CONSTANT
	 * 
	 * 
	 * @return
	 * 
	 */

	public boolean isConstantExpr() {
		if (this.getExprType() == SQLEX_CONSTANT) {
			return true;
		} else if (this.getExprType() == SQLEX_OPERATOR_EXPRESSION) {
			if (this.leftExpr != null && !this.leftExpr.isConstantExpr()) {
				return false;
			}
			if (this.rightExpr != null && !this.rightExpr.isConstantExpr()) {
				return false;
			}
			return true;
		} else if (this.getExprType() == SQLEX_FUNCTION) {
			// Make sure not an aggregate function
			if (this.containsAggregates()) {
				return false;
			}

			for (int i = 0; i < this.functionParams.size(); i++) {
				SqlExpression aSE = this.functionParams.get(i);
				if (!aSE.isConstantExpr()) {
					return false;
				}
			}
			return true;
		} else if (this.getExprType() == SQLEX_UNARY_EXPRESSION) {
			if (!this.leftExpr.isConstantExpr()) {
				return false;
			}
			return true;
		} else {
			return false;
		}
	}

	/**
	 * Functions for get and set parent containerNode
	 * 
	 * 
	 * @return
	 * 
	 */

	public RelationNode getParentContainerNode() {
		return parentContainerNode;
	}

	/**
	 * 
	 * 
	 * 
	 * @param parentContainerNode
	 * 
	 */

	public void setParentContainerNode(RelationNode parentContainerNode) {
		this.parentContainerNode = parentContainerNode;
	}

	/**
	 * 
	 * Checks to see if the SqlExpression is equal to the one passed in, or if
	 * it is contained within it.
	 * 
	 * @param aSqlExpression
	 * 
	 * @return
	 * 
	 */
	public boolean contains(SqlExpression aSqlExpression) {
		boolean check = false;

		if (aSqlExpression == this) {
			return true;
		}

		if (this.leftExpr != null) {
			check = this.leftExpr.contains(aSqlExpression);
		}

		if (!check && this.rightExpr != null) {
			return this.rightExpr.contains(aSqlExpression);
		}

		return check;
	}

	/**
	 * 
	 * Check to see if the SqlExpression contains any expressions that are only
	 * derived from the specified RelationNode. <br>
	 * 
	 * @param checkTableName
	 * 
	 * @return
	 * 
	 */
	public boolean containsColumnsExclusiveFromTable(String checkTableName) {
		boolean check = false;

		if (this.getExprType() == SQLEX_COLUMN) {
			// if (this.column.relationNode == aRelationNode)
			if (this.getColumn().relationNode.getCurrentTempTableName()
					.equalsIgnoreCase(checkTableName)) {
				return true;
			} else {
				return false;
			}
		}

		/*
		 * We don't want to check recursively. This is used just to detect
		 * special, hashable correlated subqueries
		 * 
		 * if (this.leftExpr != null) { check =
		 * this.leftExpr.containsColumnsExclusiveFromTable (checkTableName);
		 * 
		 * if (!check) { return false; } }
		 * 
		 * if (this.rightExpr != null) { return
		 * this.rightExpr.containsColumnsExclusiveFromTable (checkTableName); }
		 */

		return check;
	}

	/**
	 * 
	 * Check to see if the SqlExpression contains the specified AttributeColumn
	 * 
	 * @param anAttributeColumn
	 * 
	 * @return
	 * 
	 */
	public boolean containsColumn(AttributeColumn anAttributeColumn) {
		boolean check = false;

		if (this.getExprType() == SQLEX_COLUMN) {
			// if (this.column.relationNode == aRelationNode)
			if (this.getColumn() == anAttributeColumn) {
				return true;
			} else {
				return false;
			}
		} else if ((this.getExprType() & SQLEX_FUNCTION) > 0) {
			for (SqlExpression aSqlExpression : functionParams) {
				if (aSqlExpression.containsColumn(anAttributeColumn)) {
					return true;
				}
			}
		} else if ((this.getExprType() & SQLEX_CASE) > 0) {
			for (SqlExpression aSqlExpression : caseConstruct
					.getSQLExpressions()) {
				if (aSqlExpression.containsColumn(anAttributeColumn)) {
					return true;
				}
			}
		} else if ((this.getExprType() & SQLEX_SUBQUERY) > 0) {
			if ((this.subqueryTree.getQueryType() & QueryTree.SCALAR) == 0) {
				for (SqlExpression aSqlExpression : parentContainerNode
						.getProjectionList()) {
					if (aSqlExpression.containsColumn(anAttributeColumn)) {
						return true;
					}
				}
			}
		}

		if (leftExpr != null) {
			check = leftExpr.containsColumn(anAttributeColumn);

			if (check) {
				return true;
			}
		}

		if (rightExpr != null) {
			return rightExpr.containsColumn(anAttributeColumn);
		}

		return check;
	}

	/**
	 * 
	 * Check to see if the SqlExpression contains the specified AttributeColumn
	 * 
	 * @param aRelationNode
	 * 
	 * @return
	 * 
	 */
	public boolean contains(RelationNode aRelationNode) {
		boolean check = false;

		if (this.getExprType() == SQLEX_COLUMN) {
			// if (this.column.relationNode == aRelationNode)
			if (this.getColumn().relationNode == aRelationNode) {
				return true;
			} else {
				return false;
			}
		} else if ((this.getExprType() & SQLEX_FUNCTION) > 0) {
			for (SqlExpression aSqlExpression : functionParams) {
				if (aSqlExpression.contains(aRelationNode)) {
					return true;
				}
			}
		} else if ((this.getExprType() & SQLEX_CASE) > 0) {
			for (SqlExpression aSqlExpression : caseConstruct
					.getSQLExpressions()) {
				if (aSqlExpression.contains(aRelationNode)) {
					return true;
				}
			}
		} else if ((this.getExprType() & SQLEX_SUBQUERY) > 0) {
			if ((this.subqueryTree.getQueryType() & QueryTree.SCALAR) == 0) {
				for (SqlExpression aSqlExpression : parentContainerNode
						.getProjectionList()) {
					if (aSqlExpression.contains(aRelationNode)) {
						return true;
					}
				}
			}
		}

		if (this.leftExpr != null) {
			check = this.leftExpr.contains(aRelationNode);

			if (check) {
				return true;
			}
		}

		if (this.rightExpr != null) {
			return this.rightExpr.contains(aRelationNode);
		}

		return check;
	}

	/**
	 * Normalize numeric value to prevent ambiguous hash codes
	 * 
	 * @param anInputString
	 * @return
	 */
	private static String normalizeNumber(String anInputString, int scale) {
		// Detect scientific format
		int pos = Math.max(anInputString.indexOf("E"),
				anInputString.indexOf("e"));
		if (pos > 0) {
			return normalizeNumber(anInputString.substring(0, pos), scale)
					+ "E"
					+ normalizeNumber(anInputString.substring(pos + 1), 0);
		}
		// detect sign
		boolean negative = anInputString.charAt(0) == '-';
		pos = anInputString.indexOf(".");
		int end = anInputString.length() - 1;
		if (pos > -1) {
			while (anInputString.charAt(end) == '0') {
				end--;
			}
			if (end == pos) {
				end--;
			}
		} else {
			pos = end + 1;
		}
		int start = 0;
		while (start < pos
				&& (anInputString.charAt(start) == '-'
						|| anInputString.charAt(start) == '+' || anInputString
						.charAt(start) == '0')) {
			start++;
		}
		if (end - pos > scale) {
			end = pos + scale;
		}
		return (negative ? "-" : "")
				+ (start == pos ? "0" : anInputString.substring(start, pos))
				+ (end > pos ? anInputString.substring(pos, end + 1) : "");
	}

	/**
	 * 
	 * 
	 * 
	 * @return
	 * 
	 * @param anInputString
	 * 
	 */

	public static String normalizeDate(String anInputString) {
		String key_word = "";
		if (anInputString.toLowerCase().startsWith("date")) {
			anInputString = anInputString.substring(4, anInputString.length())
					.trim();
			key_word = "date";
		}
		// Strip quotes if present
		while (anInputString.charAt(0) == '\''
				&& anInputString.charAt(anInputString.length() - 1) == '\'') {
			anInputString = anInputString.substring(1,
					anInputString.length() - 1);
		}
		String theResult;
		if (anInputString.matches("[0-9][0-9][0-9][0-9][0-9][0-9][0-9][0-9]")) {
			// YYYYMMDD
			theResult = anInputString.substring(0, 4) + "-"
					+ anInputString.substring(4, 6) + "-"
					+ anInputString.substring(6, 8);
		} else if (anInputString
				.matches("[0-9][0-9][0-9][0-9]\\-[0-9][0-9]\\-[0-9][0-9]")) {
			// YYYY-MM-DD
			theResult = anInputString;
		} else if (anInputString
				.matches("[0-9][0-9][0-9][0-9]\\-[0-9]\\-[0-9]")) {
			// YYYY-M-D
			theResult = anInputString.substring(0, 5) + "0"
					+ anInputString.substring(5, 7) + "0"
					+ anInputString.substring(7, 8);
		} else if (anInputString
				.matches("[0-9]{1,4}\\-[a-zA-Z]*[[0-9]{1,2}]?\\-[0-9]{1,2} [0-9]{1,2}:[0-9]{1,2}[:[0-9]{0,2}]?[\\.[0-9]*]?")) {
			// YYYY-MM-DD HH:mim:sec
			theResult = anInputString;
		} else if (anInputString
				.matches("[0-9]{1,2}\\-[a-zA-Z]*[0-9]{0,2}\\-[0-9]{2,4}[ [0-9]{1,2}:[0-9]{1,2}:[0-9]{0,2}[\\.[0-9]*]?]?")) {
			// DD-MM-YYYY HH:mim:sec
			theResult = anInputString;
		} else if (anInputString
				.matches("[0-9]{1,2}/[0-9]{0,2}/[0-9]{2,4}[ [0-9]{1,2}:[0-9]{1,2}:[0-9]{0,2}[\\.[0-9]*]?]?")) {
			// MM/DD/YYYY HH:mim:sec
			theResult = anInputString;
		} /*
         */
		// else if (anInputString
		// .matches("\'[0-9]{1,2}\\-[0-9]{1,2}\\-[0-9][0-9][0-9][0-9]\'"))
		// {
		// //MM-DD-YYYY
		// String str2 = theResult.substring(theResult.length() - 4) + "-" +
		// theResult.substring(0,theResult.length() - 5);
		// theResult = "'" + str2 + "'";
		// theResult = normalizeDate(theResult);
		// theResult = theResult.replaceAll("'", "");
		// }
		else if (anInputString
				.matches("[0-9][0-9][0-9][0-9]\\-[0-9]\\-[0-9][0-9]")) {
			// YYYY-M-DD
			theResult = anInputString.substring(0, 5) + "0"
					+ anInputString.substring(5, 9);
		} else if (anInputString
				.matches("[0-9][0-9][0-9][0-9]\\-[0-9][0-9]\\-[0-9]")) {
			// YYYY-MM-D
			theResult = anInputString.substring(0, 8) + "0"
					+ anInputString.substring(8, 9);
		} else {
			throw new XDBServerException("Invalid date/time format "
					+ anInputString);
		}

		theResult = "'" + theResult + "'";

		if (key_word.length() != 0) {
			theResult = key_word + theResult;
		}
		return theResult;
	}

	/**
	 * @param anInputString
	 *            000102 00:01:02 0:1:2 00:1:2 01:02 1:02 1:2 02 2
	 * 
	 * @return
	 */
	public static String normalizeTime(String anInputString) {
		String key_word = "";
		if (anInputString.toLowerCase().startsWith("time")) {
			anInputString = anInputString.substring(4, anInputString.length())
					.trim();
			key_word = "time";
		}

		// Strip quotes if present
		while (anInputString.charAt(0) == '\''
				&& anInputString.charAt(anInputString.length() - 1) == '\'') {
			anInputString = anInputString.substring(1,
					anInputString.length() - 1);
		}

		String theResult;
		if (anInputString.matches("[0-9][0-9][0-9][0-9][0-9][0-9]")) {
			// HHMMSS
			theResult = anInputString.substring(0, 2) + ":"
					+ anInputString.substring(2, 4) + ":"
					+ anInputString.substring(4, 6);
		} else if (anInputString
				.matches("[0-9]{1,2}\\:[0-9]{1,2}\\:[0-9]{1,2}[.\\d+]*")) {
			// HH:MM:SS[.bla.bla...]
			StringTokenizer st = new StringTokenizer(anInputString, ":");
			String[] str1 = new String[st.countTokens()];
			for (int i = 0; st.hasMoreTokens(); i++) {
				str1[i] = st.nextToken();
				if (str1[i].length() == 1) {
					str1[i] = "0" + str1[i];
				}
			}

			theResult = str1[0] + ":" + str1[1] + ":" + str1[2];
		} else if (anInputString.matches("[0-9]{1,2}\\:[0-9]{1,2}")) {
			// HH:MM
			StringTokenizer st = new StringTokenizer(anInputString, ":");
			String[] str1 = new String[st.countTokens()];
			for (int i = 0; st.hasMoreTokens(); i++) {
				str1[i] = st.nextToken();
				if (str1[i].length() == 1) {
					str1[i] = "0" + str1[i];
				}
			}
			theResult = str1[0] + ":" + str1[1] + ":00";
		} else if (anInputString.matches("[0-9]{1,2}")) {
			// SS
			if (anInputString.length() == 1) {
				theResult = "00:00:0" + anInputString;
			} else {
				theResult = "00:00:" + anInputString;
			}
		} else {
			throw new XDBServerException("Invalid date/time format "
					+ anInputString);
		}

		theResult = "'" + theResult + "'";
		if (key_word.length() != 0) {
			theResult = key_word + theResult;
		}

		return theResult;
	}

	/**
	 * 
	 * 
	 * 
	 * @param anInputString
	 * 
	 * @return
	 * 
	 */

	public static String normalizeTimeStamp(String anInputString) {
		String theResult;
		String theDate = null;
		String theLongTime = null;
		String subseconds = "";
		String key_word = "";

		if (anInputString.toLowerCase().startsWith("timestamp")) {
			anInputString = anInputString.substring(9, anInputString.length())
					.trim();
			key_word = "timestamp";
		}
		// Strip quotes if present
		while (anInputString.charAt(0) == '\''
				&& anInputString.charAt(anInputString.length() - 1) == '\'') {
			anInputString = anInputString.substring(1,
					anInputString.length() - 1);
		}

		if (Props.XDB_SUBSECOND_PRECISION > 0) {
			subseconds = subsecondBaseString.substring(0,
					Props.XDB_SUBSECOND_PRECISION);
		}

		int spacePos = anInputString.indexOf(" ");
		theDate = normalizeDate(spacePos < 0 ? anInputString : anInputString
				.substring(0, spacePos));
		theDate = theDate.replaceAll("'", "");
		if (spacePos < 0) {
			if (Props.XDB_SUBSECOND_PRECISION > 0) {
				theLongTime = "00:00:00." + subseconds;
			} else {
				theLongTime = "00:00:00";
			}
		} else {
			String timePart = anInputString.substring(spacePos + 1).trim();
			String timeInfo;
			String zoneInfo = "";

			// See if we have "-" or "+" for time zone
			int dashPos = timePart.indexOf('-');
			if (dashPos < 0) {
				dashPos = timePart.indexOf('+');
			}
			if (dashPos >= 0) {
				timeInfo = timePart.substring(0, dashPos).trim();
				zoneInfo = timePart.substring(dashPos).trim();
			} else {
				int pos;
				// See if we have text for zone info like PST.
				for (pos = timePart.length() - 1; pos >= 0; pos--) {
					char c = timePart.charAt(pos);
					if (!(c >= 'A' && c <= 'Z' || c >= 'a' && c <= 'z')) {
						break;
					}
				}
				timeInfo = timePart.substring(0, pos + 1);
				zoneInfo = timePart.substring(timeInfo.length()).trim();
				timeInfo = timeInfo.trim();
			}

			int periodPos = timeInfo.indexOf(".");
			String theMls;
			if (periodPos < 0) {
				theMls = subseconds;
			} else {
				theMls = timeInfo.substring(periodPos + 1);
			}
			if (theMls.length() > Props.XDB_SUBSECOND_PRECISION) {
				throw new XDBServerException(
						"Invalid timestamp format, beyond max subsecond precision "
								+ anInputString);
			} else if (theMls.length() < Props.XDB_SUBSECOND_PRECISION) {
				theMls = theMls
						+ subseconds.substring(0, Props.XDB_SUBSECOND_PRECISION
								- theMls.length());
			}

			theLongTime = normalizeTime(periodPos < 0 ? timeInfo.trim()
					: timeInfo.substring(0, periodPos).trim());
			theLongTime = theLongTime.replaceAll("'", "");
			if (Props.XDB_SUBSECOND_PRECISION > 0) {
				theLongTime = theLongTime + '.' + theMls;
			}
			if (zoneInfo.length() > 0) {
				theLongTime += " " + zoneInfo;
			}
		}
		theResult = "'" + theDate + " " + theLongTime + "'";
		if (key_word.length() != 0) {
			theResult = key_word + theResult;
		}

		return theResult;
	}

	/**
	 * Normalize a Macaddr constant. Macaddr supports various customary formats,
	 * including '08002b:010203' '08002b-010203' '0800.2b01.0203'
	 * '08-00-2b-01-02-03' '08:00:2b:01:02:03' All of the above specify the same
	 * address. A good case for normalization is to strip the characters ':',
	 * '-', '.' from the input string
	 * 
	 * @param anInputString
	 * @return normalized macaddr
	 */
	public static String normalizeMacaddr(String anInputString) {
		String theResult;
		String theMacaddr = "";

		if (anInputString.toLowerCase().startsWith("macaddr")) {
			anInputString = anInputString.substring(7, anInputString.length())
					.trim();
		}
		theMacaddr = anInputString;
		theMacaddr = theMacaddr.replaceAll("-", "");
		theMacaddr = theMacaddr.replaceAll(":", "");
		theMacaddr = theMacaddr.replaceAll("\\.", "");

		theResult = "'" + theMacaddr + "'";

		return theResult;
	}

	private static String normalizeIPv4(String value)
			throws NumberFormatException {
		int a = 0;
		int b = 0;
		int c = 0;
		int d = 0;
		int pos = value.indexOf(".");
		if (pos > 0) {
			a = Integer.parseInt(value.substring(0, pos));
			value = value.substring(pos + 1);
			pos = value.indexOf(".");
			if (pos > 0) {
				b = Integer.parseInt(value.substring(0, pos));
				value = value.substring(pos + 1);
				pos = value.indexOf(".");
				if (pos > 0) {
					c = Integer.parseInt(value.substring(0, pos));
					value = value.substring(pos + 1);
					d = Integer.parseInt(value);
				} else {
					c = Integer.parseInt(value);
				}
			} else {
				b = Integer.parseInt(value);
			}
		} else {
			a = Integer.parseInt(value);
		}
		return a + "." + b + "." + c + "." + d;
	}

	private static String normalizeIPv6(String value)
			throws NumberFormatException {
		int[] address = new int[8];
		int curPos = 0;
		int doubleColonPos = -1;
		int pos = value.indexOf(":");
		while (pos >= 0) {
			if (pos == 0) {
				if (doubleColonPos != -1) {
					throw new NumberFormatException("Malformed IPv6 address");
				}
				doubleColonPos = curPos;
				// if address starts from :: move pos to second colon
				if (curPos == 0 && value.length() > 1 && value.charAt(1) == ':') {
					pos++;
				}
			} else {
				address[curPos++] = Integer.parseInt(value.substring(0, pos),
						16);
			}
			value = value.substring(pos + 1);
			pos = value.indexOf(":");
		}
		int prefix = 8;
		String ip4part = null;
		pos = value.indexOf(".");
		if (pos > 0) {
			// IPv4-IPv6 transition addresses normalize remaining as IPv4
			ip4part = normalizeIPv4(value);
			prefix = 6;
		} else if (value.length() > 0) {
			address[curPos++] = Integer.parseInt(value, 16);
		}
		// expand double colon
		if (doubleColonPos != -1) {
			int i = prefix - 1;
			for (curPos--; curPos >= doubleColonPos; curPos--, i--) {
				address[i] = address[curPos];
			}
			for (; i >= doubleColonPos; i--) {
				address[i] = 0;
			}
		}
		// Determine longest sequence of 0's
		int longestSeqStart = -1;
		int longestSeqLength = 0;
		int curSeqStart = -1;
		int curSeqLength = 0;
		for (int i = 0; i < prefix; i++) {
			if (address[i] == 0) {
				if (curSeqStart == -1) {
					curSeqStart = i;
				}
				curSeqLength++;
			} else {
				if (curSeqLength > 1 && curSeqLength > longestSeqLength) {
					longestSeqLength = curSeqLength;
					longestSeqStart = curSeqStart;
				}
				curSeqStart = -1;
				curSeqLength = 0;
			}
		}
		if (curSeqLength > 1 && curSeqLength > longestSeqLength) {
			longestSeqLength = curSeqLength;
			longestSeqStart = curSeqStart;
		}
		// Compose result
		String result = "";
		for (int i = 0; i < prefix; i++) {
			if (i == longestSeqStart) {
				if (result.length() == 0) {
					result = "::";
				} else {
					// result has colon at the end already
					result += ":";
				}
				i += longestSeqLength - 1;
			} else {
				result += Integer.toHexString(address[i]) + ":";
			}
		}
		if (ip4part == null) {
			if (result.endsWith("::")) {
				return result;
			} else {
				return result.substring(0, result.length() - 1);
			}
		} else {
			return result + ip4part;
		}
	}

	/**
	 * Normalize values of data type inet, normalized format is 'a.b.c.d/y'
	 * where a-d are integers from range 0..255, y is between 1..32 TODO support
	 * IPv6 addresses
	 * 
	 * @param anInputString
	 * @return
	 */
	public static String normalizeInet(String anInputString) {
		String value = anInputString.trim();
		if (value.startsWith("inet")) {
			value = value.substring(4).trim();
		}
		if (value.charAt(0) == '\'' && value.charAt(value.length() - 1) == '\'') {
			// Remove quotes
			value = value.substring(1, value.length() - 1);
		}
		int netmask = 0;
		String result;
		try {
			int pos = value.indexOf("/");
			if (pos > 0) {
				netmask = Integer.parseInt(value.substring(pos + 1));
				value = value.substring(0, pos);
			}
			if (value.indexOf(":") >= 0) {
				result = normalizeIPv6(value);
				if (netmask == 0 || netmask == 128) {
					return "'" + result + "'";
				} else {
					return "'" + result + "/" + netmask + "'";
				}
			} else {
				result = normalizeIPv4(value);
				if (netmask == 0 || netmask == 32) {
					return "'" + result + "'";
				} else {
					return "'" + result + "/" + netmask + "'";
				}
			}
		} catch (NumberFormatException nfe) {
			throw new XDBServerException("Invalid inet format: "
					+ anInputString);
		}
	}

	/**
	 * Normalize values of data type cidr, normalized format is 'a.b.c.d/y'
	 * where a-d are integers from range 0..255, y is between 1..32 TODO support
	 * IPv6 addresses
	 * 
	 * @param anInputString
	 * @return
	 */
	public static String normalizeCidr(String anInputString) {
		String value = anInputString.trim();
		if (value.startsWith("cidr")) {
			value = value.substring(4).trim();
		}
		if (value.charAt(0) == '\'' && value.charAt(value.length() - 1) == '\'') {
			// Remove quotes
			value = value.substring(1, value.length() - 1);
		}
		int netmask = 0;
		String result;
		try {
			int pos = value.indexOf("/");
			if (pos > 0) {
				netmask = Integer.parseInt(value.substring(pos + 1));
				value = value.substring(0, pos);
			}
			if (value.indexOf(":") >= 0) {
				result = normalizeIPv6(value);
				if (netmask == 0) {
					return "'" + result + "/128" + "'";
				} else {
					return "'" + result + "/" + netmask + "'";
				}
			} else {
				result = normalizeIPv4(value);
				if (netmask == 0) {
					return "'" + result + "/32" + "'";
				} else {
					return "'" + result + "/" + netmask + "'";
				}
			}
		} catch (NumberFormatException nfe) {
			throw new XDBServerException("Invalid cidr format: "
					+ anInputString);
		}
	}

	/**
	 * 
	 * 
	 * 
	 * @param anExp1
	 * 
	 * @param anExp2
	 * 
	 * @return
	 * 
	 */

	public static boolean checkCompatibilityForUnion(SqlExpression anExp1,
			SqlExpression anExp2) {
		if (anExp1.getExprDataType() == null
				|| anExp2.getExprDataType() == null) {
			return true;
		}
		int expType1 = anExp1.getExprDataType().type;
		int expType2 = anExp2.getExprDataType().type;
		switch (expType1) {
		case ExpressionType.CHAR_TYPE:
		case ExpressionType.VARCHAR_TYPE:
			switch (expType2) {
			case ExpressionType.VARCHAR_TYPE:
			case ExpressionType.CHAR_TYPE:
				return true;
			default:
				return false;
			}
		case ExpressionType.INTERVAL_TYPE:
			switch (expType2) {
			case ExpressionType.INTERVAL_TYPE:
				return true;
			default:
				return false;
			}

		case ExpressionType.TIME_TYPE:
			switch (expType2) {
			case ExpressionType.TIME_TYPE:
				return true;
			default:
				return false;
			}
		case ExpressionType.TIMESTAMP_TYPE:
			switch (expType2) {
			case ExpressionType.TIMESTAMP_TYPE:
				return true;
			default:
				return false;
			}
		case ExpressionType.DATE_TYPE:
			switch (expType2) {
			case ExpressionType.DATE_TYPE:
				return true;
			default:
				return false;
			}
		case ExpressionType.DECIMAL_TYPE:
		case ExpressionType.NUMERIC_TYPE:
		case ExpressionType.REAL_TYPE:
		case ExpressionType.DOUBLEPRECISION_TYPE:
		case ExpressionType.INT_TYPE:
		case ExpressionType.SMALLINT_TYPE:
			switch (expType2) {
			case ExpressionType.DECIMAL_TYPE:
			case ExpressionType.NUMERIC_TYPE:
			case ExpressionType.REAL_TYPE:
			case ExpressionType.DOUBLEPRECISION_TYPE:
			case ExpressionType.INT_TYPE:
			case ExpressionType.SMALLINT_TYPE:
				return true;
			default:
				return false;
			}
		case ExpressionType.BOOLEAN_TYPE:
			switch (expType2) {
			case ExpressionType.BOOLEAN_TYPE:
				return true;
			default:
				return false;
			}

		default:
			return true;
		}
	}

	/**
	 * Replaces the AttributeColumn in a SqlExpression with an equivalent one
	 * found in columnList. This helps with duplicated column names.
	 * 
	 * @param columnList
	 *            The query's projection column list
	 */
	protected void replaceColumnInExpression(List<AttributeColumn> columnList) {
		final String method = "replaceColumnInExpression";
		logger.entering(method);

		try {
			for (SqlExpression aSqlExpr : getNodes(this,
					SqlExpression.SQLEX_COLUMN)) {
				AttributeColumn column = aSqlExpr.getColumn();

				// skip if we already have equivalency
				if (columnList.contains(column)) {
					return;
				}

				// Now, see if we have something equivalent in list
				for (AttributeColumn projAC : columnList) {
					if (column.isEquivalent(projAC)
							&& column.relationNode == projAC.relationNode) {
						// these appear to be interchangable, just use one
						// instance
						aSqlExpr.setColumn(projAC);
					}
				}
			}
		} finally {
			logger.exiting(method);
		}
	}

	/**
	 * @return whether or not this expression contains a subquery.
	 */
	public boolean hasSubQuery() {
		switch (getExprType()) {
		case SQLEX_SUBQUERY:
			return true;
		case SQLEX_FUNCTION:
			for (SqlExpression param : functionParams) {
				if (param.hasSubQuery()) {
					return true;
				}
			}
			return false;
		case SQLEX_CASE:
			if (getCaseConstruct().getDefaultexpr().hasSubQuery()) {
				return true;
			}
			for (Map.Entry<QueryCondition, SqlExpression> caseEntry : getCaseConstruct().aHashtable
					.entrySet()) {
				if (!caseEntry.getKey().isSimple()
						|| caseEntry.getValue().hasSubQuery()) {
					return true;
				}
			}
			return false;
		case SQLEX_CONDITION:
			return getQueryCondition().isSimple();
		default:
			return false;
		}
	}

	/**
	 * @return a converted constant value, so we can evaluate consistently for
	 *         partitioning.
	 */
	public String getNormalizedValue() {
		if (exprType == SQLEX_CONSTANT) {
			if (constantValue != null) {
				String normalized = constantValue.trim();
				// constants may come in as 'constant'- strip single quotes
				while (normalized.length() > 1 && normalized.startsWith("'")
						&& normalized.endsWith("'")) {
					normalized = normalized.substring(1,
							normalized.length() - 1);
				}
				// constant may be a negative number, prepend the minus sign
				if ("-".equals(unaryOperator)) {
					normalized = unaryOperator + normalized;
				}
				Number aNumber = null;
				switch (exprDataType.type) {
				// Integer data types
				case Types.BIGINT:
					// fall through
				case Types.INTEGER:
					// fall through
				case Types.SMALLINT:
					// fall through
				case Types.TINYINT:
					// fall through
					// Decimal types
				case Types.DECIMAL:
					// fall through
				case Types.NUMERIC:
					// fall through
					// Float point types
				case Types.DOUBLE:
					// fall through
				case Types.FLOAT:
					// fall through
				case Types.REAL:
					aNumber = new BigDecimal(normalized);
					return normalizeNumber(aNumber.toString(),
							exprDataType.scale);
					// Date/time data types
				case Types.DATE:
					return normalizeDate(constantValue);
				case Types.TIME:
					return normalizeTime(constantValue);
				case Types.TIMESTAMP:
					return normalizeTimeStamp(constantValue);
					// Text data types
				case Types.CHAR:
					// fall through
				case Types.VARCHAR:
					if (getExprDataType().length > 0
							&& normalized.length() > getExprDataType().length) {
						normalized = normalized.substring(0,
								getExprDataType().length);
					}
					return normalized;
					// not supported, or undecided leave "as is"
				case ExpressionType.MACADDR_TYPE:
					return normalizeMacaddr(constantValue);
				case ExpressionType.INET_TYPE:
					return normalizeInet(constantValue);
				case ExpressionType.CIDR_TYPE:
					return normalizeCidr(constantValue);
				case Types.ARRAY:
					// fall through
				case Types.BINARY:
					// fall through
				case Types.BIT:
					// fall through
				case Types.BLOB:
					// fall through
				case Types.BOOLEAN:
					// fall through
				case Types.CLOB:
					// fall through
				case Types.DATALINK:
					// fall through
				case Types.DISTINCT:
					// fall through
				case Types.JAVA_OBJECT:
					// fall through
				case Types.LONGVARBINARY:
					// fall through
				case Types.LONGVARCHAR:
					// fall through
				case Types.NULL:
					// fall through
				case Types.OTHER:
					// fall through
				case Types.REF:
					// fall through
				case Types.STRUCT:
					// fall through
				case Types.VARBINARY:
					// fall through
				default:
				}
			}
		}
		return constantValue;
	}

	/**
	 * 
	 * Creates an SqlExpression of type SQLEX_FUNCTION
	 * 
	 * @param functionName
	 *            Name of the function
	 * @param functionId
	 *            IFunction.FunctionId of the function
	 * 
	 * @return SqlExpression of type SQLEX_FUNCTION for the input parameters
	 * 
	 */
	public static SqlExpression createNewTempFunction(String functionName,
			int functionId) {
		SqlExpression aNewSE = new SqlExpression();
		aNewSE.setExprType(SqlExpression.SQLEX_FUNCTION);
		aNewSE.functionName = functionName;
		aNewSE.functionId = functionId;
		aNewSE.setTempExpr(true);

		return aNewSE;
	}

	/**
	 * 
	 * Creates an SqlExpression of type SQLEX_OPERATOR_EXPRESSION
	 * 
	 * @param op
	 *            Operator
	 * @param leftExpr
	 *            Left operand
	 * @param rightExpr
	 *            Right operand
	 * 
	 * @return SqlExpression of type SQLEX_OPERATOR_EXPRESSION for the input
	 *         parameters
	 * 
	 */
	public static SqlExpression createNewTempOpExpression(String op,
			SqlExpression leftExpr, SqlExpression rightExpr) {
		SqlExpression aNewSE = new SqlExpression();
		aNewSE.setExprType(SqlExpression.SQLEX_OPERATOR_EXPRESSION);
		aNewSE.operator = op;
		aNewSE.leftExpr = leftExpr;
		aNewSE.rightExpr = rightExpr;
		aNewSE.setTempExpr(true);

		return aNewSE;
	}

	/**
	 * 
	 * Creates an SqlExpression of type SQLEX_CONSTANT
	 * 
	 * @param value
	 *            Constant value
	 * @param exprType
	 *            Desired expression type
	 * 
	 * @return SqlExpression of type SQLEX_CONSTANT for the input parameters
	 * 
	 */
	public static SqlExpression createConstantExpression(String value,
			ExpressionType exprType) {
		SqlExpression expr = new SqlExpression();
		expr.setExprType(SQLEX_CONSTANT);
		expr.setExprDataType(exprType);
		expr.setConstantValue(value);
		return expr;
	}

	/**
	 * 
	 * Checks expression to see if it contains any marked as deferred, for
	 * distinct aggregates
	 * 
	 * @return Collection of deferred expressions
	 * 
	 */
	public Collection<SqlExpression> getDeferredExpressions() {

		ArrayList<SqlExpression> list = new ArrayList<SqlExpression>();

		if (this.isDeferredGroup()) {
			list.add(this);
		} else if ((this.getExprType() & SQLEX_FUNCTION) > 0) {
			for (SqlExpression aSqlExpression : this.functionParams) {
				list.addAll(aSqlExpression.getDeferredExpressions());
			}
		} else if ((this.getExprType() & SQLEX_CASE) > 0) {
			for (SqlExpression aSqlExpression : caseConstruct
					.getSQLExpressions()) {
				list.addAll(aSqlExpression.getDeferredExpressions());
			}
		} else if ((this.getExprType() & SQLEX_SUBQUERY) > 0) {
			if ((this.subqueryTree.getQueryType() & QueryTree.SCALAR) == 0) {
				for (SqlExpression aSqlExpression : this.parentContainerNode
						.getProjectionList()) {
					list.addAll(aSqlExpression.getDeferredExpressions());
				}
			}
		}

		if (this.leftExpr != null) {
			list.addAll(leftExpr.getDeferredExpressions());
		}
		if (this.rightExpr != null) {
			list.addAll(rightExpr.getDeferredExpressions());
		}

		return list;
	}

	/**
	 * @param aggAlias
	 *            the aggAlias to set
	 */
	public void setAggAlias(String aggAlias) {
		this.aggAlias = aggAlias;
	}

	/**
	 * @return the aggAlias
	 */
	public String getAggAlias() {
		return aggAlias;
	}

	/**
	 * @param alias
	 *            the alias to set
	 */
	public void setAlias(String alias) {
		this.alias = alias;
	}

	/**
	 * @return the alias
	 */
	public String getAlias() {
		return alias;
	}

	/**
	 * @param aQueryCondition
	 *            the aQueryCondition to set
	 */
	public void setQueryCondition(QueryCondition aQueryCondition) {
		this.aQueryCondition = aQueryCondition;
	}

	/**
	 * @return the aQueryCondition
	 */
	public QueryCondition getQueryCondition() {
		return aQueryCondition;
	}

	/**
	 * @param argSeparator
	 *            the argSeparator to set
	 */
	public void setArgSeparator(String argSeparator) {
		this.argSeparator = argSeparator;
	}

	/**
	 * @return the argSeparator
	 */
	public String getArgSeparator() {
		return argSeparator;
	}

	/**
	 * @return the caseConstruct
	 */
	public SCase getCaseConstruct() {
		return caseConstruct;
	}

	/**
	 * @param caseConstruct
	 *            the caseConstruct to set
	 */
	public void setCaseConstruct(SCase caseConstruct) {
		this.caseConstruct = caseConstruct;
	}

	/**
	 * @param column
	 *            the column to set
	 */
	public void setColumn(AttributeColumn column) {
		this.column = column;
	}

	/**
	 * @return the column
	 */
	public AttributeColumn getColumn() {
		return column;
	}

	/**
	 * @param constantValue
	 *            the constantValue to set
	 */
	public void setConstantValue(String constantValue) {
		this.constantValue = constantValue;
	}

	/**
	 * @return the constantValue
	 */
	public String getConstantValue() {
		return constantValue;
	}

	/**
	 * @param exprDataType
	 *            the exprDataType to set
	 */
	public void setExprDataType(ExpressionType exprDataType) {
		this.exprDataType = exprDataType;
	}

	/**
	 * @return the exprDataType
	 */
	public ExpressionType getExprDataType() {
		return exprDataType;
	}

	/**
	 * @param expressionList
	 *            the expressionList to set
	 */
	public void setExpressionList(List<SqlExpression> expressionList) {
		this.expressionList = expressionList;
	}

	/**
	 * @return the expressionList
	 */
	public List<SqlExpression> getExpressionList() {
		return expressionList;
	}

	/**
	 * @param exprString
	 *            the exprString to set
	 */
	public void setExprString(String exprString) {
		this.exprString = exprString;
	}

	/**
	 * @return the exprString
	 */
	public String getExprString() {
		return exprString;
	}

	/**
	 * @param exprType
	 *            the exprType to set
	 */
	public void setExprType(int exprType) {
		this.exprType = exprType;
	}

	/**
	 * @return the exprType
	 */
	public int getExprType() {
		return exprType;
	}

	/**
	 * @param expTypeOfCast
	 *            the expTypeOfCast to set
	 */
	public void setExpTypeOfCast(DataTypeHandler expTypeOfCast) {
		this.expTypeOfCast = expTypeOfCast;
	}

	/**
	 * @param functionId
	 *            the functionId to set
	 */
	public void setFunctionId(int functionId) {
		this.functionId = functionId;
	}

	/**
	 * @return the functionId
	 */
	public int getFunctionId() {
		return functionId;
	}

	/**
	 * @param functionName
	 *            the functionName to set
	 */
	public void setFunctionName(String functionName) {
		this.functionName = functionName;
	}

	/**
	 * @return the functionName
	 */
	public String getFunctionName() {
		return functionName;
	}

	/**
	 * @param functionParams
	 *            the functionParams to set
	 */
	public void setFunctionParams(List<SqlExpression> functionParams) {
		this.functionParams = functionParams;
	}

	/**
	 * @return the functionParams
	 */
	public List<SqlExpression> getFunctionParams() {
		return functionParams;
	}

	/**
	 * @param isAdded
	 *            the isAdded to set
	 */
	public void setAdded(boolean isAdded) {
		this.isAdded = isAdded;
	}

	/**
	 * @return the isAdded
	 */
	public boolean isAdded() {
		return isAdded;
	}

	/**
	 * @param isAllCountGroupFunction
	 *            the isAllCountGroupFunction to set
	 */
	public void setAllCountGroupFunction(boolean isAllCountGroupFunction) {
		this.isAllCountGroupFunction = isAllCountGroupFunction;
	}

	/**
	 * @return the isAllCountGroupFunction
	 */
	public boolean isAllCountGroupFunction() {
		return isAllCountGroupFunction;
	}

	/**
	 * @param isDeferredGroup
	 *            the isDeferredGroup to set
	 */
	public void setDeferredGroup(boolean isDeferredGroup) {
		this.isDeferredGroup = isDeferredGroup;
	}

	/**
	 * @return the isDeferredGroup
	 */
	public boolean isDeferredGroup() {
		return isDeferredGroup;
	}

	/**
	 * @param isDistinctExtraGroup
	 *            the isDistinctExtraGroup to set
	 */
	public void setDistinctExtraGroup(boolean isDistinctExtraGroup) {
		this.isDistinctExtraGroup = isDistinctExtraGroup;
	}

	/**
	 * @return the isDistinctExtraGroup
	 */
	public boolean isDistinctExtraGroup() {
		return isDistinctExtraGroup;
	}

	/**
	 * @param isDistinctGroupFunction
	 *            the isDistinctGroupFunction to set
	 */
	public void setDistinctGroupFunction(boolean isDistinctGroupFunction) {
		this.isDistinctGroupFunction = isDistinctGroupFunction;
	}

	/**
	 * @return the isDistinctGroupFunction
	 */
	public boolean isDistinctGroupFunction() {
		return isDistinctGroupFunction;
	}

	/**
	 * @return true if DISTINCT clause is on the partitioned column
	 */
	public boolean isDistinctGroupFunctionOnPartitionedCol(SysDatabase database) {
		if (isDistinctGroupFunction) {
			AttributeColumn anAC = getFunctionParams().get(0).getColumn();
			if (anAC != null) {
				return anAC.getSysTable(database).isPartitionedColumn(
						anAC.columnName);
			}
		}
		return false;
	}

	/**
	 * @param leftExpr
	 *            the leftExpr to set
	 */
	public void setLeftExpr(SqlExpression leftExpr) {
		this.leftExpr = leftExpr;
	}

	/**
	 * @return the leftExpr
	 */
	public SqlExpression getLeftExpr() {
		return leftExpr;
	}

	/**
	 * @param mapped
	 *            the mapped to set
	 */
	public void setMapped(int mapped) {
		this.mapped = mapped;
	}

	/**
	 * @return the mapped
	 */
	public int getMapped() {
		return mapped;
	}

	/**
	 * @param mappedExpression
	 *            the mappedExpression to set
	 */
	public void setMappedExpression(SqlExpression mappedExpression) {
		this.mappedExpression = mappedExpression;
	}

	/**
	 * @return the mappedExpression
	 */
	public SqlExpression getMappedExpression() {
		return mappedExpression;
	}

	/**
	 * @param needParenthesisInFunction
	 *            the needParenthesisInFunction to set
	 */
	public void setNeedParenthesisInFunction(boolean needParenthesisInFunction) {
		this.needParenthesisInFunction = needParenthesisInFunction;
	}

	/**
	 * @return the needParenthesisInFunction
	 */
	public boolean needParenthesisInFunction() {
		return needParenthesisInFunction;
	}

	/**
	 * @param operandSign
	 *            the operandSign to set
	 */
	public void setOperandSign(String operandSign) {
		this.operandSign = operandSign;
	}

	/**
	 * @return the operandSign
	 */
	public String getOperandSign() {
		return operandSign;
	}

	/**
	 * @param operator
	 *            the operator to set
	 */
	public void setOperator(String operator) {
		this.operator = operator;
	}

	/**
	 * @return the operator
	 */
	public String getOperator() {
		return operator;
	}

	/**
	 * @param outerAlias
	 *            the outerAlias to set
	 */
	public String setOuterAlias(String outerAlias) {
		return this.outerAlias = outerAlias;
	}

	/**
	 * @return the outerAlias
	 */
	public String getOuterAlias() {
		return outerAlias;
	}

	/**
	 * @param paramNumber
	 *            the paramNumber to set
	 */
	public void setParamNumber(int paramNumber) {
		this.paramNumber = paramNumber;
		exprString = "&xp" + paramNumber + "xp&";
	}

	/**
	 * @return the paramNumber
	 */
	public int getParamNumber() {
		return paramNumber;
	}

	/**
	 * @param paramValue
	 *            the paramValue to set
	 */
	public void setParamValue(String paramValue) {
		this.paramValue = paramValue;
		if (paramValue == null) {
			exprString = "null";
		} else if (exprDataType != null
				&& (exprDataType.type == ExpressionType.VARCHAR_TYPE
						|| exprDataType.type == ExpressionType.CHAR_TYPE
						|| exprDataType.type == ExpressionType.TIMESTAMP_TYPE
						|| exprDataType.type == ExpressionType.DATE_TYPE
						|| exprDataType.type == ExpressionType.TIME_TYPE || exprDataType.type == Types.OTHER)) {
			exprString = "'" + ParseCmdLine.escape(paramValue) + "'";
		} else {
			exprString = paramValue;
		}
	}

	/**
	 * @return the paramValue
	 */
	public String getParamValue() {
		return paramValue;
	}

	/**
	 * @param projectionLabel
	 *            the projectionLabel to set
	 */
	public void setProjectionLabel(String projectionLabel) {
		this.projectionLabel = projectionLabel;
	}

	/**
	 * @return the projectionLabel
	 */
	public String getProjectionLabel() {
		return projectionLabel;
	}

	/**
	 * @param rightExpr
	 *            the rightExpr to set
	 */
	public void setRightExpr(SqlExpression rightExpr) {
		this.rightExpr = rightExpr;
	}

	/**
	 * @return the rightExpr
	 */
	public SqlExpression getRightExpr() {
		return rightExpr;
	}

	/**
	 * @param subqueryTree
	 *            the subqueryTree to set
	 */
	public void setSubqueryTree(QueryTree subqueryTree) {
		this.subqueryTree = subqueryTree;
	}

	/**
	 * @return the subqueryTree
	 */
	public QueryTree getSubqueryTree() {
		return subqueryTree;
	}

	/**
	 * @param unaryOperator
	 *            the unaryOperator to set
	 */
	public void setUnaryOperator(String unaryOperator) {
		this.unaryOperator = unaryOperator;
	}

	/**
	 * @return the unaryOperator
	 */
	public String getUnaryOperator() {
		return unaryOperator;
	}

	// Semantic information for CASE
	public class SCase implements IRebuildString {
		private SqlExpression defaultexpr = null;

		// Contains mapping of QueryConditions
		private Hashtable<QueryCondition, SqlExpression> aHashtable;

		/**
		 * 
		 * This function does a type check and then sets the datatype of the
		 * case expression to the sql expressions that we are going to get from
		 * the case expression
		 * 
		 * 
		 * @param database
		 * 
		 * @throws org.postgresql.stado.exception.ColumnNotFoundException
		 * 
		 * @return
		 * 
		 */

		public ExpressionType setDataType(SysDatabase database)
				throws ColumnNotFoundException {
			boolean isNumPresent = true;
			boolean isNumPrevious = true;
			ExpressionType toReturn = null;
			ExpressionType aExpressionType = null;
			String isPresentExpr = "";
			String isPreviousExpr = "";
			for (QueryCondition qc : aHashtable.keySet()) {
				for (QueryCondition aSqlExprCond : QueryCondition.getNodes(qc,
						QueryCondition.QC_SQLEXPR)) {
					SqlExpression aSqlExpression = aSqlExprCond.getExpr();
					SqlExpression.setExpressionResultType(aSqlExpression,
							database);
				}
				SqlExpression aSqlExpression = aHashtable.get(qc);
				aExpressionType = SqlExpression.setExpressionResultType(
						aSqlExpression, database);

				// Set the expression type to the first one
				if (toReturn == null) {
					if (aExpressionType.type == ExpressionType.NULL_TYPE) {
						continue;
					}
					toReturn = aExpressionType;
					isNumPresent = aExpressionType.isNumeric();
					isPresentExpr = aSqlExpression.rebuildString();
				} else {
					isNumPrevious = isNumPresent;
					isPreviousExpr = isPresentExpr;
					isNumPresent = aExpressionType.isNumeric();
					isPresentExpr = aSqlExpression.rebuildString();
					if (isNumPrevious != isNumPresent) {
						String errorMessage = ErrorMessageRepository.CASE_STATEMENT_TYPE_MISMATCH
								+ "("
								+ isPreviousExpr
								+ " <--> "
								+ isPresentExpr + ")";

						throw new XDBServerException(
								errorMessage,
								0,
								ErrorMessageRepository.CASE_STATEMENT_TYPE_MISMATCH_CODE);
					}
				}
			}

			if (defaultexpr != null) {
				aExpressionType = SqlExpression.setExpressionResultType(
						defaultexpr, database);
				if (toReturn != null
						&& aExpressionType.type != ExpressionType.NULL_TYPE) {
					boolean defExprType = aExpressionType.isNumeric();
					if (defExprType == isNumPresent) {
						return aExpressionType;
					} else {
						String errorMessage = ErrorMessageRepository.CASE_STATEMENT_TYPE_MISMATCH
								+ "("
								+ isPresentExpr
								+ " <--> "
								+ defaultexpr.rebuildString() + ")";
						throw new XDBServerException(
								errorMessage,
								0,
								ErrorMessageRepository.CASE_STATEMENT_TYPE_MISMATCH_CODE);

					}
				}
			}
			return toReturn == null ? aExpressionType /* NULL_TYPE */
			: toReturn;
		}

		/**
		 * 
		 * 
		 * 
		 * @return
		 * 
		 */

		public Collection<SqlExpression> getSQLExpressions() {
			Collection<SqlExpression> exprList = new LinkedList<SqlExpression>();
			if (aHashtable != null) {
				for (QueryCondition qc : aHashtable.keySet()) {
					for (QueryCondition qcSqlExpr : QueryCondition.getNodes(qc,
							QueryCondition.QC_SQLEXPR)) {
						exprList.add(qcSqlExpr.getExpr());
					}
				}
				exprList.addAll(aHashtable.values());
			}
			if (defaultexpr != null) {
				exprList.add(defaultexpr);
			}
			return exprList;
		}

		public SCase() {
			aHashtable = new Hashtable<QueryCondition, SqlExpression>();

		}

		public Map<QueryCondition, SqlExpression> getCases() {
			return aHashtable;
		}

		/**
		 * 
		 * 
		 * 
		 * @param qc
		 * 
		 * @param aSqlExpression
		 * 
		 */

		public void addCase(QueryCondition qc, SqlExpression aSqlExpression) {
			aHashtable.put(qc, aSqlExpression);
		}

		/**
		 * 
		 * 
		 * 
		 * @param qc
		 * 
		 * @return
		 * 
		 */

		public SqlExpression getCaseSqlExpr(QueryCondition qc) {
			return aHashtable.get(qc);
		}

		/**
		 * 
		 * 
		 * 
		 * @return
		 * 
		 */

		public String rebuildString() {
			if (aHashtable == null) {
				return "";
			}

			String caseExprString = "";
			caseExprString = "( CASE ";
			for (QueryCondition qc : aHashtable.keySet()) {
				qc.rebuildCondString();
				SqlExpression aSqlExpression = getCaseSqlExpr(qc);
				aSqlExpression.rebuildExpression();
				caseExprString = caseExprString + "  WHEN ( "
						+ qc.getCondString() + " )  THEN ( "
						+ aSqlExpression.getExprString() + " )  ";
			}
			if (defaultexpr != null) {
				defaultexpr.rebuildExpression();
				caseExprString = caseExprString + " else "
						+ defaultexpr.getExprString();
			}
			caseExprString += " end)";

			return caseExprString;
		}

		/**
		 * @param defaultexpr
		 *            the defaultexpr to set
		 */
		public void setDefaultexpr(SqlExpression defaultexpr) {
			this.defaultexpr = defaultexpr;
		}

		/**
		 * @return the defaultexpr
		 */
		public SqlExpression getDefaultexpr() {
			return defaultexpr;
		}

	}

	/**
	 * @param AttributeColumn
	 *            the column to search for
	 * 
	 * @return true if the column is present in this expression
	 */
	public boolean hasColumnInExpression(AttributeColumn col) {
		switch (this.exprType) {
		case SQLEX_FUNCTION: {
			if (functionParams != null) {
				int len = functionParams.size();
				for (int index = 0; index < len; index++) {
					SqlExpression aSqlExpr = functionParams.get(index);
					if (aSqlExpr.hasColumnInExpression(col)) {
						return true;
					}
				}
			}
			break;
		}
		case SQLEX_COLUMN: {
			if (column.columnName == col.columnName
					&& (col.getTableAlias().equals("") || column.getTableName() == col
							.getTableName())) {
				return true;
			}
		}
		}
		return false;
	}

	/**
	 * @param AttributeColumn
	 *            the column to compare
	 * 
	 * @return
	 */
	public boolean isAlliasSameAsColumnNameInFunction(AttributeColumn col) {
		if (exprType == SQLEX_FUNCTION) {
			if (functionParams != null) {
				int len = functionParams.size();
				for (int index = 0; index < len; index++) {
					SqlExpression aSqlExpr = functionParams.get(index);
					if (aSqlExpr.hasColumnInExpression(col)) {
						return true;
					}
				}
			}
		}
		return false;
	}

	/**
	 * @param SqlExpression
	 *            to compare
	 * 
	 * @return
	 */
	public boolean isSameFunction(SqlExpression aSqlExpression) {
		if (this.exprType == SqlExpression.SQLEX_FUNCTION
				&& aSqlExpression.exprType == SqlExpression.SQLEX_FUNCTION) {
			if (this.functionId == aSqlExpression.functionId) {
				if (this.outerAlias.equalsIgnoreCase(aSqlExpression.outerAlias)) {
					if (this.exprString
							.equalsIgnoreCase(aSqlExpression.exprString)) {
						return true;
					}
				}
			}
		}
		return false;
	}

	/**
	 * 
	 * @return true if this is a column
	 */
	public boolean isColumn() {
		return exprType == SqlExpression.SQLEX_COLUMN;
	}

	/**
     *
     */

	public void moveDown(RelationNode node) {
		for (SqlExpression expr : getNodes(this, SQLEX_COLUMN)) {
			if (expr.mappedExpression != null && expr.column != null
					&& node.equals(expr.column.relationNode)) {
				copy(expr.mappedExpression, expr);
			}
		}
	}
}
