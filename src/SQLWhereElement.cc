/* Asura
   Copyright (C) 2013 MIRACLE LINUX CORPORATION
 
   This program is free software: you can redistribute it and/or modify
   it under the terms of the GNU General Public License as published by
   the Free Software Foundation, either version 3 of the License, or
   (at your option) any later version.

   This program is distributed in the hope that it will be useful,
   but WITHOUT ANY WARRANTY; without even the implied warranty of
   MERCHANTABILITY or FITNESS FOR A PARTICULAR PURPOSE.  See the
   GNU General Public License for more details.

   You should have received a copy of the GNU General Public License
   along with this program.  If not, see <http://www.gnu.org/licenses/>.
*/

#include <cstdio>
#include <string>
#include <stdexcept>
#include <typeinfo>

#include "StringUtils.h"
using namespace mlpl;

#include "ItemEnum.h"
#include "Utils.h"
#include "SQLWhereElement.h"


// ---------------------------------------------------------------------------
// methods (SQLWhereOperator)
// ---------------------------------------------------------------------------
SQLWhereOperator::SQLWhereOperator(SQLWhereOperatorType type,
                                   SQLWhereOperatorPriority prio)
: m_type(type),
  m_priority(prio)
{
}

SQLWhereOperator::~SQLWhereOperator()
{
}

bool SQLWhereOperator::priorityOver(SQLWhereOperator *whereOp)
{
	return m_priority < whereOp->m_priority;
}

//
// Protected methods
//
const SQLWhereOperatorType SQLWhereOperator::getType(void) const
{
	return m_type;
}

bool SQLWhereOperator::checkType(SQLWhereElement *elem,
                                 SQLWhereElementType type) const
{
	if (!elem) {
		MLPL_DBG("elem is NULL\n");
		return false;
	}
	if (elem->getType() != type) {
		MLPL_DBG("type(%d) is not expected(%d)\n",
		         elem->getType(), type);
		return false;
	}
	return true;
}

// ---------------------------------------------------------------------------
// class: SQLWhereOperatorEqual
// ---------------------------------------------------------------------------
SQLWhereOperatorEqual::SQLWhereOperatorEqual()
: SQLWhereOperator(SQL_WHERE_OP_EQ, SQL_WHERE_OP_PRIO_EQ)
{
}

SQLWhereOperatorEqual::~SQLWhereOperatorEqual()
{
}

bool SQLWhereOperatorEqual::evaluate(SQLWhereElement *leftHand,
                                     SQLWhereElement *rightHand)
{
	if (!checkType(leftHand, SQL_WHERE_ELEM_COLUMN))
		return false;
	if (!rightHand)
		return false;
	ItemDataPtr data0 = leftHand->getItemData();
	ItemDataPtr data1 = rightHand->getItemData();
	return (*data0 == *data1);
}

// ---------------------------------------------------------------------------
// class: SQLWhereOperatorAnd
// ---------------------------------------------------------------------------
SQLWhereOperatorAnd::SQLWhereOperatorAnd()
: SQLWhereOperator(SQL_WHERE_OP_EQ, SQL_WHERE_OP_PRIO_AND)
{
}

SQLWhereOperatorAnd::~SQLWhereOperatorAnd()
{
}

bool SQLWhereOperatorAnd::evaluate(SQLWhereElement *leftHand,
                                   SQLWhereElement *rightHand)
{
	bool ret = leftHand->evaluate() && rightHand->evaluate();
	return ret;
}

// ---------------------------------------------------------------------------
// class: SQLWhereOperatorBetween
// ---------------------------------------------------------------------------
SQLWhereOperatorBetween::SQLWhereOperatorBetween()
: SQLWhereOperator(SQL_WHERE_OP_BETWEEN, SQL_WHERE_OP_PRIO_BETWEEN)
{
}

SQLWhereOperatorBetween::~SQLWhereOperatorBetween()
{
}

bool SQLWhereOperatorBetween::evaluate(SQLWhereElement *leftHand,
                                       SQLWhereElement *rightHand)
{
	if (!checkType(leftHand, SQL_WHERE_ELEM_COLUMN))
		return false;
	if (!checkType(rightHand, SQL_WHERE_ELEM_PAIRED_NUMBER))
		return false;
	SQLWherePairedNumber *pairedElem =
	  dynamic_cast<SQLWherePairedNumber *>(rightHand);
	ItemDataPtr dataColumn = leftHand->getItemData();
	ItemDataPtr dataRight0 = pairedElem->getItemData(0);
	ItemDataPtr dataRight1 = pairedElem->getItemData(1);
	return (*dataColumn >= *dataRight0 && *dataColumn <= *dataRight1);
}

// ---------------------------------------------------------------------------
// Public methods (SQLWhereElement)
// ---------------------------------------------------------------------------
SQLWhereElement::SQLWhereElement(SQLWhereElementType elemType)
: m_type(elemType),
  m_leftHand(NULL),
  m_operator(NULL),
  m_rightHand(NULL),
  m_parent(NULL)
{
}

SQLWhereElement::~SQLWhereElement()
{
	if (m_leftHand)
		delete m_leftHand;
	if (m_operator)
		delete m_operator;
	if (m_rightHand)
		delete m_rightHand;
}

SQLWhereElementType SQLWhereElement::getType(void) const
{
	return m_type;
}

SQLWhereElement *SQLWhereElement::getLeftHand(void) const
{
	return m_leftHand;
}

SQLWhereElement *SQLWhereElement::getRightHand(void) const
{
	return m_rightHand;
}

SQLWhereOperator *SQLWhereElement::getOperator(void) const
{
	return m_operator;
}

SQLWhereElement *SQLWhereElement::getParent(void) const
{
	return m_parent;
}

void SQLWhereElement::setLeftHand(SQLWhereElement *whereElem)
{
	m_leftHand = whereElem;
	whereElem->m_parent = this;
}

void SQLWhereElement::setRightHand(SQLWhereElement *whereElem)
{
	m_rightHand = whereElem;
	whereElem->m_parent = this;
}

void SQLWhereElement::setOperator(SQLWhereOperator *whereOp)
{
	m_operator = whereOp;
}

bool SQLWhereElement::isFull(void)
{
	if (!m_leftHand)
		return false;
	if (!m_operator)
		return false;
	if (!m_rightHand)
		return false;
	return true;
}

bool SQLWhereElement::isEmpty(void)
{
	if (m_leftHand)
		return false;
	if (m_operator)
		return false;
	if (m_rightHand)
		return false;
	return true;
}

bool SQLWhereElement::evaluate(void)
{
	if (!m_operator) {
		string msg;
		TRMSG(msg, "m_operator is NULL.");
		throw logic_error(msg);
	}
	bool ret = m_operator->evaluate(m_leftHand, m_rightHand);
	return ret;
}

ItemDataPtr SQLWhereElement::getItemData(int index)
{
	string className = typeid(*this).name();
	MLPL_WARN("This function is typically "
	          "overrided in the sub class: %s (%s) [%p]\n",
	          Utils::demangle(className).c_str(), className.c_str(), this);
	return ItemDataPtr();
}

SQLWhereElement *SQLWhereElement::findInsertPoint(SQLWhereElement *insertElem)
{
	string str;
	SQLWhereOperator *insertElemOp = insertElem->m_operator;
	if (!insertElemOp) {
		TRMSG(str, "Operator of insertElem is NULL.");
		throw logic_error(str);
	}

	SQLWhereElement *whereElem = this;
	for (; whereElem; whereElem = whereElem->m_parent) {
		if (whereElem->getType() != SQL_WHERE_ELEM_ELEMENT)
			continue;
		SQLWhereOperator *whereOp = whereElem->m_operator;
		if (!whereOp) {
			TRMSG(str, "Operator is NULL.");
			throw logic_error(str);
		}
		if (!whereOp->priorityOver(insertElemOp))
			break;
	}
	return whereElem;
}

int SQLWhereElement::getTreeInfo(string &str, int maxNumElem, int currNum)
{
	string leftTypeName = "-";
	string rightTypeName = "-";
	string operatorName = "-";
	if (m_leftHand)
		leftTypeName = DEMANGLED_TYPE_NAME(*m_leftHand);
	if (m_rightHand)
		rightTypeName = DEMANGLED_TYPE_NAME(*m_rightHand);
	if (m_operator)
		operatorName = DEMANGLED_TYPE_NAME(*m_operator);

	string additionalInfo = getTreeInfoAdditional();
	str += StringUtils::sprintf
	         ("[%p] %s, L:%p (%s), R:%p (%s), O:%s, %s, @%d\n",
	                            this, DEMANGLED_TYPE_NAME(*this),
	                            m_leftHand, leftTypeName.c_str(),
	                            m_rightHand, rightTypeName.c_str(),
	                            operatorName.c_str(),
	                            additionalInfo.c_str(), currNum);
	currNum++;
	if (maxNumElem >= 0 && currNum >= maxNumElem)
		return currNum;

	if (m_leftHand)
		currNum = m_leftHand->getTreeInfo(str, maxNumElem, currNum);
	if (maxNumElem >= 0 && currNum >= maxNumElem)
		return currNum;

	if (m_rightHand)
		currNum = m_rightHand->getTreeInfo(str, maxNumElem, currNum);
	return currNum;
}

string SQLWhereElement::getTreeInfoAdditional(void)
{
	return "-";
}

// ---------------------------------------------------------------------------
// class: SQLWhereColumn
// ---------------------------------------------------------------------------
SQLWhereColumn::SQLWhereColumn
  (string &columnName, SQLWhereColumnDataGetter dataGetter, void *priv,
   SQLWhereColumnPrivDataDestructor privDataDestructor)
: SQLWhereElement(SQL_WHERE_ELEM_COLUMN),
  m_columnName(columnName),
  m_valueGetter(dataGetter),
  m_priv(priv),
  m_privDataDestructor(privDataDestructor)
{
}

SQLWhereColumn::~SQLWhereColumn()
{
	if (m_privDataDestructor)
		(*m_privDataDestructor)(this, m_priv);
}

const string &SQLWhereColumn::getColumnName(void) const
{
	return m_columnName;
}

void *SQLWhereColumn::getPrivateData(void) const
{
	return m_priv;
}

ItemDataPtr SQLWhereColumn::getItemData(int index)
{
	return (*m_valueGetter)(this, m_priv);
}

string SQLWhereColumn::getTreeInfoAdditional(void)
{
	return StringUtils::sprintf("name: %s", m_columnName.c_str());
}

// ---------------------------------------------------------------------------
// class: SQLWhereNumber
// ---------------------------------------------------------------------------
SQLWhereNumber::SQLWhereNumber(const PolytypeNumber &value)
: SQLWhereElement(SQL_WHERE_ELEM_NUMBER),
  m_value(value)
{
}

SQLWhereNumber::SQLWhereNumber(int value)
: SQLWhereElement(SQL_WHERE_ELEM_NUMBER),
  m_value(value)
{
}

SQLWhereNumber::SQLWhereNumber(double value)
: SQLWhereElement(SQL_WHERE_ELEM_NUMBER),
  m_value(value)
{
}

SQLWhereNumber::~SQLWhereNumber()
{
}

const PolytypeNumber &SQLWhereNumber::getValue(void) const
{
	return m_value;
}

string SQLWhereNumber::getTreeInfoAdditional(void)
{
	string str;
	if (m_value.getType() == PolytypeNumber::TYPE_NONE)
		str = StringUtils::sprintf("None");
	else if (m_value.getType() == PolytypeNumber::TYPE_INT64)
		str = StringUtils::sprintf("[INT64] %"PRId64, m_value.getAsInt64());
	else if (m_value.getType() == PolytypeNumber::TYPE_DOUBLE)
		str = StringUtils::sprintf("[DOUBLE] %f", m_value.getAsDouble());
	else
		str = StringUtils::sprintf("Unknown type: ", m_value.getType());
	return str;
}

// ---------------------------------------------------------------------------
// class: SQLWhereString
// ---------------------------------------------------------------------------
SQLWhereString::SQLWhereString(string &str)
: SQLWhereElement(SQL_WHERE_ELEM_STRING),
  m_str(str)
{
}

SQLWhereString::~SQLWhereString()
{
}

const string &SQLWhereString::getValue(void) const
{
	return m_str;
}

ItemDataPtr SQLWhereString::getItemData(int index)
{
	return ItemDataPtr(new ItemString(ITEM_ID_NOBODY, m_str), false);
}

string SQLWhereString::getTreeInfoAdditional(void)
{
	return m_str;
}

// ---------------------------------------------------------------------------
// class: SQLWherePairedNumber
// ---------------------------------------------------------------------------
SQLWherePairedNumber::SQLWherePairedNumber(const PolytypeNumber &v0,
                                           const PolytypeNumber &v1)
: SQLWhereElement(SQL_WHERE_ELEM_PAIRED_NUMBER),
  m_value0(v0),
  m_value1(v1)
{
}

SQLWherePairedNumber::~SQLWherePairedNumber()
{
}

const PolytypeNumber &SQLWherePairedNumber::getFirstValue(void) const
{
	return m_value0;
}

const PolytypeNumber &SQLWherePairedNumber::getSecondValue(void) const
{
	return m_value1;
}

ItemDataPtr SQLWherePairedNumber::getItemData(int index)
{
	string msg;
	PolytypeNumber *value = NULL;
	if (index == 0)
		value = &m_value0;
	else if (index == 1)
		value = &m_value1;
	else {
		TRMSG(msg, "Invalid index: %d\n", index);
		throw logic_error(msg);
	}

	PolytypeNumber::NumberType type = value->getType();
	ItemData *item = NULL;
	if (type == PolytypeNumber::TYPE_INT64) {
		item = new ItemInt(ITEM_ID_NOBODY, value->getAsInt64());
	} else {
		TRMSG(msg, "Not supported: type %d\n", type);
		throw logic_error(msg);
	}
	return ItemDataPtr(item, false);
}

string SQLWherePairedNumber::getTreeInfoAdditional(void)
{
	string str;
	PolytypeNumber *values[2] = {&m_value0, &m_value1};

	for (size_t i = 0; i < 2; i++) {
		PolytypeNumber::NumberType type = values[i]->getType();
		if (type == PolytypeNumber::TYPE_NONE) {
			str = StringUtils::sprintf("[NONE]");
		} else if (type == PolytypeNumber::TYPE_INT64) {
			str = StringUtils::sprintf
			        ("[INT64] %"PRId64, values[i]->getAsInt64());
		} else if (type == PolytypeNumber::TYPE_DOUBLE) {
			str = StringUtils::sprintf
			        ("[DOUBLE] %f", values[i]->getAsDouble());
		} else {
			str = StringUtils::sprintf
			        ("Unknown type: ", values[i]->getType());
		}
	}
	return str;
}

