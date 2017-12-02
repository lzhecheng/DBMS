#include "qe.h"

RC Iterator::getOneAttr(vector<Attribute> attrs, void *data, void *target, string attr, AttrType &type, int &length) {
	RC rc = -1;
	// deal with null-indicator
	int nullFieldsIndicatorActualSize = ceil((double)attrs.size() / CHAR_BIT);
	unsigned char *nullFieldsIndicator = (unsigned char *)malloc(nullFieldsIndicatorActualSize);
	memcpy(nullFieldsIndicator, data, nullFieldsIndicatorActualSize);

	int i = 0;
	int offset = nullFieldsIndicatorActualSize;
	vector<Attribute>::iterator iter;
	for(iter = attrs.begin(); iter != attrs.end(); iter++) {
		bool isNull = nullFieldsIndicator[i / 8] & (0 << (7 - i % 8));
		if(!isNull) {
			// not null
			type = (*iter).type;
			int currentLength = 0;
			if(type == TypeVarChar) {
				currentLength = *((int*)((char*)data + offset));
				offset += sizeof(int);
			} else {
				currentLength = sizeof(int);
			}
			if(attr == (*iter).name) {
				// find target
				memcpy(target, (char*)data + offset, currentLength);
				length = currentLength;
				break;
			} else {
				offset += currentLength;
			}
		} else {
			// null
			if(attr == (*iter).name) {
				// find target
				length = -1;
				break;
			}
		}
	}

	free(nullFieldsIndicator);
	return rc;
}


Filter::Filter(Iterator* input, const Condition &condition) {
	this->input = input;
	this->condition = condition;
}

RC Filter::getNextTuple(void *data) {
	RC rc = 0;
	do {
		rc = input->getNextTuple(data);
		if(rc == QE_EOF) {
			break;
		}
	} while(!isValid(data));

	return rc;
}

bool Filter::isValid(void *data) {
	bool isValid = false;
	// get all attributes
	vector<Attribute> attrs;
	getAttributes(attrs);

	void *left = malloc(PAGE_SIZE);
	void *right = malloc(PAGE_SIZE);
	int leftLength = 0, rightLength = 0;
	getOneAttr(attrs, data, left, condition.lhsAttr, type, leftLength); // get everything about left

	if(condition.bRhsIsAttr) {
		// right-hand side is an attribute
		AttrType rightType;
		getOneAttr(attrs, data, right, condition.rhsAttr, rightType, rightLength);
		if(rightType == type) {
			isValid = compareValue(left, leftLength, right, rightLength, type);
		}
	} else {
		// right-hand side is a value
		if(condition.rhsValue.type == type && prepareRightValue(condition.rhsValue, right, rightLength) == 0) {
			isValid = compareValue(left, leftLength, right, rightLength, type);
		}
	}

	free(left);
	free(right);
	return isValid;
}

bool Filter::compareValue(void *left, int leftLength, void *right, int rightLength, AttrType type) {
	bool result = false;
	CompOp compOp = condition.op;
	if(compOp == NO_OP) {
		result = true;
	} else if(leftLength == -1 || rightLength == -1) {
		result = false;
	} else {
		if(type == TypeVarChar)
		{
			// varchar
			//get condition value
			string leftStr = "";
			string rightStr = "";
			for(int i = 0; i < leftLength; i++)
			{
				leftStr += *((char *)left + i);
			}
			for(int i = 0; i < rightLength; i++)
			{
				rightStr += *((char *)right + i);
			}

			// compare
			if((compOp == EQ_OP && leftStr == rightStr) ||
					(compOp == LT_OP && leftStr < rightStr) ||
					(compOp == LE_OP && leftStr <= rightStr) ||
					(compOp == GT_OP && leftStr > rightStr) ||
					(compOp == GE_OP && leftStr >= rightStr) ||
					(compOp == NE_OP && leftStr != rightStr))
			{
				result = true;
			}
		}
		else if(type == TypeInt)
		{
			// int
			int leftInt = *((int *)left);
			int rightInt = *((int *)((char *)right));
			// compare
			if((compOp == EQ_OP && rightInt == leftInt) ||
				(compOp == LT_OP && rightInt > leftInt) ||
				(compOp == LE_OP && rightInt >= leftInt) ||
				(compOp == GT_OP && rightInt < leftInt) ||
				(compOp == GE_OP && rightInt <= leftInt) ||
				(compOp == NE_OP && rightInt != leftInt))
			{
				result = true;
			}
		}
		else
		{
			// real
			float leftReal = *((float *)left);
			float rightReal = *((float *)((char *)right));
			// compare
			if((compOp == EQ_OP && rightReal == leftReal) ||
					(compOp == LT_OP && rightReal > leftReal) ||
					(compOp == LE_OP && rightReal >= leftReal) ||
					(compOp == GT_OP && rightReal < leftReal) ||
					(compOp == GE_OP && rightReal <= leftReal) ||
					(compOp == NE_OP && rightReal != leftReal))
			{
				result = true;
			}
		}
	}

	return result;
}

RC Filter::prepareRightValue(Value value, void *right, int &rightLength) {
	RC rc = 0;
	AttrType type = value.type;
	if(type == TypeVarChar) {
		rightLength = *((int*)((char*)value.data));
		memcpy(right, (char*)value.data + sizeof(int), rightLength);
	} else {
		rightLength = sizeof(int);
		memcpy(right, (char*)value.data, rightLength);
	}

	return rc;
}


void Filter::getAttributes(vector<Attribute> &attrs) const {
	input->getAttributes(attrs);
}

//Project::Project(Iterator *input, const vector<string> &attrNames) {
//	iter = input;
//	this->attrNames = attrNames;
//}
//
//RC Project::getNextTuple(void *data) {
//	RC rc = 0;
//	return rc;
//}
