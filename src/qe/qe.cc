#include "qe.h"

RC Iterator::getOneAttr(vector<Attribute> attrs, void *data, void *target, string attr, AttrType &type, int &length) {
	RC rc = -1; // maybe unable to find the attribute
	// deal with null-indicator
	int nullFieldsIndicatorActualSize = ceil((double)attrs.size() / CHAR_BIT);
	unsigned char *nullFieldsIndicator = (unsigned char *)malloc(nullFieldsIndicatorActualSize);
	memcpy(nullFieldsIndicator, data, nullFieldsIndicatorActualSize);

	int i = 0;
	int offset = nullFieldsIndicatorActualSize;
	vector<Attribute>::iterator iter;
	for(iter = attrs.begin(); iter != attrs.end(); iter++) {
		bool isNull = nullFieldsIndicator[i / 8] & (1 << (7 - i % 8));
		i++;
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
				rc = 0;
				break;
			} else {
				offset += currentLength;
			}
		} else {
			// null
			if(attr == (*iter).name) {
				// find target
				length = -1;
				rc = 0;
				break;
			}
		}
	}
	free(nullFieldsIndicator);
	return rc;
}

bool Iterator::compareValue(void *left, int leftLength, void *right, int rightLength, AttrType type, CompOp compOp) {
	bool result = false;
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

void Iterator::joinTwoAttributes(vector<Attribute> leftAttrs, vector<Attribute> rightAttrs, vector<Attribute> &joinedAttrs) {
	joinedAttrs.clear();
	vector<Attribute>::iterator iterLeft;
	vector<Attribute>::iterator iterRight;
	for(iterLeft = leftAttrs.begin(); iterLeft != leftAttrs.end(); iterLeft++) {
		joinedAttrs.push_back(*iterLeft);
	}
	for(iterRight = rightAttrs.begin(); iterRight != rightAttrs.end(); iterRight++) {
		joinedAttrs.push_back(*iterRight);
	}
}

void Iterator::joinTwoTuples(vector<Attribute> leftAttrs, void *left, vector<Attribute> rightAttrs, void *right, void *join) {
	// left
	int nullFieldsIndicatorActualSizeLeft;
	int leftLength;
	lengthOfTuple(leftAttrs, left, nullFieldsIndicatorActualSizeLeft, leftLength);
	// right
	int nullFieldsIndicatorActualSizeRight;
	int rightLength;
	lengthOfTuple(rightAttrs, right, nullFieldsIndicatorActualSizeRight, rightLength);
	// joined
	int nullFieldsIndicatorActualSizeJoined = ceil((double)(leftAttrs.size() + rightAttrs.size()) / CHAR_BIT);

	// make joined
	int offset = 0;
	memcpy((char*)join + offset, left, leftAttrs.size());
	offset += leftAttrs.size();
	memcpy((char*)join + offset, right, rightAttrs.size());
	offset = nullFieldsIndicatorActualSizeJoined;
	memcpy((char*)join + offset, (char*)left + nullFieldsIndicatorActualSizeLeft, leftLength);
	offset += leftLength;
	memcpy((char*)join + offset, (char*)right + nullFieldsIndicatorActualSizeRight, rightLength);
}

void Iterator::lengthOfTuple(vector<Attribute> attrs, void *data, int &nullindicatorLength, int &length) {
	int nullFieldsIndicatorActualSize = ceil((double)attrs.size() / CHAR_BIT);
	unsigned char *nullFieldsIndicator = (unsigned char *)malloc(nullFieldsIndicatorActualSize);
	memcpy(nullFieldsIndicator, data, nullFieldsIndicatorActualSize);

	int tLength = nullFieldsIndicatorActualSize;
	for(unsigned i = 0; i < attrs.size(); i++) {
		if((nullFieldsIndicator[i / 8] & (1 << (7 - i % 8))) == 0) {
			tLength += (attrs.at(i).type == TypeVarChar) ? *((int*)((char*)left + tLength)) + sizeof(int) : sizeof(int);
		}
	}
	nullindicatorLength = nullFieldsIndicatorActualSize;
	length = tLength - nullFieldsIndicatorActualSize;
}

// *****Filter starts here*****
Filter::Filter(Iterator* input, const Condition &condition) {
	this->input = input;
	this->condition = condition;
	this->type = condition.rhsValue.type;
	input->getAttributes(attrs);
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

	void *left = malloc(PAGE_SIZE);
	void *right = malloc(PAGE_SIZE);
	int leftLength = 0, rightLength = 0;
	AttrType leftType;
	getOneAttr(attrs, data, left, condition.lhsAttr, leftType, leftLength); // get everything about left

	if(condition.bRhsIsAttr) {
		// right-hand side is an attribute
		AttrType rightType; // no use
		getOneAttr(attrs, data, right, condition.rhsAttr, rightType, rightLength);
		isValid = compareValue(left, leftLength, right, rightLength, type, condition.op);
	} else {
		// right-hand side is a value
		if(leftType == type && prepareRightValue(condition.rhsValue, right, rightLength) == 0) {
			isValid = compareValue(left, leftLength, right, rightLength, type, condition.op);
		}
	}

	free(left);
	free(right);
	return isValid;
}


RC Filter::prepareRightValue(Value value, void *right, int &rightLength) {
	RC rc = 0;
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
	attrs.clear();
	attrs = this->attrs;
}

// *****Project starts here*****
Project::Project(Iterator *input, const vector<string> &attrNames) {
	this->input = input;
	this->attrNames = attrNames;
	input->getAttributes(allAttrs);
	getTargetAttrs();
}

RC Project::getNextTuple(void *data) {
	RC rc = 0;
	rc = input->getNextTuple(data);
	if(rc != QE_EOF) {
		pickAttrs(data);
	}

	return rc;
}

void Project::getAttributes(vector<Attribute> &attrs) const {
	attrs.clear();
	attrs = projectedAttrs;
}

void Project::getTargetAttrs() {
	vector<Attribute>::iterator iterAttr;
	vector<string>::iterator iterAttrName;
	// find all projected attributes
	for(iterAttrName = attrNames.begin(); iterAttrName != attrNames.end(); iterAttrName++) {
		for(iterAttr = allAttrs.begin(); iterAttr != allAttrs.end(); iterAttr++) {
			if((*iterAttr).name == *iterAttrName) {
				projectedAttrs.push_back(*iterAttr);
				break;
			}
		}
	}
}

RC Project::pickAttrs(void *data) {
	RC rc = 0;
	// null-indicator for data
	int nullFieldsIndicatorActualSizeData = ceil((double)allAttrs.size() / CHAR_BIT);
	unsigned char *nullFieldsIndicatorData = (unsigned char *)malloc(nullFieldsIndicatorActualSizeData);
	memcpy(nullFieldsIndicatorData, data, nullFieldsIndicatorActualSizeData);
	// null-indicator for space
	int nullFieldsIndicatorActualSizeSpace = ceil((double)projectedAttrs.size() / CHAR_BIT);
	unsigned char *nullFieldsIndicatorSpace = (unsigned char *)malloc(nullFieldsIndicatorActualSizeSpace);
	memset(nullFieldsIndicatorSpace, 0, nullFieldsIndicatorActualSizeSpace);

	void *space = malloc(PAGE_SIZE);
	int spaceOffset = nullFieldsIndicatorActualSizeSpace;
	int dataOffset = nullFieldsIndicatorActualSizeData;

	for(int i = 0; i < allAttrs.size(); i++) {
		// check if this attribute should be projected
		bool isOneProjectedAttrs = false;
		for(int j = 0; j < projectedAttrs.size(); j++) {
			if(allAttrs.at(i).name == projectedAttrs.at(j).name) {
				isOneProjectedAttrs = true;
				break;
			}
		}

		bool isNull = nullFieldsIndicatorData[i / 8] & (1 << (7 - i % 8));
		int length = 0;
		if(!isNull) {
			length = allAttrs.at(i).type == TypeVarChar ? *((int*)((char*)data + dataOffset)) + sizeof(int): sizeof(int);
		} else {
			length = 0;
		}
		if(!isOneProjectedAttrs) {
			// not a projected attribute
			if(!isNull) {
				// not null
				dataOffset += length;
			}
		} else {
			// a projected attribute
			if(!isNull) {
				// not null
				memcpy((char*)space + spaceOffset, (char*)data + dataOffset, length);
				dataOffset += length;
				spaceOffset += length;
			} else {
				// null
				nullFieldsIndicatorSpace[i / 8] = nullFieldsIndicatorSpace[i / 8] | (1 << (7 - i % 8));
			}
		}
	}

	// put rearranged data into parameter data
	memcpy(space, nullFieldsIndicatorSpace, nullFieldsIndicatorActualSizeSpace);
	memcpy(data, space, spaceOffset);

	free(space);
	return rc;
}

//*****BNL Join starts here*****
BNLJoin::BNLJoin(Iterator *leftIn, TableScan *rightIn, const Condition &condition, const unsigned numPages) {
	this->leftIn = leftIn;
	this->rightIn = rightIn;
	this->condition = condition;
	this->numPages = numPages;

	leftIn->getAttributes(leftAttrs);
	rightIn->getAttributes(rightAttrs);
	joinTwoAttributes(leftAttrs, rightAttrs, joinedAttrs); // make joined attributes

	maxTuples = numPages * MAX_TUPLE_IN_PAGE;
	posMultipleKey = 0;
	right = malloc(MAX_TUPLE_SIZE);
}

BNLJoin::~BNLJoin() {
	free(right);
}


RC BNLJoin::getNextTuple(void *data) {
	RC rc = 0;
	void *left = malloc(MAX_TUPLE_SIZE);

	if(posMultipleKey == 0) {
		// start a new right
		while(true) {
			if(getNextRightTuple() != 0) {
				// joining finishes
				rc = -1;
				break;
			} else {
				if(isValid(left)) {
					// find one but need to check if valid
					rc = 0;
					break;
				}
			}
		}
	} else {
		// last right still has some left to join (a key - multiple values)
		// sure to find one
		isValid(left);
		rc = 0;
	}

	// join two tuple
	if(rc == 0) {
		joinTwoTuples(leftAttrs, left, rightAttrs, right, data);
	}

	free(left);
	return rc;
}

bool BNLJoin::isValid(void *left) {
	bool result = false;
	string stringValue;
	int intValue;
	float realValue;
	getValue(false, right, stringValue, intValue, realValue);

	if(condition.rhsValue.type == TypeVarChar) {
		// varchar
		if(mapVarchar.find(stringValue) == mapVarchar.end()) {
			// fail to find
			result = false;
		} else {
			// find
			vector<void *> tmp = mapVarchar[stringValue];
			result = true;
			memcpy(left, tmp[posMultipleKey], MAX_TUPLE_SIZE);
			posMultipleKey++;
			if(posMultipleKey == tmp.size()) {
				posMultipleKey = 0;
			}
		}
	} else if(condition.rhsValue.type == TypeInt) {
		// int
		if(mapInt.find(intValue) == mapInt.end()) {
			// fail to find
			result = false;
		} else {
			// find
			vector<void *> tmp = mapInt[intValue];
			result = true;
			memcpy(left, tmp[posMultipleKey], MAX_TUPLE_SIZE);
			posMultipleKey++;
			if(posMultipleKey == tmp.size()) {
				posMultipleKey = 0;
			}
		}
	} else {
		// real
		if(mapReal.find(realValue) == mapReal.end()) {
			// fail to find
			result = false;
		} else {
			// find
			vector<void *> tmp = mapReal[realValue];
			result = true;
			memcpy(left, tmp[posMultipleKey], MAX_TUPLE_SIZE);
			posMultipleKey++;
			if(posMultipleKey == tmp.size()) {
				posMultipleKey = 0;
			}
		}
	}

	return result;
}

RC BNLJoin::getNextBlock() {
	RC rc = 0;
	string stringValue;
	int intValue;
	float realValue;

	if(condition.rhsValue.type == TypeVarChar) {
		// varchar
		clearMap();
		for(int i = 0; i < maxTuples; i++) {
			void *data = malloc(MAX_TUPLE_SIZE);
			rc = leftIn->getNextTuple(data);
			getValue(true, data, stringValue, intValue, realValue);
			if(rc != 0) {
				// no more tuple in left
				if(i == 0) {
					// not able to have a new block, finish
					rc = -1;
					free(data);
					break;
				} else {
					// have a new block
					free(data);
					rc = 0;
					break;
				}
			} else {
				if(mapVarchar.find(stringValue) != mapVarchar.end()) {
					// key exists
					mapVarchar[stringValue].push_back(data);
				} else {
					// key not exists
					vector<void *> tmp;
					tmp.push_back(data);
					mapVarchar[stringValue] = tmp;
				}
			}
		}
	} else if(condition.rhsValue.type == TypeInt) {
		// int
		clearMap();
		for(int i = 0; i < maxTuples; i++) {
			void *data = malloc(MAX_TUPLE_SIZE);
			rc = leftIn->getNextTuple(data);
			getValue(true, data, stringValue, intValue, realValue);
			if(rc != 0) {
				// no more tuple in left
				if(i == 0) {
					// not able to have a new block, finish
					rc = -1;
					free(data);
					break;
				} else {
					// have a new block
					rc = 0;
					free(data);
					break;
				}
			} else {
				if(mapInt.find(intValue) != mapInt.end()) {
					// key exists
					mapInt[intValue].push_back(data);
				} else {
					// key not exists
					vector<void *> tmp;
					tmp.push_back(data);
					mapInt[intValue] = tmp;
				}
			}
		}
	} else {
		// real
		clearMap();
		for(int i = 0; i < maxTuples; i++) {
			void *data = malloc(MAX_TUPLE_SIZE);
			rc = leftIn->getNextTuple(data);
			getValue(true, data, stringValue, intValue, realValue);
			if(rc != 0) {
				// no more tuple in left
				if(i == 0) {
					// not able to have a new block, finish
					rc = -1;
					free(data);
					break;
				} else {
					// have a new block
					rc = 0;
					free(data);
					break;
				}
			} else {
				if(mapReal.find(realValue) != mapReal.end()) {
					// key exists
					mapReal[realValue].push_back(data);
				} else {
					// key not exists
					vector<void *> tmp;
					tmp.push_back(data);
					mapReal[realValue] = tmp;
				}
			}
		}
	}

	return rc;
}

void BNLJoin::clearMap() {
	if(condition.rhsValue.type == TypeVarChar) {
		unordered_map<string, vector<void *>>::iterator iter;
		for(iter = mapVarchar.begin(); iter != mapVarchar.end(); iter++) {
			vector<void *>::iterator iterVector;
			for(iterVector = (*iter).second.begin(); iterVector != (*iter).second.end(); iterVector++) {
				free(*iterVector);
			}
		}
		mapVarchar.clear();
	} else if(condition.rhsValue.type == TypeInt) {
		unordered_map<int, vector<void *>>::iterator iter;
		for(iter = mapInt.begin(); iter != mapInt.end(); iter++) {
			vector<void *>::iterator iterVector;
			for(iterVector = (*iter).second.begin(); iterVector != (*iter).second.end(); iterVector++) {
				free(*iterVector);
			}
		}
		mapInt.clear();
	} else {
		unordered_map<float, vector<void *>>::iterator iter;
		for(iter = mapReal.begin(); iter != mapReal.end(); iter++) {
			vector<void *>::iterator iterVector;
			for(iterVector = (*iter).second.begin(); iterVector != (*iter).second.end(); iterVector++) {
				free(*iterVector);
			}
		}
		mapReal.clear();
	}
}

void BNLJoin::getValue(bool left, void *data, string &stringValue, int &intValue, float &realValue) {
	int length;
	void *target = malloc(MAX_TUPLE_SIZE);
    if(!left) getOneAttr(rightAttrs, right, target, condition.rhsAttr, condition.rhsValue.type, length);
    else getOneAttr(leftAttrs, data, target, condition.lhsAttr, condition.rhsValue.type, length);
    if(condition.rhsValue.type == TypeVarChar) {
		// varchar
		string result = "";
        for(int i = 0 ; i < length; i++) {
        		result += *((char*)target + i);
        }
		stringValue = result;
	} else if(condition.rhsValue.type == TypeInt) {
		// int
		intValue = *((int*)((char*)target));
	} else {
		// real
		realValue = *((float*)((char*)target));
	}

	free(target);
}


RC BNLJoin::getNextRightTuple() {
	RC rc = 0;
	rc = rightIn->getNextTuple(right);
	if(rc != 0) {
		// last tuple in right already got. Check if should get the next block
		rc = getNextBlock();
		if(rc == 0) {
			// a new block
			rightIn->setIterator();
			rc = getNextRightTuple();
		}
		// if rc == -1, join should finish
	}
	return rc;
}

void BNLJoin::getAttributes(vector<Attribute> &attrs) const {
	attrs.clear();
	attrs = joinedAttrs;
}

//*****INL Join starts here*****
INLJoin::INLJoin(Iterator *leftIn, IndexScan *rightIn, const Condition &condition) {
	this->leftIn = leftIn;
	this->rightIn = rightIn;
	this->condition = condition;
	getRightTupleFinish = true;
	left = malloc(MAX_TUPLE_SIZE);

	leftIn->getAttributes(leftAttrs);
	leftIn->getAttributes(rightAttrs);
	joinTwoAttributes(leftAttrs, rightAttrs, joinedAttrs);
<<<<<<< HEAD
<<<<<<< HEAD
	type = condition.rhsValue.type;
	leftTargetAttr = malloc(MAX_TUPLE_SIZE);
=======


>>>>>>> parent of 76be3de... Some small fixed
=======


>>>>>>> parent of 76be3de... Some small fixed
}

INLJoin::~INLJoin() {
	free(left);
	free(leftTargetAttr);
}

RC INLJoin::getNextTuple(void *data) {
	RC rc = 0;
	void *right = malloc(MAX_TUPLE_SIZE);

	if(getRightTupleFinish) {
		rc = leftIn->getNextTuple(left);
		this->rightTupleToFirst(left);
		getRightTupleFinish = false;
	}
	if(rc != -1) {
		rc = rightIn->getNextTuple(right);
		if(rc == -1) {
			// rightNextTuple finished this time
			getRightTupleFinish = true;
			rc = this->getNextTuple(data);
		} else {
<<<<<<< HEAD
<<<<<<< HEAD
			this->joinTwoTuples(leftAttrs, left, rightAttrs, right, data); // output data
			break;
=======
			this->joinTwoTuples(leftAttrs, left, rightAttrs, right, data);
>>>>>>> parent of 76be3de... Some small fixed
=======
			this->joinTwoTuples(leftAttrs, left, rightAttrs, right, data);
>>>>>>> parent of 76be3de... Some small fixed
		}
	}

	free(right);
	return rc;
}

<<<<<<< HEAD
<<<<<<< HEAD
RC INLJoin::rightTupleToFirst(void *left) {
	RC rc = 0;
	int length = 0;
	AttrType leftType; // used in fact

	this->getOneAttr(leftAttrs, left, leftTargetAttr, condition.lhsAttr, leftType, length);
	if(length != -1) {
		// attribute in this left is not null
		// if varchar, put 4 bytes before data
		if(leftType == TypeVarChar) {
			void *tmp = malloc(MAX_TUPLE_SIZE);
			memset(tmp, 0, MAX_TUPLE_SIZE);
			memcpy(tmp, leftTargetAttr, length);

			// set varchar's target
			memcpy(leftTargetAttr, &length, sizeof(int));
			memcpy((char*)leftTargetAttr + sizeof(int), tmp, length);
			free(tmp);
		}
		rightIn->setIterator(leftTargetAttr, leftTargetAttr, true, true); // set up scan in right
	} else {
		rc = -1;
	}

	return rc;
=======
void INLJoin::rightTupleToFirst(void *left) {
	void *target = malloc(MAX_TUPLE_SIZE);
	int length = 0;
	this->getOneAttr(leftAttrs, left, target, condition.lhsAttr, condition.rhsValue.type, length);
	// if varchar, put 4 bytes before data
	if(condition.rhsValue.type == TypeVarChar) {
		void *tmp = malloc(MAX_TUPLE_SIZE);
		memcpy(tmp, target, length);
		memcpy((char*)target + sizeof(int), tmp, length);
		memcpy(target, &length, sizeof(int));
		free(tmp);
	}
	rightIn->setIterator(target, target, true, true);
	free(target);
>>>>>>> parent of 76be3de... Some small fixed
=======
void INLJoin::rightTupleToFirst(void *left) {
	void *target = malloc(MAX_TUPLE_SIZE);
	int length = 0;
	this->getOneAttr(leftAttrs, left, target, condition.lhsAttr, condition.rhsValue.type, length);
	// if varchar, put 4 bytes before data
	if(condition.rhsValue.type == TypeVarChar) {
		void *tmp = malloc(MAX_TUPLE_SIZE);
		memcpy(tmp, target, length);
		memcpy((char*)target + sizeof(int), tmp, length);
		memcpy(target, &length, sizeof(int));
		free(tmp);
	}
	rightIn->setIterator(target, target, true, true);
	free(target);
>>>>>>> parent of 76be3de... Some small fixed
}


void INLJoin::getAttributes(vector<Attribute> &attrs) const {
	attrs.clear();
	attrs = joinedAttrs;
<<<<<<< HEAD
}

//*****Aggregate starts here*****
Aggregate::Aggregate(Iterator *input, Attribute aggAttr, AggregateOp op) {
	this->attr = aggAttr;
	this->op = op;
	this->iter = input;
	iter->getAttributes(this->attrs);
	this->firstTime = true;
	this->min = 0;
	this->max = 0;
	this->sum = 0;
	this->avg = 0;
	this->count = 0;
}


RC Aggregate::getNextTuple(void *data) {
	if (!firstTime)
		return QE_EOF;
	else
		firstTime = false;

	RC rc = 0;
	void* entry = malloc(PAGE_SIZE);
	if (entry == NULL) cerr << "malloc() failed" << endl;
	memset(entry, 0, PAGE_SIZE);
	void* target = malloc(PAGE_SIZE);
	if (target == NULL) cerr << "malloc() failed" << endl;
	memset(target, 0, PAGE_SIZE);

	int length = 0;
	float value = 0;

	while (this->iter->getNextTuple(entry) != EOF) {
		rc = this->iter->getOneAttr(this->attrs, entry, target, this->attr.name, this->attr.type, length);
		if (rc != 0) cerr << "getOneAttr() failed" << endl;
		count += 1.0;
		if (attr.type != TypeVarChar) {
			if (attr.type == TypeInt)
				value = (float)(*(int*)target);
			else
				value = *(float*)target;

			sum += value;
			avg = 1.0 * sum / count;
			if (min > value)
				min = value;
			if (max < value)
				max = value;
		}
	}

	char ch = 0;
	memcpy(data, &ch, 1);

	if (this->op == MIN)
		memcpy((char*)data + 1, &min, sizeof(float));
	else if (this->op == MAX)
		memcpy((char*)data + 1, &max, sizeof(float));
	else if (this->op == SUM)
		memcpy((char*)data + 1, &sum, sizeof(float));
	else if (this->op == AVG)
		memcpy((char*)data + 1, &avg, sizeof(float));
	else if (this->op == COUNT)
		memcpy((char*)data + 1, &count, sizeof(float));
	else cerr << "op error1" << endl;

	free(entry);
	free(target);
}


void Aggregate::getAttributes(vector<Attribute> &attrs) const {
	Attribute tmp;
	string s = "";
	if (this->op == MIN)
		s += "MIN";
	else if (this->op == MAX)
		s += "MAX";
	else if (this->op == SUM)
		s += "SUM";
	else if (this->op == AVG)
		s += "AVG";
	else if (this->op == COUNT)
		s += "COUNT";
	else cerr << "op error2" << endl;

	s += "(" + this->attr.name + ")";
	tmp.name = s;
	tmp.type = TypeReal;
	tmp.length = sizeof(float);
	attrs.push_back(tmp);
=======
>>>>>>> parent of 76be3de... Some small fixed
}
