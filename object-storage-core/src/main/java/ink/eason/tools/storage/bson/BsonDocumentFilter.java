package ink.eason.tools.storage.bson;

import org.bson.*;
import org.bson.types.BSONTimestamp;
import org.bson.types.Binary;
import org.bson.types.Decimal128;
import org.bson.types.ObjectId;

import java.util.ArrayList;
import java.util.Date;
import java.util.HashSet;
import java.util.List;
import java.util.Map;
import java.util.Set;
import java.util.regex.Pattern;
import java.util.stream.Collectors;

public class BsonDocumentFilter {

    /**
     * 检查单个 RawBsonDocument 是否满足过滤条件
     *
     * @param doc    单个 Bson 文档
     * @param filter 过滤条件
     * @return 如果匹配则返回 true，否则返回 false
     */
    public static boolean matches(Map<String,BsonValue> doc, BsonDocument filter) {
        for (String key : filter.keySet()) {
            BsonValue filterValue = filter.get(key);
            switch (key) {
                case "$and":
                    if (!handleAnd(doc, filterValue.asArray())) {
                        return false;
                    }
                    break;
                case "$or":
                    if (!handleOr(doc, filterValue.asArray())) {
                        return false;
                    }
                    break;
                default:
                    BsonValue docValue = doc.get(key);
                    if (!fieldMatches(docValue, filterValue)) {
                        return false;
                    }
            }
        }
        return true;
    }

    /**
     * 处理 $and 操作符
     */
    private static boolean handleAnd(Map<String,BsonValue> doc, BsonArray conditions) {
        for (BsonValue condition : conditions) {
            if (!matches(doc, condition.asDocument())) {
                return false;
            }
        }
        return true;
    }

    /**
     * 处理 $or 操作符
     */
    private static boolean handleOr(Map<String,BsonValue> doc, BsonArray conditions) {
        for (BsonValue condition : conditions) {
            if (matches(doc, condition.asDocument())) {
                return true;
            }
        }
        return false;
    }

    /**
     * 检查字段是否匹配过滤条件
     *
     * @param docValue    文档中的字段值
     * @param filterValue 过滤条件的值
     * @return 如果匹配则返回 true
     */
    private static boolean fieldMatches(BsonValue docValue, BsonValue filterValue) {
        if (filterValue.isDocument()) {
            BsonDocument conditionDoc = filterValue.asDocument();
            for (String operator : conditionDoc.keySet()) {
                BsonValue operatorValue = conditionDoc.get(operator);
                if (!applyOperator(docValue, operator, operatorValue)) {
                    return false;
                }
            }
            return true;
        } else {
            // 隐式的 $eq 操作
            return compare(docValue, filterValue) == 0;
        }
    }

    /**
     * 应用具体的操作符
     */
    private static boolean applyOperator(BsonValue docValue, String operator, BsonValue operatorValue) {
        // 如果文档中字段不存在 (docValue == null)，大部分操作符都应该返回 false
        if (docValue == null) {
            return operator.equals("$ne") || operator.equals("$nin");
        }

        switch (operator) {
            case "$eq":
                return compare(docValue, operatorValue) == 0;
            case "$ne":
                return compare(docValue, operatorValue) != 0;
            case "$gt":
                return compare(docValue, operatorValue) > 0;
            case "$gte":
                return compare(docValue, operatorValue) >= 0;
            case "$lt":
                return compare(docValue, operatorValue) < 0;
            case "$lte":
                return compare(docValue, operatorValue) <= 0;
            case "$in":
                return operatorValue.asArray().stream().anyMatch(val -> compare(docValue, val) == 0);
            case "$nin":
                return operatorValue.asArray().stream().noneMatch(val -> compare(docValue, val) == 0);
            case "$regex":
                return handleRegex(docValue, operatorValue);
            default:
                // 不支持的操作符，可以抛出异常或返回 false
                return false;
        }
    }

    /**
     * 处理正则表达式匹配
     */
    private static boolean handleRegex(BsonValue docValue, BsonValue regexValue) {
        if (!docValue.isString()) {
            return false;
        }
        String docString = docValue.asString().getValue();
        Pattern pattern = Pattern.compile(regexValue.asString().getValue());
        return pattern.matcher(docString).find();
    }

    /**
     * 通用的比较方法
     *
     * @return 0 for equal, > 0 for docValue > filterValue, < 0 for docValue < filterValue
     */
    @SuppressWarnings({"rawtypes", "unchecked"})
    private static int compare(BsonValue docValue, BsonValue filterValue) {
        if (docValue == null && filterValue == null) return 0;
        if (docValue == null) return -1;
        if (filterValue == null) return 1;
        if (docValue.getBsonType() != filterValue.getBsonType()) {
            // 对于数字类型，可以进行转换后比较
            if (docValue.isNumber() && filterValue.isNumber()) {
                Number num1 = getNumberFromBsonValue(docValue);
                Number num2 = getNumberFromBsonValue(filterValue);
                return Double.compare(num1.doubleValue(), num2.doubleValue());
            }
            // 类型不同且无法比较，则认为不相等
            return -1;
        }

        // 类型相同
        switch (docValue.getBsonType()) {
            case INT32:
                return Integer.compare(docValue.asInt32().getValue(), filterValue.asInt32().getValue());
            case INT64:
                return Long.compare(docValue.asInt64().getValue(), filterValue.asInt64().getValue());
            case DOUBLE:
                return Double.compare(docValue.asDouble().getValue(), filterValue.asDouble().getValue());
            case STRING:
                return docValue.asString().getValue().compareTo(filterValue.asString().getValue());
            case DATE_TIME:
                return Long.compare(docValue.asDateTime().getValue(), filterValue.asDateTime().getValue());
            case BOOLEAN:
                return Boolean.compare(docValue.asBoolean().getValue(), filterValue.asBoolean().getValue());
            case OBJECT_ID:
                return docValue.asObjectId().getValue().compareTo(filterValue.asObjectId().getValue());
            // 对于其他类型，我们只支持判等
            default:
                return docValue.equals(filterValue) ? 0 : -1;
        }
    }

    private static Number getNumberFromBsonValue(BsonValue bsonValue) {
        if (bsonValue.isInt32()) {
            return bsonValue.asInt32().getValue();
        } else if (bsonValue.isInt64()) {
            return bsonValue.asInt64().getValue();
        } else if (bsonValue.isDouble()) {
            return bsonValue.asDouble().getValue();
        } else if (bsonValue.isDecimal128()) {
            return bsonValue.asDecimal128().getValue().bigDecimalValue();
        }
        throw new IllegalArgumentException("Unsupported number type: " + bsonValue.getBsonType());
    }


    // ================== 测试代码 ==================
    public static void main(String[] args) {

    }
}
