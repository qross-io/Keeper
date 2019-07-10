package io.qross.util

import com.fasterxml.jackson.databind.JsonNode

//case class DataType(className: String, typeName: String)

object DataType extends Enumeration {
    type DataType = Value
    val INTEGER: DataType = Value("INTEGER")
    val DECIMAL: DataType = Value("REAL")
    val TEXT: DataType = Value("TEXT")
    val BLOB: DataType = Value("BLOB")
    //val NUMBER: DataType = Value("REAL")
    
    def ofClassName(value: String): DataType = {
        val name = if (value.contains(".")) {
            value.substring(value.lastIndexOf(".") + 1).toLowerCase()
        }
        else {
            value.toLowerCase()
        }
    
        name match {
            case "int" | "integer" | "long" | "boolean" | "timestamp" => DataType.INTEGER
            case "float" | "double" | "bigdecimal"  => DataType.DECIMAL
            case "[B" => DataType.BLOB
            case _ => DataType.TEXT
        }
    }
   
    def of(value: Any): DataType = {
        if (value != null) {
            ofClassName(value.getClass.getName)
        }
        else {
            DataType.TEXT
        }
    }
    
    def from(node: JsonNode): DataType = {
        if (node.isIntegralNumber || node.isInt || node.isBoolean || node.isLong || node.isShort || node.isBigInteger) {
            DataType.INTEGER
        }
        else if (node.isFloatingPointNumber || node.isDouble || node.isFloat || node.isBigDecimal) {
            DataType.DECIMAL
        }
        else if (node.isBinary) {
            DataType.BLOB
        }
        else {
            DataType.TEXT
        }
    }
}

/*
-- DataType --


-- JsonNodeDataType --
ARRAY,
BINARY,
BOOLEAN,
MISSING,
NULL,
NUMBER,
OBJECT,
POJO,
STRING

-- SQLite DataTypes --
NULL
INTEGER
REAL
TEXT
BLOB
*/

/*

CREATE TABLE data_types (
    id BIGINT DEFAULT 1,
    t1 TINYINT DEFAULT 1,
    t2 SMALLINT DEFAULT 1,
    t3 MEDIUMINT DEFAULT 1,
    t4 INT DEFAULT 1,
    t5 BIGINT DEFAULT 1,
    t6 FLOAT DEFAULT 1.1,
    t7 DOUBLE DEFAULT 1.1,
    t8 DECIMAL(15,4) DEFAULT 1.1,
    t9 DATE,
    ta TIME,
    tb YEAR,
    tc DATETIME,
    td TIMESTAMP,
    te CHAR,
    tf VARCHAR(10),
    tg TINYBLOB,
    th TINYTEXT,
    ti BLOB,
    tj TEXT,
    tk MEDIUMBLOB,
    tl MEDIUMTEXT,
    tm LONGBLOB,
    tn LONGTEXT
)

t1, java.lang.Integer, TINYINT-6
t2, java.lang.Integer, SMALLINT5
t3, java.lang.Integer, MEDIUMINT4
t4, java.lang.Integer, INT4
t5, java.lang.Long, BIGINT-5
t6, java.lang.Float, FLOAT7
t7, java.lang.Double, DOUBLE8
t8, java.math.BigDecimal, DECIMAL3
t9, java.sql.Date, DATE91
ta, java.sql.Time, TIME92
tb, java.sql.Date, YEAR91
tc, java.sql.Timestamp, DATETIME93
td, java.sql.Timestamp, TIMESTAMP93
te, java.lang.String, CHAR1
tf, java.lang.String, VARCHAR12
tg, [B, TINYBLOB-3
th, java.lang.String, VARCHAR-1
ti, [B, BLOB-4
tj, java.lang.String, VARCHAR-1
tk, [B, MEDIUMBLOB-4
tl, java.lang.String, VARCHAR-1
tm, [B, LONGBLOB-4
tn, java.lang.String, VARCHAR-1

-- MySQL DataTypes --
TINYINT
SMALLINT
MEDIUMINT
INT
BIGINT
FLOAT
DOUBLE
DECIMAL

DATE
TIME
YEAR
DATETIME
TIMESTAMP

CHAR
VARCHAR
TINYBLOB
TINYTEXT
BLOB
TEXT
MEDIUMBLOB
MEDIUMTEXT
LONGBLOB
LONGTEXT
*/