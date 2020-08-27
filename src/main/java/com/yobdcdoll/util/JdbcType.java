package com.yobdcdoll.util;

import java.math.BigDecimal;
import java.sql.Types;

public class JdbcType {
    public static int compare(String a, String b, int jdbcType) {
        int result = 0;

        switch (jdbcType) {
            case Types.CHAR:
            case Types.VARCHAR:
            case Types.LONGVARCHAR:
                result = a.compareTo(b);
                break;

            case Types.NUMERIC:
            case Types.DECIMAL:
                BigDecimal aBigDecimal = new BigDecimal(a);
                BigDecimal bBigDecimal = new BigDecimal(b);
                result = aBigDecimal.compareTo(bBigDecimal);
                break;

            case Types.BIT:
                Boolean aBoolean = new Boolean(a);
                Boolean bBoolean = new Boolean(b);
                result = aBoolean.compareTo(bBoolean);
                break;

            case Types.TINYINT:
                Byte aByte = new Byte(a);
                Byte bByte = new Byte(b);
                result = aByte.compareTo(bByte);
                break;

            case Types.SMALLINT:
                Short aShort = new Short(a);
                Short bShort = new Short(b);
                result = aShort.compareTo(bShort);
                break;

            case Types.INTEGER:
                Integer aInteger = new Integer(a);
                Integer bInteger = new Integer(b);
                result = aInteger.compareTo(bInteger);
                break;

            case Types.BIGINT:
                Long aLong = new Long(a);
                Long bLong = new Long(b);
                result = a.compareTo(b);
                break;

            case Types.REAL:
            case Types.FLOAT:
                Float aFloat = new Float(a);
                Float bFloat = new Float(b);
                result = a.compareTo(b);
                break;

            case Types.DOUBLE:
                Double aDouble = new Double(a);
                Double bDouble = new Double(b);
                result = aDouble.compareTo(bDouble);
                break;

            case Types.BINARY:
            case Types.VARBINARY:
            case Types.LONGVARBINARY:
                throw new UnsupportedOperationException("binary type compare not supported");
//                break;

            case Types.DATE:
                throw new UnsupportedOperationException("date type compare not supported");
//                break;

            case Types.TIME:
                throw new UnsupportedOperationException("time type compare not supported");
//                break;

            case Types.TIMESTAMP:
                throw new UnsupportedOperationException("TIMESTAMP type compare not supported");
//                break;
        }
        return result;
    }

}
