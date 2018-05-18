/**
 * Copyright 2015 Jan Lolling jan.lolling@gmail.com
 * 
 * Licensed under the Apache License, Version 2.0 (the "License");
 * you may not use this file except in compliance with the License.
 * You may obtain a copy of the License at
 *
 *    http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */
package de.jlo.talendcomp.camunda;

import java.math.BigDecimal;
import java.math.BigInteger;
import java.sql.Timestamp;
import java.text.DecimalFormat;
import java.text.NumberFormat;
import java.text.ParseException;
import java.text.ParsePosition;
import java.text.SimpleDateFormat;
import java.util.Date;
import java.util.HashMap;
import java.util.Locale;
import java.util.Map;

import com.fasterxml.jackson.databind.JsonNode;

public final class TypeUtil {
	
	private static final Map<String, DecimalFormat> numberformatMap = new HashMap<String, DecimalFormat>();
	private static final String DEFAULT_DATE_PATTERN = "yyyy-MM-dd'T'HH:mm:ss.SSS";
	private static final String DEFAULT_LOCALE = "en_UK";
	
	public TypeUtil() {}
	
	public static DecimalFormat getNumberFormat(String localeStr) {
		DecimalFormat nf = numberformatMap.get(localeStr);
		if (nf == null) {
			Locale locale = new Locale(localeStr);
			nf = (DecimalFormat) NumberFormat.getInstance(locale);
			numberformatMap.put(localeStr, nf);
		}
		return nf;
	} 
	
	/**
	 * concerts the string format into a Date
	 * @param dateString
	 * @param pattern
	 * @return the resulting Date
	 */
	public static Date convertToDate(String dateString, String pattern) throws Exception {
		if (dateString == null || dateString.isEmpty()) {
			return null;
		}
		if (pattern == null) {
			pattern = DEFAULT_DATE_PATTERN;
		}
		try {
			SimpleDateFormat sdf = new SimpleDateFormat(pattern);
			Date date = null;
			try {
				date = sdf.parse(dateString);
			} catch (ParseException pe) {
				date = GenericDateUtil.parseDate(dateString);
			}
			return date;
		} catch (Throwable t) {
			throw new Exception("Failed to convert string to date:" + t.getMessage(), t);
		}
	}
	
	public static Timestamp convertToTimestamp(String dateString, String pattern) throws Exception {
		Date date = convertToDate(dateString, pattern);
		if (date != null) {
			return new Timestamp(date.getTime());
		} else {
			return null;
		}
	}

	public static Boolean convertToBoolean(String value) throws Exception {
		if (value == null) {
			return null;
		}
		value = value.toLowerCase();
		if ("true".equals(value)) {
			return Boolean.TRUE;
		} else if ("false".equals(value)) {
			return Boolean.FALSE;
		} else if ("1".equals(value)) {
			return Boolean.TRUE;
		} else if ("0".equals(value)) {
			return Boolean.FALSE;
		} else if ("yes".equals(value)) {
			return Boolean.TRUE;
		} else if ("y".equals(value)) {
			return Boolean.TRUE;
		} else if ("sí".equals(value)) {
			return Boolean.TRUE;
		} else if ("да".equals(value)) {
			return Boolean.TRUE;
		} else if ("no".equals(value)) {
			return Boolean.FALSE;
		} else if ("нет".equals(value)) {
			return Boolean.FALSE;
		} else if ("n".equals(value)) {
			return Boolean.FALSE;
		} else if ("ja".equals(value)) {
			return Boolean.TRUE;
		} else if ("j".equals(value)) {
			return Boolean.TRUE;
		} else if ("nein".equals(value)) {
			return Boolean.FALSE;
		} else if ("oui".equals(value)) {
			return Boolean.TRUE;
		} else if ("non".equals(value)) {
			return Boolean.FALSE;
		} else if ("ok".equals(value)) {
			return Boolean.TRUE;
		} else if ("x".equals(value)) {
			return Boolean.TRUE;
		} else if (value != null) {
			throw new Exception("Value: " + value + " cannot be parsed to a boolean");
		} else {
			return null;
		}
	}

	public static Boolean convertToBoolean(JsonNode node) throws Exception {
		if (node.isBoolean()) {
			return node.asBoolean();
		} else if (node.isTextual()) {
			return convertToBoolean(node.asText());
		} else if (node.isNumber()) {
			return convertToBoolean(node.asText());
		} else if (node.isNull()) {
			return null;
		} else if (node.isMissingNode()) {
			return null;
		} else {
			throw new Exception("Node: " + node + " cannot be converted to Boolean");
		}
	}

	public static Double convertToDouble(String value) throws Exception {
		if (value == null || value.isEmpty()) {
			return null;
		}
		DecimalFormat decfrm = getNumberFormat(DEFAULT_LOCALE);
		decfrm.setParseBigDecimal(false);
		return decfrm.parse(value).doubleValue();
	}

	public static Double convertToDouble(JsonNode node) throws Exception {
		if (node.isNumber()) {
			return node.asDouble();
		} else if (node.isTextual()) {
			return convertToDouble(node.asText());
		} else if (node.isNull()) {
			return null;
		} else if (node.isMissingNode()) {
			return null;
		} else {
			throw new Exception("Node: " + node + " cannot be converted to Double");
		}
	}

	public static Integer convertToInteger(String value) throws Exception {
		if (value == null || value.isEmpty()) {
			return null;
		}
		DecimalFormat decfrm = getNumberFormat(DEFAULT_LOCALE);
		decfrm.setParseBigDecimal(false);
		return decfrm.parse(value).intValue();
	}
	
	public static Integer convertToInteger(JsonNode node) throws Exception {
		if (node.isNumber()) {
			return node.asInt();
		} else if (node.isTextual()) {
			return convertToInteger(node.asText());
		} else if (node.isNull()) {
			return null;
		} else if (node.isMissingNode()) {
			return null;
		} else {
			throw new Exception("Node: " + node + " cannot be converted to Integer");
		}
	}

	public static Short convertToShort(String value) throws Exception {
		if (value == null || value.isEmpty()) {
			return null;
		}
		return Short.parseShort(value);
	}

	public static Short convertToShort(JsonNode node) throws Exception {
		if (node.isNumber()) {
			return (short) node.asInt();
		} else if (node.isTextual()) {
			return convertToShort(node.asText());
		} else if (node.isNull()) {
			return null;
		} else if (node.isMissingNode()) {
			return null;
		} else {
			throw new Exception("Node: " + node + " cannot be converted to Short");
		}
	}

	public static Character convertToChar(String value) throws Exception {
		if (value == null || value.isEmpty()) {
			return null;
		}
		return value.charAt(0);
	}

	public static String convertToString(String value) throws Exception {
		if (value == null || value.isEmpty()) {
			return null;
		}
		value = value.replace("\\n", "\n").replace("\\\"", "\"");
		return value;
	}

	public static Float convertToFloat(String value) throws Exception {
		if (value == null || value.isEmpty()) {
			return null;
		}
		return Float.parseFloat(value);
	}

	public static Float convertToFloat(JsonNode node) throws Exception {
		if (node.isNumber()) {
			return (float) node.asDouble();
		} else if (node.isTextual()) {
			return convertToFloat(node.asText());
		} else if (node.isNull()) {
			return null;
		} else if (node.isMissingNode()) {
			return null;
		} else {
			throw new Exception("Node: " + node + " cannot be converted to Float");
		}
	}

	public static Long convertToLong(String value) throws Exception {
		if (value == null || value.isEmpty()) {
			return null;
		}
		return Long.parseLong(value);
	}

	public static Long convertToLong(JsonNode node) throws Exception {
		if (node.isNumber()) {
			return node.asLong();
		} else if (node.isTextual()) {
			return convertToLong(node.asText());
		} else if (node.isNull()) {
			return null;
		} else if (node.isMissingNode()) {
			return null;
		} else {
			throw new Exception("Node: " + node + " cannot be converted to Long");
		}
	}

	public static BigDecimal convertToBigDecimal(String value) throws Exception {
		if (value == null || value.isEmpty()) {
			return null;
		}
		try {
			DecimalFormat decfrm = getNumberFormat(DEFAULT_LOCALE);
			decfrm.setParseBigDecimal(true);
			ParsePosition pp = new ParsePosition(0);
			return (BigDecimal) decfrm.parse(value, pp);
		} catch (RuntimeException e) {
			throw new Exception("convertToBigDecimal:" + value + " failed:" + e.getMessage(), e);
		}
	}

	public static BigDecimal convertToBigDecimal(JsonNode node) throws Exception {
		if (node.isNumber()) {
			return new BigDecimal(node.asText());
		} else if (node.isTextual()) {
			return convertToBigDecimal(node.asText());
		} else if (node.isNull()) {
			return null;
		} else {
			throw new Exception("Node: " + node + " cannot be converted to BigDecimal");
		}
	}

	public static BigInteger convertToBigInteger(String value) throws Exception {
		if (value == null || value.isEmpty()) {
			return null;
		}
		try {
			return new BigInteger(value);
		} catch (RuntimeException e) {
			throw new Exception("convertToBigDecimal:" + value + " failed:" + e.getMessage(), e);
		}
	}

	public static BigInteger convertToBigInteger(JsonNode node) throws Exception {
		if (node.isNumber()) {
			return node.bigIntegerValue();
		} else if (node.isTextual()) {
			return convertToBigInteger(node.asText());
		} else if (node.isNull()) {
			return null;
		} else {
			throw new Exception("Node: " + node + " cannot be converted to BigInteger");
		}
	}

	public static double roundScale(double value, int scale) {
    	double d = Math.pow(10, scale);
        return Math.round(value * d) / d;
    }
 
}
