/*
 * Copyright © 2013-2014.
 * 
 * Licensed under the Apache License, Version 2.0 (the "License");
 * you may not use this file except in compliance with the License.
 * You may obtain a copy of the License at
 *
 *      http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */

package io.vertx.spi.cluster.redis.impl;

import java.lang.reflect.Field;
import java.lang.reflect.InvocationTargetException;
import java.lang.reflect.Method;
import java.lang.reflect.Modifier;
import java.util.ArrayList;
import java.util.List;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

/**
 * Reflect Utility.
 * 
 * @author Leo Tu - leo.tu.taipei@gmail.com
 */
public class ReflectUtil {
	static protected Logger log = LoggerFactory.getLogger(ReflectUtil.class);

	static private boolean android = false; // java.lang.NoSuchFieldException: modifiers

	public static void setAndroid(boolean android) {
		ReflectUtil.android = android;
	}

	public static boolean isAndroid() {
		return android;
	}

	protected ReflectUtil() {
	}

	static public <T> T getField(Object reflectObj, String fieldName) {
		if (reflectObj == null) {
			throw new IllegalArgumentException("(reflectObj == null)");
		}
		return getField(reflectObj, reflectObj.getClass(), fieldName);
	}

	/**
	 * If the underlying field is a static field, the reflectObj argument is ignored; it may be null.
	 * 
	 * @param reflectObj
	 *            may be null.
	 */
	static public <T> T getField(Object reflectObj, Class<?> clsObj, String fieldName) {
		if (clsObj == null) {
			throw new IllegalArgumentException("(clsObj == null)");
		}
		if (fieldName == null || fieldName.length() == 0) {
			throw new IllegalArgumentException(
					"(fieldName == null || fieldName.length() == 0), fieldName=[" + fieldName + "]");
		}
		try {
			Field field = clsObj.getDeclaredField(fieldName);
			boolean keepStatus = field.isAccessible();
			if (!keepStatus) {
				field.setAccessible(true);
			}
			try {
				Object fieldObj = field.get(reflectObj);
				@SuppressWarnings("unchecked")
				T t = (T) fieldObj;
				return t;
			} finally {
				field.setAccessible(keepStatus);
			}
		} catch (Exception e) {
			String msg = "reflectObj=[" + (reflectObj == null ? "<null>" : reflectObj.getClass().getName())
					+ "], clsObj=[" + clsObj.getName() + "], fieldName=[" + fieldName + "]";
			log.warn(msg + ": " + e.toString());
			throw new RuntimeException(msg, e);
		}
	}

	// /**
	// * is private field
	// */
	// static public boolean isPrivateField(Object reflectObj, Class<?> clsObj, String fieldName) {
	// if (clsObj == null) {
	// throw new IllegalArgumentException("(clsObj == null)");
	// }
	// if (fieldName == null || fieldName.length() == 0) {
	// throw new IllegalArgumentException("(fieldName == null || fieldName.length() == 0), fieldName=["
	// + fieldName + "]");
	// }
	// try {
	// Field field = clsObj.getDeclaredField(fieldName);
	// return (field.getModifiers() & Modifier.PRIVATE) != 0;
	// } catch (Exception e) {
	// String msg = "reflectObj=[" + (reflectObj == null ? "<null>" : reflectObj.getClass().getName())
	// + "], clsObj=[" + clsObj.getName() + "], fieldName=[" + fieldName + "]";
	// log.warn(msg + ": " + e.toString());
	// throw new RuntimeException(msg, e);
	// }
	// }
	//
	// static public boolean isPrivateStaticField(Class<?> clsObj, String staticFieldName) {
	// return isPrivateField(null, clsObj, staticFieldName);
	// }

	/**
	 * If the underlying field is a static field, the reflectObj argument is ignored; it may be null.
	 */
	static public <T> T getFinalField(Object reflectObj, String fieldName) {
		if (reflectObj == null) {
			throw new IllegalArgumentException("(reflectObj == null)");
		}
		return getFinalField(reflectObj, reflectObj.getClass(), fieldName);
	}

	/**
	 * If the underlying field is a static field, the reflectObj argument is ignored; it may be null.
	 * 
	 * @param reflectObj
	 *            may be null.
	 */
	static public <T> T getFinalField(Object reflectObj, Class<?> clsObj, String fieldName) {
		if (clsObj == null) {
			throw new IllegalArgumentException("(clsObj == null)");
		}
		if (fieldName == null || fieldName.length() == 0) {
			throw new IllegalArgumentException(
					"(fieldName == null || fieldName.length() == 0), fieldName=[" + fieldName + "]");
		}
		try {
			Field field = clsObj.getDeclaredField(fieldName);
			boolean keepStatus = field.isAccessible();
			// System.out.println("keepStatus=" + keepStatus);
			if (!keepStatus) {
				field.setAccessible(true);
			}
			try {
				if (!android) {
					Field modifiersField = Field.class.getDeclaredField("modifiers");
					modifiersField.setAccessible(true);
					modifiersField.setInt(field, field.getModifiers() & ~Modifier.FINAL);
				}
				//
				Object fieldObj = field.get(reflectObj);
				@SuppressWarnings("unchecked")
				T t = (T) fieldObj;
				return t;
			} finally {
				field.setAccessible(keepStatus);
			}
		} catch (Exception e) {
			String msg = "reflectObj=[" + (reflectObj == null ? "<null>" : reflectObj.getClass().getName())
					+ "], clsObj=[" + clsObj.getName() + "], fieldName=[" + fieldName + "]";
			log.warn(msg + ": " + e.toString());
			throw new RuntimeException(msg, e);
		}
	}

	static public <T> T getField(Object reflectObj, Field field) {
		if (reflectObj == null) {
			throw new IllegalArgumentException("(reflectObj == null)");
		}
		return getFieldValue(reflectObj, field);
	}

	static protected <T> T getFieldValue(Object reflectObj, Field field) {
		if (field == null) {
			throw new IllegalArgumentException("(field == null)");
		}
		try {
			boolean keepStatus = field.isAccessible();
			if (!keepStatus) {
				field.setAccessible(true);
			}
			try {
				Object fieldObj = field.get(reflectObj);
				@SuppressWarnings("unchecked")
				T t = (T) fieldObj;
				return t;
			} finally {
				field.setAccessible(keepStatus);
			}
		} catch (Exception e) {
			String msg = "reflectObj=[" + (reflectObj == null ? "<null>" : reflectObj.getClass().getName())
					+ "], field=[" + field + "]";
			log.warn(msg + ": " + e.toString());
			throw new RuntimeException(msg, e);
		}
	}

	static public void setField(Object reflectObj, Field field, Object newValue) {
		if (reflectObj == null) {
			throw new IllegalArgumentException("(reflectObj == null)");
		}
		setFieldValue(reflectObj, field, newValue);
	}

	static protected void setFieldValue(Object reflectObj, Field field, Object newValue) {
		if (field == null) {
			throw new IllegalArgumentException("(field == null)");
		}
		try {
			boolean keepStatus = field.isAccessible();
			if (!keepStatus) {
				field.setAccessible(true);
			}
			try {
				field.set(reflectObj, newValue);
			} finally {
				field.setAccessible(keepStatus);
			}
		} catch (Exception e) {
			String msg = "reflectObj=[" + (reflectObj == null ? "<null>" : reflectObj.getClass().getName())
					+ "], field=[" + field + "]";
			log.warn(msg + ": " + e.toString());
			throw new RuntimeException(msg, e);
		}
	}

	/**
	 * If the underlying field is a static field, the reflectObj argument is ignored; it may be null.
	 *
	 * @param reflectObj
	 *            may be null.
	 */
	static public void setField(Object reflectObj, Class<?> clsObj, String fieldName, Object newValue) {
		if (clsObj == null) {
			throw new IllegalArgumentException("(clsObj == null)");
		}
		if (fieldName == null || fieldName.length() == 0) {
			throw new IllegalArgumentException(
					"(fieldName == null || fieldName.length() == 0), fieldName=[" + fieldName + "]");
		}
		try {
			Field field = clsObj.getDeclaredField(fieldName);
			boolean keepStatus = field.isAccessible();
			if (!keepStatus) {
				field.setAccessible(true);
			}
			try {
				field.set(reflectObj, newValue);
			} finally {
				field.setAccessible(keepStatus);
			}
		} catch (Exception e) {
			String msg = "reflectObj=[" + (reflectObj == null ? "<null>" : reflectObj.getClass().getName())
					+ "], clsObj=[" + clsObj.getName() + "], fieldName=[" + fieldName + "]";
			log.warn(msg + ": " + e.toString());
			throw new RuntimeException(msg, e);
		}
	}

	/**
	 * If the underlying field is a static field, the reflectObj argument is ignored; it may be null.
	 *
	 * @param reflectObj
	 *            may be null.
	 */
	static public void setFinalField(Object reflectObj, Class<?> clsObj, String fieldName, Object newValue) {
		if (clsObj == null) {
			throw new IllegalArgumentException("(clsObj == null)");
		}
		if (fieldName == null || fieldName.length() == 0) {
			throw new IllegalArgumentException(
					"(fieldName == null || fieldName.length() == 0), fieldName=[" + fieldName + "]");
		}
		try {
			Field field = clsObj.getDeclaredField(fieldName);
			boolean keepStatus = field.isAccessible();
			if (!keepStatus) {
				field.setAccessible(true);
			}
			if (!android) {
				Field modifiersField = Field.class.getDeclaredField("modifiers");
				modifiersField.setAccessible(true);
				modifiersField.setInt(field, field.getModifiers() & ~Modifier.FINAL);
				try {
					field.set(reflectObj, newValue);
				} finally {
					field.setAccessible(keepStatus);
					modifiersField.setInt(field, field.getModifiers() & Modifier.FINAL);
				}
			} else {
				try {
					field.set(reflectObj, newValue);
				} finally {
					field.setAccessible(keepStatus);
				}
			}
		} catch (Exception e) {
			String msg = "reflectObj=[" + (reflectObj == null ? "<null>" : reflectObj.getClass().getName())
					+ "], clsObj=[" + clsObj.getName() + "], fieldName=[" + fieldName + "]";
			log.warn(msg + ": " + e.toString());
			throw new RuntimeException(msg, e);
		}
	}

	static public <T> T getStaticField(Class<?> clsObj, String staticFieldName) {
		return getField(null, clsObj, staticFieldName);
	}
	
	static public <T> T getStaticFinalField(Class<?> clsObj, String staticFieldName) {
		return getFinalField(null, clsObj, staticFieldName);
	}

	static public void setField(Object reflectObj, String fieldName, Object newValue) {
		Class<?> clsObj = reflectObj.getClass();
		setField(reflectObj, clsObj, fieldName, newValue);
	}

	static public void setStaticField(Class<?> clsObj, String fieldName, Object newValue) {
		setField(null, clsObj, fieldName, newValue);
	}

	static public void setStaticFinalField(Class<?> clsObj, String fieldName, Object newValue) {
		setFinalField(null, clsObj, fieldName, newValue);
	}

	static public void listField(Class<?> clsObj) {
		listField(clsObj, log);
	}

	static public void listField(Class<?> clsObj, Logger log) {
		if (clsObj == null) {
			throw new IllegalArgumentException("(clsObj == null)");
		}
		Field[] fields = clsObj.getDeclaredFields(); // getFields();
		for (int i = 0; i < fields.length; i++) {
			log.debug("fields[" + i + "], getName=[" + fields[i].getName() + "], toString=[" + fields[i].toString()
					+ "], isAccessible=[" + fields[i].isAccessible() + "]");
		}
	}

	static public void listField(Object reflectObj) {
		listField(reflectObj, log);
	}

	static public void listField(Object reflectObj, Logger log) {
		listField(reflectObj.getClass(), log);
	}

	static public void listMethod(Class<?> clsObj) {
		listMethod(clsObj, log);
	}

	static public void listMethod(Class<?> clsObj, Logger log) {
		if (clsObj == null) {
			throw new IllegalArgumentException("(clsObj == null)");
		}
		Method[] methods = clsObj.getDeclaredMethods(); // getMethods();
		for (int i = 0; i < methods.length; i++) {
			log.debug("methods[" + i + "], getName=[" + methods[i].getName() + "], toString=[" + methods[i].toString()
					+ "], isAccessible=[" + methods[i].isAccessible() + "]");
		}
	}

	static public void listMethod(Object reflectObj) {
		listMethod(reflectObj, log);
	}

	static public void listMethod(Object reflectObj, Logger log) {
		listMethod(reflectObj.getClass(), log);
	}

	static public Method getMethod(Object reflectObj, String methodName, Class<?>[] argsTypes) {
		return getMethod(reflectObj.getClass(), methodName, argsTypes);
	}

	static public Method getMethod(Class<?> clsObj, String methodName, Class<?>[] argsTypes) {
		try {
			Method method = clsObj.getDeclaredMethod(methodName, argsTypes);
			boolean keepStatus = method.isAccessible();
			if (!keepStatus) {
				method.setAccessible(true);
			}
			return method;
		} catch (Exception e) {
			Throwable ee = e;
			while (ee.getCause() != null && ee instanceof InvocationTargetException
					&& ee.getCause() instanceof Exception) {
				ee = (Exception) ee.getCause();
			}
			String msg = "clsObj=[" + clsObj.getName() + "], methodName=[" + methodName + "]";
			if (ee != e && ee instanceof RuntimeException) {
				log.debug(msg + ": " + e.toString());
				throw (RuntimeException) ee;
			} else {
				log.debug(msg + ": " + e.toString());
				// Thread.dumpStack();
				throw new RuntimeException(msg, e);
			}
		}
	}

	static public Method[] getMethod(Class<?> clsObj, String methodName) {
		Method[] methods = clsObj.getDeclaredMethods();
		List<Method> findMethods = new ArrayList<>();
		for (Method method : methods) {
			if (method.getName().equals(methodName)) {
				findMethods.add(method);
			}
		}
		return findMethods.toArray(new Method[0]);
	}
	

	static public Method getStaticMethod(Class<?> clsObj, String staticFieldName, Class<?>... argsTypes) {
		return getMethod(clsObj, staticFieldName, argsTypes);
	}


	/**
	 * @param reflectObj
	 *            may be null.
	 */
	static public <T> T callMethod(Object reflectObj, Class<?> clsObj, String methodName, Class<?>[] argsTypes,
			Object[] argsValues) {
		if (clsObj == null) {
			throw new IllegalArgumentException("(clsObj == null)");
		}
		if (methodName == null || methodName.length() == 0) {
			throw new IllegalArgumentException(
					"(methodName == null || methodName.length() == 0), methodName=[" + methodName + "]");
		}
		try {
			Method method = clsObj.getDeclaredMethod(methodName, argsTypes);
			// Method method = clsObj.getMethod(methodName, argsTypes);
			boolean keepStatus = method.isAccessible();
			if (!keepStatus) {
				method.setAccessible(true);
			}
			try {
				@SuppressWarnings("unchecked")
				T obj = (T) method.invoke(reflectObj, argsValues);
				return obj;
			} finally {
				method.setAccessible(keepStatus);
			}
		} catch (Exception e) {
			Throwable ee = e;
			while (ee.getCause() != null && ee instanceof InvocationTargetException
					&& ee.getCause() instanceof Exception) {
				ee = (Exception) ee.getCause();
			}
			String msg = "reflectObj=[" + (reflectObj == null ? "<null>" : reflectObj.getClass().getName())
					+ "], clsObj=[" + clsObj.getName() + "], methodName=[" + methodName + "]";
			if (ee != e && ee instanceof RuntimeException) {
				log.warn(msg + ": " + e.toString());
				throw (RuntimeException) ee;
			} else {
				log.warn(msg + ": " + e.toString());
				throw new RuntimeException(msg, e);
			}
		}
	}

	static public <T> T callMethod(Object reflectObj, Method method, Object argValue) {
		return callMethod(reflectObj, method, (Class<?>[]) null, new Object[] { argValue });
	}

	@Deprecated
	static public <T> T callMethod(Object reflectObj, Method method, Class<?> argType, Object argValue) {
		return callMethod(reflectObj, method, new Class<?>[] { argType }, new Object[] { argValue });
	}

	/**
	 * @param reflectObj
	 *            may be null.
	 */
	static public <T> T callMethod(Object reflectObj, Method method, Object[] argsValues) {
		return callMethod(reflectObj, method, (Class<?>[]) null, argsValues);
	}

	/**
	 * @param reflectObj
	 *            may be null.
	 */
	@Deprecated
	static public <T> T callMethod(Object reflectObj, Method method, Class<?>[] argsTypes, Object[] argsValues) {
		if (method == null) {
			throw new IllegalArgumentException("(method == null)");
		}
		if (method.getParameterTypes().length != argsValues.length) {
			throw new IllegalArgumentException(
					"(method.getParameterTypes().length != argsValues.length), method.parameterTypes.length="
							+ method.getParameterTypes().length + ", argsValues.length=" + argsValues.length);
		}
		try {
			boolean keepStatus = method.isAccessible();
			if (!keepStatus) {
				method.setAccessible(true);
			}
			try {
				@SuppressWarnings("unchecked")
				T obj = (T) method.invoke(reflectObj, argsValues);
				return obj;
			} finally {
				method.setAccessible(keepStatus);
			}
		} catch (Exception e) {
			Throwable ee = e;
			while (ee.getCause() != null && ee instanceof InvocationTargetException
					&& ee.getCause() instanceof Exception) {
				ee = (Exception) ee.getCause();
			}
			String msg = "reflectObj=[" + (reflectObj == null ? "<null>" : reflectObj.getClass().getName())
					+ "], methodName=[" + method.getName() + "]";
			if (ee != e && ee instanceof RuntimeException) {
				log.warn(msg + ": " + e.toString());
				throw (RuntimeException) ee;
			} else {
				log.warn(msg + ": " + e.toString());
				throw new RuntimeException(msg, e);
			}
		}
	}

	static public <T> T callStaticMethod(Class<?> clsObj, String staticMethodName) {
		return callMethod(null, clsObj, staticMethodName, new Class[0], new Object[0]);
	}

	static public <T> T callStaticMethod(Class<?> clsObj, String staticMethodName, Class<?>[] argsTypes,
			Object[] argsValues) {
		return callMethod(null, clsObj, staticMethodName, argsTypes, argsValues);
	}

	static public <T> T callStaticMethod(Method method, Object... argsValues) {
		return callMethod(null, method, (Class<?>[]) null, argsValues);
	}

	static public <T> T callMethod(Object reflectObj, Class<?> clsObj, String methodName) {
		return callMethod(reflectObj, clsObj, methodName, new Class[0], new Object[0]);
	}

	static public <T> T callMethod(Object reflectObj, String methodName, Class<?>[] argsTypes, Object[] argsValues) {
		Class<?> clsObj = reflectObj.getClass();
		return callMethod(reflectObj, clsObj, methodName, argsTypes, argsValues);
	}

	static public <T> T callMethod(Object reflectObj, String methodName) {
		Class<?> clsObj = reflectObj.getClass();
		return callMethod(reflectObj, clsObj, methodName, new Class[0], new Object[0]);
	}

	static public <T> T callMethod(Object reflectObj, Method method) {
		return callMethod(reflectObj, method, new Class[0], new Object[0]);
	}

	// ===
	static public boolean existField(Class<?> clsObj, String fieldName, boolean mustStatic) {
		if (clsObj == null) {
			throw new IllegalArgumentException("(clsObj == null)");
		}
		if (fieldName == null || fieldName.length() == 0) {
			throw new IllegalArgumentException(
					"(fieldName == null || fieldName.length() == 0), fieldName=[" + fieldName + "]");
		}
		try {
			Field field = clsObj.getDeclaredField(fieldName);
			if (mustStatic) {
				return Modifier.isStatic(field.getModifiers());
			} else {
				return true;
			}
		} catch (NoSuchFieldException e) {
			return false;
		}
	}

	static public boolean existField(Class<?> clsObj, String fieldName) {
		return existField(clsObj, fieldName, false);
	}

	static public boolean existStaticField(Class<?> clsObj, String staticFieldName) {
		return existField(clsObj, staticFieldName, true);
	}

	// ===
	static public boolean existMethod(Class<?> clsObj, String methodName, Class<?>[] argsTypes, boolean mustStatic) {
		if (clsObj == null) {
			throw new IllegalArgumentException("(clsObj == null)");
		}
		if (methodName == null || methodName.length() == 0) {
			throw new IllegalArgumentException(
					"(methodName == null || methodName.length() == 0), methodName=[" + methodName + "]");
		}
		try {
			Method method = clsObj.getDeclaredMethod(methodName, argsTypes);
			if (mustStatic) {
				return Modifier.isStatic(method.getModifiers());
			} else {
				return true;
			}
		} catch (NoSuchMethodException e) {
			return false;
		}
	}

	static public boolean existMethod(Class<?> clsObj, String methodName, Class<?>[] argsTypes) {
		return existMethod(clsObj, methodName, argsTypes, false);
	}

	static public boolean existStaticMethod(Class<?> clsObj, String staticFieldName, Class<?>[] argsTypes) {
		return existMethod(clsObj, staticFieldName, argsTypes, true);
	}

}