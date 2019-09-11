/*
 * Copyright (c) 2018 The original author or authors
 * ------------------------------------------------------
 * All rights reserved. This program and the accompanying materials
 * are made available under the terms of the Eclipse Public License v1.0
 * and Apache License v2.0 which accompanies this distribution.
 *
 *     The Eclipse Public License is available at
 *     http://www.eclipse.org/legal/epl-v10.html
 *
 *     The Apache License v2.0 is available at
 *     http://www.opensource.org/licenses/apache2.0.php
 *
 * You may elect to redistribute this code under either of these licenses.
 */
package io.vertx.spi.cluster.redis;

import java.net.InetAddress;
import java.net.NetworkInterface;
import java.net.UnknownHostException;
import java.util.Enumeration;
import java.util.StringTokenizer;
import java.util.function.Function;
import java.util.regex.Pattern;

import org.junit.Assert;
import org.junit.FixMethodOrder;
import org.junit.Test;
import org.junit.runners.MethodSorters;

import io.vertx.core.logging.Logger;
import io.vertx.core.logging.LoggerFactory;
import io.vertx.core.logging.SLF4JLogDelegateFactory;

/**
 * 
 * @author <a href="mailto:leo.tu.taipei@gmail.com">Leo Tu</a>
 */
@FixMethodOrder(MethodSorters.NAME_ASCENDING)
public class IpUtil {
  static {
    System.setProperty(LoggerFactory.LOGGER_DELEGATE_FACTORY_CLASS_NAME, SLF4JLogDelegateFactory.class.getName());
  }
  private static final Logger log = LoggerFactory.getLogger(IpUtil.class);

  private static final String VALID_IP_ADDRESS_REGEX = //
      "^([01]?\\d\\d?|2[0-4]\\d|25[0-5])\\." + //
          "([01]?\\d\\d?|2[0-4]\\d|25[0-5])\\." + //
          "([01]?\\d\\d?|2[0-4]\\d|25[0-5])\\." + //
          "([01]?\\d\\d?|2[0-4]\\d|25[0-5])$";

  // Pattern objects are thread safe
  private static final Pattern VALID_IP_PATTERN = Pattern.compile(VALID_IP_ADDRESS_REGEX);

  static public boolean validIpAddress(String hostAddress) {
    if (hostAddress.indexOf(':') != -1 || new StringTokenizer(hostAddress, ".").countTokens() != 4) {
      return false;
    }
    return VALID_IP_PATTERN.matcher(hostAddress).matches();
  }

  static public String getLocalRealIP() {
    return getLocalRealIP((hostAddress) -> {
      return validIpAddress(hostAddress);
    });
  }

  static public String getLocalRealIP(Function<String, Boolean> filter) {
    try {
      Enumeration<NetworkInterface> nis = NetworkInterface.getNetworkInterfaces();
      for (; nis.hasMoreElements();) {
        NetworkInterface ni = nis.nextElement();
        Enumeration<InetAddress> ia = ni.getInetAddresses();
        for (; ia.hasMoreElements();) {
          InetAddress addr = ia.nextElement();
          String hostAddress = addr.getHostAddress();
          if (addr.isSiteLocalAddress() && !addr.isLoopbackAddress()) {
            if (filter != null) {
              if (filter.apply(hostAddress)) {
                return hostAddress;
              }
            } else {
              return hostAddress;
            }
          } else if (validIpAddress(hostAddress) && !addr.isLoopbackAddress()) {
            return hostAddress;
          }
        }
      }
      //
      try {
        return InetAddress.getLocalHost().getHostAddress();
      } catch (UnknownHostException ee) {
        log.warn(ee.toString());
        return "127.0.0.1";
      }
    } catch (Exception e) {
      log.warn(e.toString());
      try {
        return InetAddress.getLocalHost().getHostAddress();
      } catch (UnknownHostException ee) {
        log.warn(ee.toString());
        return "127.0.0.1";
      }
    }
  }

  @Test
  public void test1True() {
    log.debug("BEGIN...");

    log.debug("========== true =======");
    String[] ips = new String[] { //
        "1.1.1.1", //
        "255.255.255.255", //
        "192.168.1.1", //
        "10.10.1.1", //
        "132.254.111.10", //
        "26.10.2.10", //
        "127.0.0.1" };

    for (String ip : ips) {
      log.debug("{} = {}", ip, validIpAddress(ip));
      Assert.assertTrue(validIpAddress(ip));
    }
  }

  @Test
  public void test2False() {
    log.debug("========== false =======");
    String[] ips2 = new String[] { //
        "10.10.10", //
        "10.10", //
        "10", //
        "a.a.a.a", //
        "10.0.0.a", //
        "10.10.10.256", //
        "222.222.2.999", //
        "999.10.10.20", //
        "2222.22.22.22", //
        "22.2222.22.2", //
        "10.10.10", //
        "10.10.10" };
    for (String ip : ips2) {
      log.debug("{} = {}", ip, validIpAddress(ip));
      Assert.assertFalse(validIpAddress(ip));
    }

    log.debug("END.");
  }
}
