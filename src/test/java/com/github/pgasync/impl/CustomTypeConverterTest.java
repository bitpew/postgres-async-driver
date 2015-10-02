package com.github.pgasync.impl;

import com.github.pgasync.ConnectionPoolBuilder;
import com.github.pgasync.Converter;
import org.junit.AfterClass;
import org.junit.BeforeClass;
import org.junit.ClassRule;
import org.junit.Test;

import java.io.UnsupportedEncodingException;
import java.net.Inet4Address;
import java.net.Inet6Address;
import java.net.InetAddress;
import java.net.UnknownHostException;
import java.util.Arrays;

import static com.github.pgasync.impl.DatabaseRule.envOrDefault;
import static org.junit.Assert.assertEquals;

/**
 * Conversion tests from/to SQL types using custom converters.
 *
 * @author Marc Dietrichstein
 */
public class CustomTypeConverterTest {

    private static final InetAddress IPV4_ADDRESS = toInetAddress("127.0.0.1");
    private static final InetAddress IPV6_ADDRESS = toInetAddress("2001:db8:85a3::8a2e:370:7334");

    @ClassRule
    public static final DatabaseRule dbr = new DatabaseRule(new ConnectionPoolBuilder()
            .database(envOrDefault("PG_DATABASE", "postgres"))
            .username(envOrDefault("PG_USERNAME", "postgres"))
            .password(envOrDefault("PG_PASSWORD", "postgres"))
            .converters(new InetAddressConverter())
            .ssl(true)
            .poolSize(1)
    );


    @BeforeClass
    public static void create() throws UnknownHostException {
        drop();
        dbr.query("CREATE TABLE CTC_TEST (TYPE TEXT, ADDR INET)");
        dbr.query("INSERT INTO CTC_TEST VALUES($1, $2)", Arrays.asList("IPV4", IPV4_ADDRESS));
        dbr.query("INSERT INTO CTC_TEST VALUES($1, $2)", Arrays.asList("IPV6", IPV6_ADDRESS));
    }

    @AfterClass
    public static void drop() {
        dbr.query("DROP TABLE IF EXISTS CTC_TEST");
    }

    @Test
    public void shouldConvertToInetAddress() throws UnknownHostException {
        assertEquals(IPV4_ADDRESS, dbr.query("SELECT ADDR from CTC_TEST WHERE TYPE=$1", Arrays.asList("IPV4")).row(0).get(0, InetAddress.class));
        assertEquals(IPV6_ADDRESS, dbr.query("SELECT ADDR from CTC_TEST WHERE TYPE=$1", Arrays.asList("IPV6")).row(0).get(0, InetAddress.class));
    }

    @Test
    public void shouldConvertToInet4Address() throws UnknownHostException {
        assertEquals(IPV4_ADDRESS, dbr.query("SELECT ADDR from CTC_TEST WHERE TYPE=$1", Arrays.asList("IPV4")).row(0).get(0, Inet4Address.class));
    }

    @Test
    public void shouldConvertToInet6Address() throws UnknownHostException {
        assertEquals(IPV6_ADDRESS, dbr.query("SELECT ADDR from CTC_TEST WHERE TYPE=$1", Arrays.asList("IPV6")).row(0).get(0, Inet6Address.class));
    }

    private static InetAddress toInetAddress(String address) {
        try {
            return InetAddress.getByName(address);
        } catch (UnknownHostException e) {
            throw new RuntimeException(e);
        }
    }

    /**
     * Custom converter for the postgres INET type.
     *
     * Should not be used in production since it calls java.net.InetAddress.getByName which might try to resolve names via DNS.
     *
     */
    private static final class InetAddressConverter implements Converter<InetAddress> {

        @Override
        public Class<InetAddress> type() {
            return InetAddress.class;
        }

        @Override
        public byte[] from(InetAddress o) {
            try {
                return o.getHostAddress().getBytes("UTF-8");
            } catch (UnsupportedEncodingException e) {
                throw new RuntimeException(e);
            }
        }

        @Override
        public InetAddress to(Oid oid, byte[] value) {
            try {
                return InetAddress.getByName(new String(value, "UTF-8"));
            } catch (UnknownHostException|UnsupportedEncodingException e) {
                throw new RuntimeException(e);
            }
        }
    }
}
