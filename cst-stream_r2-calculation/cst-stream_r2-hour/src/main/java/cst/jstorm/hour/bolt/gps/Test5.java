package cst.jstorm.hour.bolt.gps;

import com.cst.stream.common.*;

import java.io.IOException;
import java.text.ParseException;
import java.util.List;

/**
 * @author Johnney.Chiu
 * create on 2018/3/14 17:36
 * @Description aaa
 * @title
 */
public class Test5 {

    public static void main(String... args) throws IOException, ParseException {

        //test1();
        test2();

    }

    public static void test1() throws ParseException {
        String test= StreamRedisConstants.StreamRedisFormat.getFirstZoneRedisKey(
                StreamRedisConstants.HourKey.HOUR_AM,
                "68cabd75469c4ecd97070f9a7506272a",
                DateTimeUtil.toLongTimeString(1528886839000L, DateTimeUtil.DEFAULT_DATE_HOUR));
        System.out.println(test);
        String latestZoneRedisKey = StreamRedisConstants.StreamRedisFormat.getLatestZoneRedisKey(StreamRedisConstants.HourKey.HOUR_DE,
                "M502801012882");
        System.out.println(latestZoneRedisKey);


        String rowKey = RowKeyGenerate.getRowKeyById("68cabd75469c4ecd97070f9a7506272a", 1528884251000L, CstConstants.TIME_SELECT.HOUR, StreamTypeDefine.GPS_TYPE);

        System.out.println(rowKey);
    }

    public static void test2() throws ParseException {
        List<String> strs= DateTimeUtil.calcLongTimeBetween(1530663370000L, 1530670750000L, DateTimeUtil.DEFAULT_DATE_HOUR, DateTimeUtil.ONE_HOUR);
        strs.stream().forEach(System.out::println);
    }


}
