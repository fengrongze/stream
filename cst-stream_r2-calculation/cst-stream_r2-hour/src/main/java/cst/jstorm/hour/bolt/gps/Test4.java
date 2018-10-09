package cst.jstorm.hour.bolt.gps;

import org.apache.commons.lang.math.NumberUtils;


/**
 * @author Johnney.Chiu
 * create on 2018/3/9 15:42
 * @Description test
 * @title
 */
public class Test4 {

    public static void main(String ... args){
        /*List<String> timeArea = DateTimeUtil.calcLongTimeBetween(
                1520560904000L, 1520565175000L, DateTimeUtil.DEFAULT_DATE_HOUR, DateTimeUtil.ONE_HOUR);
        for (String str:timeArea){
            System.out.println(str);

        }*/
        /*String msg = "{\"lastTime\":1520302553000,\"data\":{\"@type\":\"AmHourTransfor\",\"carId\":\"8eed87672af24dc89155499f3aa7f3e6\",\"time\":1520302553000,\"ignition\":48,\"flameOut\":0,\"insertNum\":0,\"coll\n" +
                "ision\":0,\"overSpeed\":0,\"isMissing\":0,\"pulloutTimes\":0.0,\"isFatigue\":0}}";*/
        /*AmHourTransfor amHourTransfor = new AmHourTransfor("8eed87672af24dc89155499f3aa7f3e6", 1111l, 100, 100, 100, 100, 100,
                100, 100, 100);
        try {
        LastTimeData<AmHourTransfor> lastTimeData=new LastTimeData<>(amHourTransfor.getTime(),amHourTransfor);
        String msg = JsonHelper.toString(lastTimeData);
        System.out.println(msg);


        lastTimeData = JsonHelper.toBean(msg, new TypeReference<LastTimeData<AmHourTransfor>>() {
        });
        } catch (IOException e) {
            e.printStackTrace();
        }*/
        Float f = 10F;
        Float f1 = null;


        System.out.println(NumberUtils.max(7.1, 1.1, 7.0));
    }
}
