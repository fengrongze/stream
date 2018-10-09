package cst.jstorm.hour.bolt.gps;

import com.alibaba.fastjson.JSON;
import com.cst.stream.stathour.LastTimeData;
import com.cst.stream.stathour.am.AmHourTransfor;
import com.cst.stream.stathour.gps.GpsHourSource;
import com.cst.stream.stathour.gps.GpsHourTransfor;
import com.cst.jstorm.commons.utils.CryptogramUtil;
import com.cst.jstorm.commons.utils.http.HttpResult;
import com.cst.jstorm.commons.utils.http.HttpUtils;
import com.cst.jstorm.commons.utils.spring.MyApplicationContext;
import org.apache.http.client.config.RequestConfig;
import org.apache.http.impl.client.CloseableHttpClient;
import org.springframework.context.support.AbstractApplicationContext;
import org.springframework.context.support.ClassPathXmlApplicationContext;

import java.io.IOException;
import java.io.Serializable;
import java.io.UnsupportedEncodingException;
import java.net.URLEncoder;
import java.util.Arrays;
import java.util.HashMap;
import java.util.List;

/**
 * @author Johnney.chiu
 * create on 2017/11/30 14:33
 * @Description ss
 */
public class Test implements Serializable{

    public static void main(String... args)  {

        try {
            List<String> list = Arrays.asList("xiaoq,xiaomei".split(","));
            list.stream().forEach(System.out::println);
            GpsHourTransfor gpsHourTransfor = new GpsHourTransfor("72b41560207a46ac9dc6e647c38ff086", 1514889673000l, 22,  0
                    , 1);


            LastTimeData<GpsHourTransfor> lastTimeData = new LastTimeData<>(1514889673000l, gpsHourTransfor);
            System.out.println();

            GpsHourSource gpsHourSource = new GpsHourSource();
            LastTimeData<GpsHourSource> lastTimeData1 = new LastTimeData<>(1514889673000l,gpsHourSource);
//            objectMapper.writerFor(new TypeReference<LastTimeData<GpsHourSource>>() {
//            });
            System.out.println(JSON.toJSONString(gpsHourSource));
            System.out.println(JSON.toJSONString(lastTimeData1));
            //test3();
            //test4();
           /* int[] a = new int[10];
            int[] b = Arrays.copyOf(a, a.length + 1);
            b[a.length] = 10;
            System.out.println(b);
            CSTData amHourSource = new AmHourSource();
            String json = objectMapper.writeValueAsString(amHourSource);
            System.out.println(json);*/
        } catch (Exception e) {
            e.printStackTrace();
        }
    }

    public static void test3()throws IOException{
        AbstractApplicationContext context = MyApplicationContext.getDefaultContext();
        HttpUtils httpUtils = (HttpUtils) context.getBean("httpUtils");
        long ex1 = System.currentTimeMillis();
        HttpResult httpResult = null;
        long l = System.currentTimeMillis() ;
        AmHourTransfor amHourTransfor = new AmHourTransfor("0000139ca0124edb90cb5a5d27ab2b44", l,
                1, 2, 3, 4, 5, 0, 7f, 1,0);
        String json = JSON.toJSONString(amHourTransfor);
        System.out.println(json);
        /*json = "{\"@type\":\"AmHourTransfor\",\"carId\":\"0000139ca0124edb90cb5a5d27ab2b43\",\"time\":1514173787,\"ignition\":1,\"flameOut\":2,\"insertNum\":3,\"collision\":4,\"overSpeed\":5,\"isMissing\":0,\"pulloutTimes\":7.0,\"isFatigue\":1}";
        //amHourTransfor = objectMapper.readValue(json, AmHourTransfor.class);
        System.out.println(json);*/
        httpResult = httpUtils.putjson("http://localhost:8088/stream/hour/am/save",json
                , new HashMap<String, String>() {{
                    put("Content-Type", "application/json");
                }});
        System.out.println("========result put======");
        System.out.println(httpResult==null?"-------------------":httpResult.getData());
        System.out.println("========result put======");
        httpResult = httpUtils.get("http://localhost:8088/stream/hour/am/find/" + amHourTransfor.getCarId()+"/"+l,
                new HashMap<String, String>() {{
                    put("Content-Type", "application/json");
                }});
        System.out.println("========result get======");
        System.out.println(httpResult==null?"-------------------":httpResult.getData());
        System.out.println("========result get======");
        context.close();
    }

    public static void test1() {
        long annotation = 0;
        long xml = 0;
        int i;
        for (i = 0; i < 100; i++) {
            //AnnotationContext
            AbstractApplicationContext context = MyApplicationContext.getDefaultContext();
            HttpUtils httpUtils = (HttpUtils) context.getBean("httpUtils");
            long ex1 = System.currentTimeMillis();
            String rowId = "row7";
            HttpResult str = httpUtils.get("http://localhost:8088/users/find/" + rowId,
                    new HashMap<String, String>() {{
                        put("Content-Type", "application/json");
                    }});
            annotation += (System.currentTimeMillis() - ex1);
            System.out.println(str);

            context.close();

            //XmlContext
            AbstractApplicationContext context2 = new ClassPathXmlApplicationContext("springs/spring-httpclient.xml");

            CloseableHttpClient closeableHttpClient = (CloseableHttpClient) context2.getBean("httpClient");
            RequestConfig requestConfig = (RequestConfig) context2.getBean("requestConfig");
            httpUtils = new HttpUtils(closeableHttpClient, requestConfig);
            ex1 = System.currentTimeMillis();
            rowId = "row1";
            str = httpUtils.get("http://localhost:8088/users/find/" + rowId,
                    new HashMap<String, String>() {{
                        put("Content-Type", "application/json");
                    }});
            xml += (System.currentTimeMillis() - ex1);

            //System.out.println("xml:" + );
            System.out.println(str);
            context2.close();
        }

        System.out.println("annotation avg time:" + (annotation / i));
        System.out.println("xml avg time:" + (xml / i));
    }
    public static void test4() {
        long annotation = 0;
        long xml = 0;
        int i;
        for (i = 0; i < 2; i++) {

    try {
        //XmlContext
        AbstractApplicationContext context2 = new ClassPathXmlApplicationContext(new String []{"springs/spring-httpclient.xml","springs/spring-httpclient.xml"});
        CloseableHttpClient closeableHttpClient = (CloseableHttpClient) context2.getBean("httpClient");
        RequestConfig requestConfig = (RequestConfig) context2.getBean("requestConfig");
        HttpUtils httpUtils = new HttpUtils(closeableHttpClient, requestConfig);
        long ex1 = System.currentTimeMillis();
        String rowId = "row1";
        HttpResult str = httpUtils.get("http://localhost:8088/users/find/" + rowId,
                new HashMap<String, String>() {{
                    put("Content-Type", "application/json");
                }});
        xml += (System.currentTimeMillis() - ex1);

        //System.out.println("xml:" + );
        System.out.println(str);
        context2.close();
    }catch (Exception e){
        e.printStackTrace();
    }


        }

        System.out.println("annotation avg time:" + (annotation / i));
        System.out.println("xml avg time:" + (xml / i));
    }
    public static void test2() {
        String la = "40.047669";
        String lo = "116.313082";
        System.out.println(getProvince(la,lo));
    }

    private static String getProvince(String la, String lo) {
        String ak = "KdRpIn0K6ahLY61zKBVOdwhaz9jkMs9N";
        String sk = "W8SZAUXFuHMXICdoCykjeaG8VP8sINzp";
        AbstractApplicationContext context = MyApplicationContext.getDefaultContext();
        HttpUtils httpUtils = (HttpUtils) context.getBean("httpUtils");
        try {
            long begin = System.currentTimeMillis();
            String snstr = "/geocoder/v2/?location=" + la + "," + lo + "&output=json&ak=" + ak + sk;
            snstr = URLEncoder.encode(snstr, "UTF-8");
            String sn = CryptogramUtil.md5Encrypt(snstr);
            if (sn == null) return null;
            String url = "http://api.map.baidu.com/geocoder/v2/?location=" + la + "," + lo + "&output=json&ak=" + ak + "&sn=" + sn;
            HttpResult httpResult = httpUtils.get(url,
                    new HashMap<String, String>() {{
                        put("Content-Type", "application/json");
                    }});
            return httpResult.getData();
           /* Document doc = Jsoup.connect().userAgent("Mozilla").ignoreContentType(true).get();
            return doc.text();*/
        } catch (UnsupportedEncodingException e) {
            e.printStackTrace();
        }
        return "";
    }
}
