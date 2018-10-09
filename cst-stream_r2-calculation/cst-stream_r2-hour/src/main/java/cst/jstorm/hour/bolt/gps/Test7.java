package cst.jstorm.hour.bolt.gps;

import com.alibaba.fastjson.JSON;
import com.alibaba.fastjson.TypeReference;
import com.cst.stream.stathour.CSTData;
import com.cst.stream.stathour.gps.GpsHourLatestData;

import com.google.gson.Gson;
import com.google.gson.reflect.TypeToken;
import lombok.ToString;

import java.util.ArrayList;
import java.util.List;

/**
 * @author Johnney.Chiu
 * create on 2018/7/27 16:16
 * @Description
 * @title
 */
public class Test7 {
    public static void main(String... args){
       /* GpsHourLatestData gpsHourLatestData = GpsHourLatestData.builder()
                .carId("xiaoq")
                .isNonLocal(0)
                .maxSatelliteNum(100)
                .gpsCount(100)
                .time(System.currentTimeMillis())
                .build();

        GpsHourLatestData gpsHourLatestData2 = GpsHourLatestData.builder()
                .carId("xiaoq")
                .isNonLocal(0)
                .maxSatelliteNum(130)
                .gpsCount(100)
                .time(System.currentTimeMillis())
                .build();
        List<GpsHourLatestData> list = new ArrayList<GpsHourLatestData>() {{
            add(gpsHourLatestData2);
            add(gpsHourLatestData);
        }};
        //System.out.println(JsonHelper.toStringWithoutException(list));
        System.out.println(JSON.toJSONString(list));

        String str = "{\"carId\":\"xiaoq\",\"time\":1532679536651,\"mixSatelliteNum\":200,\"maxSatelliteNum\":100,\"gpsCount\":100,\"isNonLocal\":0}";
        GpsHourLatestData gpsHourLatestData1 = JSON.parseObject(str, new TypeReference<GpsHourLatestData>() {
        });
        System.out.println(gpsHourLatestData1);
        //GpsHourLatestData gpsHourLatestData1 = JsonHelper.toBeanWithoutException(str, GpsHourLatestData.class);
        //System.out.println(gpsHourLatestData1);

        System.out.println(JSON.parseArray("[{\"carId\":\"xiaoq\",\"gpsCount\":100,\"isNonLocal\":0,\"maxSatelliteNum\":130,\"time\":1532680507230,\"dinChange\":1},{\"carId\":\"xiaoq\",\"gpsCount\":100,\"isNonLocal\":0,\"maxSatelliteNum\":100,\"time\":1532680507230}]", GpsHourLatestData.class));;


        */

        String str = "{\"carId\":\"xiaoq\",\"time\":1532679536651,\"mixSatelliteNum\":200,\"maxSatelliteNum\":100,\"gpsCount\":100,\"isNonLocal\":0}";
        GpsHourLatestData gpsHourLatestData1 = JSON.parseObject(str, new TypeReference<GpsHourLatestData>() {
        });

        System.out.println(gpsHourLatestData1);

        User<GpsHourLatestData> user = new User<>(str,GpsHourLatestData.class);
        System.out.println(user);

        Student<GpsHourLatestData> student = new Student<>(str);
        System.out.println(student);

    }

}

@ToString
class User<T extends CSTData>{
    private T t;
    public T getT() {
        return t;
    }
    public void setT(T t) {
        this.t = t;
    }

    public User(String str,Class<T> clazz){
        t = JSON.parseObject(str, clazz);
    }
}

@ToString
class Student<T extends CSTData>{
    private T t;
    public T getT() {
        return t;
    }
    public void setT(T t) {
        this.t = t;
    }

    public Student(String str){
        t = new Gson().fromJson(str, new TypeToken<T>() {
        }.getType());
    }
}