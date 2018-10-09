package com.cst.jstorm.commons.stream.custom;

import com.cst.cmds.car.query.service.CarModelQueryService;
import com.cst.cmds.car.query.service.CarQueryService;
import com.cst.hfrq.dto.car.CarInfo;
import com.cst.jstorm.commons.stream.constants.PropKey;
import com.cst.jstorm.commons.utils.http.HttpUtils;
import com.cst.stream.common.StreamRedisConstants;
import com.google.gson.JsonObject;
import com.google.gson.JsonParser;
import org.apache.commons.lang.StringUtils;
import org.apache.http.client.config.RequestConfig;
import org.apache.http.impl.client.CloseableHttpClient;
import org.apache.http.impl.client.HttpClients;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import redis.clients.jedis.JedisCluster;

import java.util.HashMap;
import java.util.Map;

import static com.cst.jstorm.commons.stream.custom.GasProcess.gasDataFromCmds;
import static com.cst.jstorm.commons.stream.custom.GasProcess.setGasNumFromRedis;

/**
 * @author Johnney.chiu
 * create on 2017/12/8 17:08
 * @Description 车的地区处理
 */
public class ProvinceProcess {

    private static final Logger logger= LoggerFactory.getLogger(ProvinceProcess.class);

    public static final int CITY_EXPIRE_TIME = 5  * 60;

    public static final Map<String, String> PROVINCE_SIMPLE_MAPPING;

    static {
        PROVINCE_SIMPLE_MAPPING = ProvinceProcess.initProvince(PropKey.STR_PROVINCES);
    }


    //get province from jedis
    public static String getCityFromRedis(JedisCluster jedis,String carId){
        if(StringUtils.isEmpty(carId))
            return null;
        if(jedis==null)
            return null;
        return jedis.get(getPrivinceKey(carId));

    }

    //set province to jedis
    public static String setCityToJedis(JedisCluster jedis,String carId, String city, int expireSeconds){
        if(StringUtils.isEmpty(carId)||StringUtils.isEmpty(city))
            return null;
        return jedis.setex(getPrivinceKey(carId), expireSeconds, city);
    }

    public static String getPrivinceKey(String carId){
        return StreamRedisConstants.GasKey.getCarCityKey(carId);
    }

    public static void getCityFromLalo(JedisCluster jedis, String carId, int expireSeconds,
                                       HttpUtils httpUtils, Double la, Double lo, String ak,
                                       String sk,CarQueryService carService,
                                       CarModelQueryService modelService,int gasNumExpireTime){
        String gasNum=null;
        String city = getCityFromLalo(httpUtils, String.valueOf(la), String.valueOf(lo), ak, sk);
        Map<String, String> cmdsData;
        if (StringUtils.isNotBlank(city)) {
            setCityToJedis(jedis, carId, city, expireSeconds);
        }else{
            cmdsData = gasDataFromCmds(carId, carService,
                    modelService);
            if (null!=cmdsData) {
                gasNum = cmdsData.get("gasNum");
                city=cmdsData.get("city");
                if (StringUtils.isNotBlank(gasNum)) {
                    setGasNumFromRedis(jedis, carId, gasNum, gasNumExpireTime);
                }
                if(StringUtils.isNotBlank(city)){
                    setCityToJedis(jedis, carId, city, expireSeconds);
                }

            }
        }
        //logger.info("get city is {},get gas no is {}", city, gasNum);
    }

    //通过百度api la lo获得city
    @SuppressWarnings("unchecked")
    public static String getCityFromLalo(HttpUtils httpUtils,String la, String lo, String ak, String sk) {
        if(StringUtils.isEmpty(la)||StringUtils.isEmpty(lo))
            return null;
        long start = System.currentTimeMillis();
        String result = BaiduApiAccess.getProvinceWithBaiduApiSN(httpUtils, BaiduApiAccess.generateSN(la, lo, ak, sk), ak, la, lo);
        long cost = System.currentTimeMillis() - start;
        if (cost > 999) {
            logger.info("Request Baidu location api time cost: {} ms", cost);
        }

        //logger.info("get data {},{}, from baidu {},{} api :{}",la,lo,ak,sk,result);
        if(StringUtils.isEmpty(result))
            return null;
        Map<String,String> map= null;
        try {
            //注意

            JsonObject jsonObject = (JsonObject) new JsonParser().parse(result);
            if(jsonObject.get("result")==null||jsonObject.get("result").getAsJsonObject().get("addressComponent")==null)
                return null;
            String city=jsonObject.get("result").getAsJsonObject().get("addressComponent").getAsJsonObject().get("city").getAsString();
            String province=jsonObject.get("result").getAsJsonObject().get("addressComponent").getAsJsonObject().get("province").getAsString();
            if(StringUtils.isBlank(city)||StringUtils.isBlank(province)) return null;
            //由于这里返回的是标准的名称，比如，重庆市，山东省，河北省，所以要去掉最后一个字
            if(province.endsWith("市")||province.endsWith("省")){
                province=province.substring(0, province.length()-1);
            }else if(province.endsWith("自治区")||province.endsWith("特别行政区")){//对于特别的只取前两个值
                province=province.substring(0,2);
                //内蒙古自治区简写后为内蒙古
                if(province.equals("内蒙")) province+="古";
            }

            return province;
        } catch (Exception e) {
            e.printStackTrace();
        }
        return null;
    }

    //通过cmds获取车牌，通过车牌获取所在地

    public static String getCityFromCMDS(Map<String,String> map_province, CarQueryService carService, String carid) {
        if(StringUtils.isEmpty(carid))
            return null;
        long start = System.currentTimeMillis();
        try {
            CarInfo car = carService.selectOne(carid);
            if(car!=null&&car.getLisence()!=null&&!car.getLisence().equals("")){
                return map_province.get(car.getLisence().substring(0, 1));
            }
        } catch (Exception e) {
			logger.error("Query car info error: carId={}", carid, e);
        } finally {
			long cost = System.currentTimeMillis() - start;
			if (cost > 999) {
				logger.info("Query car info: carId={}, time cost {}ms", carid, cost);
			}
        }
        return null;
    }


    public static Map<String,String> initProvince( String propKey){
        Map<String, String> map = new HashMap<>();
        String[] provinces;
        provinces = propKey.split(",");
        for(String pro : provinces){
            if(pro==null||pro.length()==0) continue;
            String[] tmp=pro.split(" ");
            map.put(tmp[0], tmp[1]);
        }
        return map;
    }
    //一般性获取城市省份的计算
    public static String getCityWithDefaultFlow(Map<String,String> map_province, JedisCluster jedis, CarQueryService carService, String carId, int expireSeconds){
        String tmp = getCityFromRedis(jedis,carId);
        //通过la,lo获取车辆所在地，交给gpsbolt计算 并将数据放入缓存中
        if(tmp == null) {
            tmp = getCityFromCMDS(map_province, carService, carId);
        }
        // 默认值
        if(tmp == null) {
            tmp="重庆";
        }
        setCityToJedis(jedis,carId, tmp, expireSeconds);
        return tmp;
    }


    public static  void main(String ...args){
        CloseableHttpClient client = HttpClients.createDefault();
        RequestConfig requestConfig = RequestConfig.custom()
                .setConnectionRequestTimeout(3000)
                .setConnectTimeout(3000)
                .setSocketTimeout(3000).build();

        HttpUtils httpUtils = new HttpUtils(client, requestConfig);
        String city = getCityFromLalo(httpUtils, "30.554858", "106.484071", "KdRpIn0K6ahLY61zKBVOdwhaz9jkMs9N", "W8SZAUXFuHMXICdoCykjeaG8VP8sINzp");

        System.out.println(city);
    }



}
