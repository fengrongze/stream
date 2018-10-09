package com.cst.jstorm.commons.stream.custom;

import com.cst.cmds.car.query.service.CarModelQueryService;
import com.cst.cmds.car.query.service.CarQueryService;
import com.cst.hfrq.dto.car.CarInfo;
import com.cst.hfrq.dto.car.CarModelInfo;
import com.cst.jstorm.commons.stream.constants.RedisKey;
import com.cst.jstorm.commons.stream.operations.GeneralStreamHttp;
import com.cst.jstorm.commons.utils.http.HttpUtils;
import com.cst.stream.base.BaseResult;
import com.cst.stream.common.DateTimeUtil;
import com.cst.stream.common.JsonHelper;
import com.cst.stream.common.StreamRedisConstants;
import com.cst.stream.vo.AppOilVo;
import com.fasterxml.jackson.core.type.TypeReference;
import org.apache.commons.lang.StringUtils;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import redis.clients.jedis.JedisCluster;

import java.util.HashMap;
import java.util.Map;

/**
 * @author Johnney.chiu
 * create on 2017/12/11 14:06
 * @Description 计算油价的
 */
public class GasProcess {
    private static final Logger logger= LoggerFactory.getLogger(GasProcess.class);

    public static final String NIGHTY_GAS = "90";
    public static final String NIGHTY_THREE_GAS = "93";
    public static final String NIGHTY_SEVEN_GAS = "97";
    public static final String ZERO_GAS = "0";
    private static final int GAS_PRICE_EXPIRE_TIME =2*60*60;

    //远程服务获取油标
    public static String getGasNo(CarQueryService carService, CarModelQueryService modelService, String carid){
        String gasNo=null;
        try{
            CarInfo car=carService.selectOne(carid);
            if(car!=null){
                gasNo=car.getGasNo();
                String modelid=car.getModelId();
                if((gasNo==null||gasNo.equals(""))&&(modelid!=null&&!"".equals(modelid))){
                    CarModelInfo model=modelService.selectOne(modelid);
                    if(model!=null&&model.getFuelType()!=null&&!"".equals(model.getFuelType())){
                        gasNo=model.getFuelType();
                    }
                }
            }
        }catch(Throwable e){
            logger.error("gas get exception:{}",carid,e);
        }
        return gasNo;
    }

    public static String getGasNo(CarInfo car, CarModelQueryService modelService){
        String gasNo=null;
        try{
            if(car!=null){
                gasNo=car.getGasNo();
                String modelid=car.getModelId();
                if((gasNo==null||gasNo.equals(""))&&(modelid!=null&&!"".equals(modelid))){
                    CarModelInfo model=modelService.selectOne(modelid);
                    if(model!=null&&model.getFuelType()!=null&&!"".equals(model.getFuelType())){
                        gasNo=model.getFuelType();
                    }
                }
            }
        }catch(Throwable e){
            logger.error("gas get exception:{}",car.getCarId(),e);
        }
        return gasNo;
    }

    public static String getGasNumFromRedis(JedisCluster jedis,String carId){
        if(StringUtils.isEmpty(carId))
            return null;
        String mod=carId;
        return jedis.get(StreamRedisConstants.GasKey.getGasNumKey(carId));
    }
    public static void setGasNumFromRedis(JedisCluster jedis,String carId,String gasNum,int gasNumExpireTime){
        if(StringUtils.isEmpty(carId))
            return ;
        String mod=carId;
        jedis.setex(StreamRedisConstants.GasKey.getGasNumKey(carId), gasNumExpireTime,gasNum);
    }



    public static String getGasPriceFromRedis(JedisCluster jedis,String city, String gasNo,String rdate) {
        if(StringUtils.isEmpty(city)||StringUtils.isEmpty(gasNo)||StringUtils.isEmpty(rdate))
            return null;
        return jedis.get(StreamRedisConstants.GasKey.getGasPriceKey(rdate.substring(0, 10), city, gasNo));
    }
    public static void setGasPriceToRedis(JedisCluster jedis,String city, String gasNo,String rdate,String gasPrice,
                                          int gasPriceExpireTime) {
        if(StringUtils.isEmpty(city)||StringUtils.isEmpty(gasNo)||StringUtils.isEmpty(rdate))
            return ;
        String priceKey = StreamRedisConstants.GasKey.getGasPriceKey(rdate.substring(0, 10), city, gasNo);
        jedis.setex(priceKey,gasPriceExpireTime,gasPrice);
    }

    public static double getGasPriceFromService(HttpUtils httpUtils, String moduleUrl, String city, String date, String gasNo) {
        String url = String.format(moduleUrl, city, date);
        BaseResult<AppOilVo> result = null;
        try {
            String str = GeneralStreamHttp.getGeneralData(httpUtils, url);
            if(!StringUtils.isEmpty(str))
                result = JsonHelper.toBean(str,
                        new TypeReference<BaseResult<AppOilVo>>() {
                        });
        } catch (Throwable e) {
            logger.error("get oil price error:",e);
        }
        if(result==null||!result.isSuccess()||result.getData()==null)
            return 0d;
        switch(gasNo){
            case ZERO_GAS:
                return result.getData().getB0();
            case NIGHTY_GAS:
                return result.getData().getB90();
            case NIGHTY_SEVEN_GAS:
                return result.getData().getB97();
            case NIGHTY_THREE_GAS:
                return result.getData().getB93();
            default:
                return 0d;
        }
    }

    public static String judgeGas(String gasNum){

        if(StringUtils.isBlank(gasNum))
            return NIGHTY_THREE_GAS;
        else if (gasNum.contains(NIGHTY_GAS))
            return NIGHTY_GAS;
        else if (gasNum.contains(NIGHTY_THREE_GAS))
            return NIGHTY_THREE_GAS;
        else if(gasNum.contains(NIGHTY_SEVEN_GAS)) return NIGHTY_SEVEN_GAS;
        else return ZERO_GAS;
    }


    //一般性获得油价
    public static double getGasPriceWithDefaultFlow( String moduleUrl,String city,
                                                    JedisCluster jedis, String carId, long upTime, String rdate,
                                                     CarQueryService carService, CarModelQueryService modelService,
                                                    HttpUtils httpUtils,int gasNumExpireTime,int gasPriceExpireTime){

        //从缓存中获取油标
        String gasNum = getGasNumFromRedis(jedis, carId);
        if(StringUtils.isEmpty(gasNum)){
            gasNum = getGasNo(carService, modelService, carId);
            if (!StringUtils.isEmpty(gasNum))
                setGasNumFromRedis(jedis,carId,gasNum,gasNumExpireTime);
        }
        String standardGas = judgeGas(gasNum);
        String gasPriceStr=getGasPriceFromRedis(jedis, city, standardGas, rdate);

        //从缓存中获取油价
        if(StringUtils.isEmpty(gasPriceStr)){
            String realUrl=String.format(moduleUrl,
                    city, DateTimeUtil.toLongTimeString(upTime,DateTimeUtil.DEFAULT_SHORT_DATE_DEFULT));

            double gasPrice = getGasPriceFromService(httpUtils, realUrl,city,rdate,standardGas);
            if(gasPrice>0)
                setGasPriceToRedis(jedis,city,standardGas,rdate,String.valueOf(gasPrice),gasPriceExpireTime);
            return gasPrice;
        }
        return Double.parseDouble(gasPriceStr);
    }


    public static double getCarGasPrice(String moduleUrl, JedisCluster jedis, String carId, long upTime, String rdate,
                                        HttpUtils httpUtils,int gasPriceExpireTime) {


        String city = ProvinceProcess.getCityFromRedis(jedis, carId);
        if (StringUtils.isBlank(city)) {
            city = "重庆";
        }
        // 从缓存中获取油标
        String gasNum = getGasNumFromRedis(jedis, carId);
        String standardGas = judgeGas(gasNum);
        String gasPriceStr=getGasPriceFromRedis(jedis, city, standardGas, rdate);

        // 从缓存中获取油价
        if(StringUtils.isEmpty(gasPriceStr)){
            String realUrl=String.format(moduleUrl, city, DateTimeUtil.toLongTimeString(upTime,DateTimeUtil.DEFAULT_SHORT_DATE_DEFULT));
            double gasPrice = getGasPriceFromService(httpUtils, realUrl,city,rdate,standardGas);
            if (gasPrice > 0) {
                setGasPriceToRedis(jedis,city,standardGas,rdate,String.valueOf(gasPrice),gasPriceExpireTime);
            }
            return gasPrice;
        }
        return Double.parseDouble(gasPriceStr);
    }


    public static Map<String,String> gasDataFromCmds(String carId, CarQueryService carService,
                                                     CarModelQueryService modelService){
        Map<String, String> map = new HashMap<String,String>() {{
            put("city", null);
            put("gasNum", null);
        }};
        CarInfo car = null;
        String city=null;
        long start = System.currentTimeMillis();
        try {
            car = carService.selectOne(carId);
            if (car == null) {
                logger.warn("Found invalid car: carId={}", carId);
            }
            if(car != null && StringUtils.isNotBlank(car.getLisence())){
                city = ProvinceProcess.PROVINCE_SIMPLE_MAPPING.get(car.getLisence().substring(0, 1));
            }
        } catch (Exception e) {
            logger.error("Query car info error: carId={}", carId, e);
        } finally {
            long cost = System.currentTimeMillis() - start;
            if (cost > 999) {
                logger.info("Query car info: carId={}, time cost {}ms", carId, cost);
            }
        }
        map.put("city", city);
        String gasNum;
        if (car == null) {
            gasNum = getGasNo(carService, modelService, carId);
        } else {
            gasNum = getGasNo(car, modelService);
        }

       /* if (StringUtils.isNotBlank(gasNum)) {
            setGasNumFromRedis(jedis,carId,gasNum,gasNumExpireTime);
        }*/
        map.put("gasNum", gasNum);
        return map;
    }

}
