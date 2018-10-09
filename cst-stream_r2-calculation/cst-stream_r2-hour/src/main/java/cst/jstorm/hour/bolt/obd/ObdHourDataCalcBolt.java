package cst.jstorm.hour.bolt.obd;

import backtype.storm.task.TopologyContext;
import backtype.storm.topology.BasicOutputCollector;
import backtype.storm.topology.OutputFieldsDeclarer;
import backtype.storm.topology.base.BaseBasicBolt;
import backtype.storm.tuple.Fields;
import backtype.storm.tuple.Tuple;
import backtype.storm.tuple.Values;
import com.cst.cmds.car.query.service.CarModelQueryService;
import com.cst.cmds.car.query.service.CarQueryService;
import com.cst.jstorm.commons.stream.custom.ComsumerContextSelect;
import com.cst.stream.common.*;
import com.cst.stream.stathour.obd.ObdHourLatestData;
import com.cst.stream.stathour.obd.ObdHourSource;
import com.cst.stream.stathour.obd.ObdHourTransfor;
import com.cst.jstorm.commons.stream.constants.OtherKey;
import com.cst.jstorm.commons.stream.constants.PropKey;
import com.cst.jstorm.commons.stream.constants.RedisKey;
import com.cst.jstorm.commons.stream.constants.StreamKey;
import com.cst.jstorm.commons.stream.custom.CustomContextConfiguration;
import com.cst.jstorm.commons.stream.custom.GasProcess;
import com.cst.jstorm.commons.stream.custom.ProvinceProcess;
import com.cst.jstorm.commons.stream.operations.GeneralDataStreamExecution;
import com.cst.jstorm.commons.stream.operations.hbasestrategy.IHBaseQueryAndPersistStrategy;
import com.cst.jstorm.commons.stream.operations.hbasestrategy.StrategyChoose;
import com.cst.jstorm.commons.utils.HttpURIUtil;
import com.cst.jstorm.commons.utils.LogbackInitUtil;
import com.cst.jstorm.commons.utils.PropertiesUtil;
import com.cst.jstorm.commons.utils.RedisUtil;
import com.cst.jstorm.commons.utils.http.HttpUtils;
import com.cst.jstorm.commons.utils.spring.MyApplicationContext;
import cst.jstorm.hour.calcalations.obd.ObdHourCalcBiz;
import org.apache.commons.collections.CollectionUtils;
import org.apache.commons.lang.StringUtils;
import org.apache.commons.lang.math.NumberUtils;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.springframework.context.support.AbstractApplicationContext;
import redis.clients.jedis.JedisCluster;

import java.util.*;

public class ObdHourDataCalcBolt extends BaseBasicBolt {
	private static final int EXTIRE_TIME = 2 * 60 * 60;
	private static final long serialVersionUID = 564071150877244021L;
	private transient Logger logger;
	private transient JedisCluster jedis;
	private AbstractApplicationContext beanContext;
    private Properties prop;
	private boolean forceLoad;
	private transient org.apache.hadoop.hbase.client.Connection connection;
	private transient HttpUtils httpUtils;
	/** 车辆油单价缓存秒数 */
	private int CAR_GAS_PRICE_EXPIRE_SECONDS;

	private ObdHourCalcBiz obdHourCalcBiz;

	public ObdHourDataCalcBolt(Properties prop, boolean forceLoad) {
		this.prop = prop;
		this.forceLoad = forceLoad;
	}

	@Override
	public void prepare(Map stormConf, TopologyContext context) {
		super.prepare(stormConf, context);
		prop = PropertiesUtil.initProp(prop, forceLoad);
		LogbackInitUtil.changeLogback(prop, true);
		logger = LoggerFactory.getLogger(ObdHourDataCalcBolt.class);
		//beanContext = MyApplicationContext.getDefaultContext();
		beanContext = ComsumerContextSelect.getDefineContextWithHttpUtilWithParam(prop.getProperty("active.env"));
		//logger.info("----------------------------------------beanContext is {}", beanContext);
		jedis = RedisUtil.buildJedisCluster(prop, RedisKey.STORM_REDISCLUSTER);

		connection = (org.apache.hadoop.hbase.client.Connection) beanContext.getBean(OtherKey.DataDealKey.HBASE_CONNECTION);
		httpUtils = (HttpUtils) beanContext.getBean(OtherKey.DataDealKey.HTTP_UTILS);
		CAR_GAS_PRICE_EXPIRE_SECONDS = NumberUtils.toInt(prop.getProperty("gas.price.expire.time"), RedisKey.ExpireTime.GAS_PRICE_TIME);
		obdHourCalcBiz = new ObdHourCalcBiz();
	}
	
	@Override
    @SuppressWarnings("unchecked")
	public void execute(Tuple input, BasicOutputCollector collector) {
		if (!StreamKey.ObdStream.OBD_HOUR_BOLT_F.equals(input.getSourceStreamId()) && !StreamKey.ElectricObdStream.ELECTRIC_OBD_HOUR_BOLT_F.equals(input.getSourceStreamId())) {
			logger.info("streamId is error,return ");
			return;
		}
		String msg = input.getString(0);
		if (StringUtils.isEmpty(msg)) {
			logger.info("msg is empty,return ");
			return;
		}

		try {
			Map<String, Object> map = new HashMap<String, Object>() {{
				put(OtherKey.DataDealKey.TIME_SELECT, CstConstants.TIME_SELECT.HOUR);
				put(OtherKey.MIDLLE_DEAL.NEXT_KEY, new LinkedList<String>());
				//put(OtherKey.MIDLLE_DEAL.NEXT_STREAM, StreamKey.ObdStream.OBD_DAY_BOLT_F);
				put(OtherKey.MIDLLE_DEAL.FMT, DateTimeUtil.DEFAULT_DATE_HOUR);
				put(OtherKey.MIDLLE_DEAL.INTERVAL, DateTimeUtil.ONE_HOUR);
				put(OtherKey.MIDLLE_DEAL.LAST_DATA_PARAM, StreamTypeDefine.OBD_TYPE);
				put(OtherKey.MIDLLE_DEAL.LAST_VALUE_EXPIRE_TIME, prop.getProperty("last.value.expire.time"));
				put(OtherKey.MIDLLE_DEAL.ZONE_VALUE_EXPIRE_TIME, prop.getProperty("hour.zone.value.expire.time"));
				put(OtherKey.MIDLLE_DEAL.BESINESS_KEY_TYPE,StreamTypeDefine.OBD_TYPE);
                put(OtherKey.MIDLLE_DEAL.REDIS_HEAD, StreamRedisConstants.HourKey.HOUR_OBD);

			}};
			GeneralDataStreamExecution<ObdHourSource, ObdHourTransfor,ObdHourLatestData, ObdHourCalcBiz> generalStreamExecution =
					new GeneralDataStreamExecution<>()
					.createJedis(jedis)
					.createSpecialCalc(obdHourCalcBiz)
					.createSpecialSource(msg, StreamRedisConstants.HourKey.HOUR_OBD, DateTimeUtil.DEFAULT_DATE_HOUR);
			// 获取车辆的油单价
			String fuelPerPrice=null;
			if (!Boolean.valueOf(prop.getProperty("ignore.outside.status"))) {
				fuelPerPrice = String.valueOf(GasProcess.getCarGasPrice(
						prop.getProperty("url_base").concat(HttpURIUtil.CAR_PRICE_URL),
						jedis, generalStreamExecution.getS().getCarId(), generalStreamExecution.getS().getTime(),
						DateTimeUtil.toLongTimeString(generalStreamExecution.getS().getTime(), DateTimeUtil.DEFAULT_DATE_DEFULT)
						,httpUtils,  CAR_GAS_PRICE_EXPIRE_SECONDS
				));

			}
			//计算补充
			map.put("fuelPrice", StringUtils.isBlank(fuelPerPrice)?prop.getProperty("default_oil_price"):fuelPerPrice);
			map.put("highSpeedStandard", prop.getProperty("highSpeedStandard"));
			map.put("night_high", prop.getProperty("night_high"));
			map.put("night_low", prop.getProperty("night_low"));
            map.put("travel_speed", prop.getProperty("travel_speed"));
			map.put("jump_mile", prop.getProperty("jump_mile"));
			map.put("tooling_probability_count", prop.getProperty("tooling_probability_count"));
			IHBaseQueryAndPersistStrategy<ObdHourSource> iFirstStrategy = StrategyChoose.generateStrategy(prop.getProperty(PropKey.DEAL_STRATEGY),
					httpUtils, prop.getProperty("url_base"), HttpURIUtil.OBD_HOUR_SOURCE_FIND,
					connection, HBaseTable.HOUR_FIRST_ZONE.getTableName(),
					HBaseTable.HOUR_FIRST_ZONE.getFirstFamilyName(), HbaseColumn.HourSourceColumn.obdHourColumns,
                    ObdHourSource.class);
            IHBaseQueryAndPersistStrategy<ObdHourTransfor> iResultStrategy = StrategyChoose.generateStrategy(prop.getProperty(PropKey.DEAL_STRATEGY),
                    httpUtils, prop.getProperty("url_base"), HttpURIUtil.OBD_HOUR_FIND,
                    connection, HBaseTable.HOUR_STATISTICS.getTableName(),
                    HBaseTable.HOUR_STATISTICS.getFirstFamilyName(), HbaseColumn.HourStatisticsCloumn.obdHourColumns,
                    ObdHourTransfor.class);

			logger.debug("deal hour data :{} ",msg);
			generalStreamExecution.dealHourData(map,iFirstStrategy, iResultStrategy);

			//hbase中拿不到该时区数据 查找最近一条上传的数据

			List<String> persistValues = (List) map.get(OtherKey.MIDLLE_DEAL.PERSIST_KEY);
			if (CollectionUtils.isNotEmpty(persistValues)) {
				for (String str : persistValues) {
					collector.emit(StreamKey.ObdStream.OBD_HOUR_BOLT_S, new Values(str, generalStreamExecution.gentMsgId()));
				}
			}
			if (map.get(OtherKey.MIDLLE_DEAL.FIRST_TIME_ZONE) != null) {
				collector.emit(StreamKey.ObdStream.OBD_HOUR_BOLT_FIRST_DATA,
						new Values(map.get(OtherKey.MIDLLE_DEAL.FIRST_TIME_ZONE),
								generalStreamExecution.gentMsgId()));

            }
		} catch (Throwable e) {
			logger.error("Process OBD hour data exception: {}", msg, e);
		}
	}

	@Override
	public void declareOutputFields(OutputFieldsDeclarer declarer) {
		declarer.declareStream(StreamKey.ObdStream.OBD_HOUR_BOLT_S, new Fields(new String[] {
				StreamKey.ObdStream.OBD_KEY_F, StreamKey.ObdStream.OBD_KEY_S }));
		declarer.declareStream(StreamKey.ObdStream.OBD_HOUR_BOLT_FIRST_DATA, new Fields(new String[] {
				StreamKey.ObdStream.OBD_KEY_F, StreamKey.ObdStream.OBD_KEY_S }));
	}


	@Override
	public void cleanup() {
		super.cleanup();
		if (beanContext != null) {
			beanContext.close();
		}
	}



}
