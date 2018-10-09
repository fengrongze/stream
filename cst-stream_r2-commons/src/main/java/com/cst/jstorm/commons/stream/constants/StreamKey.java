package com.cst.jstorm.commons.stream.constants;

public class StreamKey {

	public static class GpsStream{
		/**
		 * gps hour bolt first
		 */
		public static final String GPS_HOUR_BOLT_F = "gpsHourBolt";

		/**
		 * gps hour bolt first
		 */
		public static final String GPS_GDCP3_HOUR_BOLT_F = "gpsGdcp3HourBolt";

		/**
		 * gps hour bolt second
		 */
		public static final String GPS_HOUR_BOLT_S = "gpsPersistHourBolt";


        /**
         * gps hour no delay bolt second
         */
        public static final String GPS_HOUR_BOLT_NO_DELAY = "gpsPersistHourBoltNoDelay";

		/**
		 * gps hour bolt first data persist
		 */
		public static final String GPS_HOUR_BOLT_FIRST_DATA = "gpsHourBoltPersistFirstData";
		/**
		 * gps day bolt first data persist
		 */
		public static final String GPS_DAY_BOLT_FIRST_DATA = "gpsDayBoltPersistFirstData";
        /**
		 * gps day bolt first
		 */
		public static final String GPS_DAY_BOLT_F = "gpsDayBolt";
		/**
		 * gps day bolt second
		 */
		public static final String GPS_DAY_BOLT_S = "gpsPersistDayBolt";

		/**
		 * gps Schedule day bolt second
		 */
		public static final String GPS_SCHEDULE_DAY_BOLT_S = "gpsSchedulePersistDayBolt";

		/**
		 * gps Dormancy day bolt second
		 */
		public static final String GPS_DORMANCY_DAY_BOLT_S = "gpsDormancyPersistDayBolt";

		/**
		 * gps month bolt first
		 */
		public static final String GPS_MONTH_BOLT_F = "gpsMonthBolt";
		/**
		 * gps month bolt second
		 */
		public static final String GPS_MONTH_BOLT_S = "gpsPersistMonthBolt";

		/**
		 * gps year bolt first
		 */
		public static final String GPS_YEAR_BOLT_F = "gpsYearBolt";
		/**
		 * gps year bolt second
		 */
		public static final String GPS_YEAR_BOLT_S = "gpsPersistYearBolt";


        /**
         * gps day no delay bolt second
         */
        public static final String GPS_DAY_BOLT_NO_DELAY = "gpsPersistDayBoltNoDelay";

        /**
		 * gps fileds key1
		 */
		public static final String GPS_KEY_F = "m";

		/**
		 * gps fileds key2
		 */
		public static final String GPS_KEY_S = "s";

	}

    public static class ObdStream {

        /**
         * obd hour bolt first
         */
        public static final String OBD_HOUR_BOLT_F = "obdHourBolt";
		/**
		 * obd hour bolt first
		 */
		public static final String OBD_GDCP3_HOUR_BOLT_F = "obdGdcp3HourBolt";
		/**
		 * obd hour bolt persist
		 */
		public static final String OBD_HOUR_BOLT_S = "obdHourBoltPersist";

        /**
         * obd hour no delay bolt persist
         */
        public static final String OBD_HOUR_BOLT_NO_DELAY = "obdHourBoltPersistNoDelay";

		/**
		 * obd hour bolt first data persist
		 */
		public static final String OBD_HOUR_BOLT_FIRST_DATA = "obdHourBoltPersistFirstData";
		/**
		 * obd day bolt first data persist
		 */
		public static final String OBD_DAY_BOLT_FIRST_DATA = "obdDayBoltPersistFirstData";
        /**
         * obd day bolt
         */
        public static final String OBD_DAY_BOLT_F = "obdDayBolt";
		/**
		 * obd day bolt
		 */
		public static final String OBD_DAY_BOLT_S = "obdDayBoltPersist";
		/**
		 * obd schedule day bolt
		 */
		public static final String OBD_SCHEDULE_DAY_BOLT_S = "obdScheduleDayBoltPersist";

		/**
		 * obd Dormancy day bolt second
		 */
		public static final String OBD_DORMANCY_DAY_BOLT_S = "obdDormancyPersistDayBolt";
		/**
		 * obd day bolt
		 */
		public static final String OBD_DAY_PERSIST_S = "obdDayBoltIntegrity";

		/**
		 * am day bolt
		 */
		public static final String AM_DAY_PERSIST_S = "amDayBoltIntegrity";

		/**
		 * de day bolt
		 */
		public static final String DE_DAY_PERSIST_S = "deDayBoltIntegrity";

		/**
		 * obd hour bolt
		 */
		public static final String OBD_HOUR_PERSIST_S = "obdHourBoltIntegrity";

		/**
		 * de hour bolt
		 */
		public static final String DE_HOUR_PERSIST_S = "deHourBoltIntegrity";

		/**
		 * obd month bolt
		 */
		public static final String OBD_MONTH_BOLT_F = "obdMonthBolt";
		/**
		 * obd month bolt
		 */
		public static final String OBD_MONTH_BOLT_S = "obdMonthBoltPersist";

		/**
		 * obd year bolt
		 */
		public static final String OBD_YEAR_BOLT_F = "obdYearBolt";
		/**
		 * obd year bolt
		 */
		public static final String OBD_YEAR_BOLT_S = "obdYearBoltPersist";

        /**
         * obd day no delay bolt persist
         */
        public static final String OBD_DAY_BOLT_NO_DELAY = "obdDayBoltPersistNoDelay";


        /**
        * obd fileds key1
         */
        public static final String OBD_KEY_F = "v";

        /**
         * obd fileds key2
         */
        public static final String OBD_KEY_S = "k";

		/**
		 * obd fileds key3
		 */
		public static final String OBD_KEY_T = "t";
    }

	public static class MileageStream {

		/**
		 * mileage hour bolt first
		 */
		public static final String MILEAGE_HOUR_BOLT_F = "mileageHourBolt";
		/**
		 * mileage hour bolt first
		 */
		public static final String MILEAGE_GDCP3_HOUR_BOLT_F = "mileageGdcp3HourBolt";
		/**
		 * mileage hour bolt persist
		 */
		public static final String MILEAGE_HOUR_BOLT_S = "mileageHourBoltPersist";

		/**
		 * mileage hour no delay bolt persist
		 */
		public static final String MILEAGE_HOUR_BOLT_NO_DELAY = "mileageHourBoltPersistNoDelay";

		/**
		 * mileage hour bolt first data persist
		 */
		public static final String MILEAGE_HOUR_BOLT_FIRST_DATA = "mileageHourBoltPersistFirstData";
		/**
		 * mileage day bolt first data persist
		 */
		public static final String MILEAGE_DAY_BOLT_FIRST_DATA = "mileageDayBoltPersistFirstData";
		/**
		 * mileage day bolt
		 */
		public static final String MILEAGE_DAY_BOLT_F = "mileageDayBolt";
		/**
		 * mileage day bolt
		 */
		public static final String MILEAGE_DAY_BOLT_S = "mileageDayBoltPersist";
		/**
		 * mileage schedule day bolt
		 */
		public static final String MILEAGE_SCHEDULE_DAY_BOLT_S = "mileageScheduleDayBoltPersist";

		/**
		 * mileage Dormancy day bolt second
		 */
		public static final String MILEAGE_DORMANCY_DAY_BOLT_S = "mileageDormancyPersistDayBolt";
		/**
		 * mileage day bolt
		 */
		public static final String MILEAGE_DAY_PERSIST_S = "mileageDayBoltIntegrity";

		/**
		 * mileage hour bolt
		 */
		public static final String MILEAGE_HOUR_PERSIST_S = "mileageHourBoltIntegrity";


		/**
		 * mileage month bolt
		 */
		public static final String MILEAGE_MONTH_BOLT_F = "mileageMonthBolt";
		/**
		 * mileage month bolt
		 */
		public static final String MILEAGE_MONTH_BOLT_S = "mileageMonthBoltPersist";

		/**
		 * mileage year bolt
		 */
		public static final String MILEAGE_YEAR_BOLT_F = "mileageYearBolt";
		/**
		 * mileage year bolt
		 */
		public static final String MILEAGE_YEAR_BOLT_S = "mileageYearBoltPersist";

		/**
		 * mileage day no delay bolt persist
		 */
		public static final String MILEAGE_DAY_BOLT_NO_DELAY = "mileageDayBoltPersistNoDelay";


		/**
		 * mileage fileds key1
		 */
		public static final String MILEAGE_KEY_F = "v";

		/**
		 * mileage fileds key2
		 */
		public static final String MILEAGE_KEY_S = "k";

		/**
		 * mileage fileds key3
		 */
		public static final String MILEAGE_KEY_T = "t";
	}

	public static class ElectricObdStream {

		/**
		 * electric obd hour bolt first
		 */
		public static final String ELECTRIC_OBD_HOUR_BOLT_F = "ElectricObdHourBolt";


		/**
		 * electric obd hour bolt first
		 */
		public static final String ELECTRIC_GDCP3_OBD_HOUR_BOLT_F = "ElectricGdcp3ObdHourBolt";

		/**
		 * electric obd fileds key1
		 */
		public static final String ELECTRIC_OBD_KEY_F = "v";

		/**
		 * electric obd fileds key2
		 */
		public static final String ELECTRIC_OBD_KEY_S = "k";

		/**
		 * electric obd fileds key3
		 */
		public static final String OBD_KEY_T = "t";
	}
	public static class AmStream{
        /**
         * am hour bolt
         */
        public static final String AM_HOUR_BOLT_F = "amHourBolt";

		/**
		 * am hour bolt
		 */
		public static final String AM_GDCP3_HOUR_BOLT_F = "amGdcp3HourBolt";
        /**
		* am hour bolt persist
         */
		public static final String AM_HOUR_BOLT_S = "amHourBoltPersist";

		/**
		 * am hour bolt no delay persist
		 */
		public static final String AM_HOUR_BOLT_NO_DELAY = "amHourBoltPersistNoDelay";
		/**
		 * am hour bolt first data persist
		 */
		public static final String AM_HOUR_BOLT_FIRST_DATA = "amHourBoltPersistFirstData";
		/**
		 * am day bolt first data persist
		 */
		public static final String AM_DAY_BOLT_FIRST_DATA = "amDayBoltPersistFirstData";
		/**
         * am day bolt
         */
        public static final String AM_DAY_BOLT_F = "amDayBolt";

		/**
		 * am day bolt
		 */
		public static final String AM_DAY_BOLT_S = "amDayBoltPersist";

		/**
		 * am schedule day bolt
		 */
		public static final String AM_SCHEDULE_DAY_BOLT_S = "amScheduleDayBoltPersist";
		/**
		 * am Dormancy day bolt second
		 */
		public static final String AM_DORMANCY_DAY_BOLT_S = "amDormancyPersistDayBolt";
		/**
		 * am month bolt
		 */
		public static final String AM_MONTH_BOLT_F = "amMonthBolt";

		/**
		 * am month bolt
		 */
		public static final String AM_MONTH_BOLT_S = "amMonthBoltPersist";

		/**
		 * am year bolt
		 */
		public static final String AM_YEAR_BOLT_F = "amYearBolt";

		/**
		 * am year bolt
		 */
		public static final String AM_YEAR_BOLT_S = "amYearBoltPersist";


		/**
		 * am day bolt no delay persist
		 */
		public static final String AM_DAY_BOLT_NO_DELAY = "amDayBoltPersistNoDelay";


		/**
         * am fileds key1
         */
        public static final String AM_KEY_F = "m";

        /**
         * am fileds key2
         */
        public static final String AM_KEY_S = "s";
    }


    public static class DeStream{
        /**
         * de hour bolt
         */
        public static final String DE_HOUR_BOLT_F = "deHourBolt";

		/**
		 * de hour bolt
		 */
		public static final String DE_GDCP3_HOUR_BOLT_F = "deGdcp3HourBolt";
        /**
         * de hour bolt
         */
        public static final String DE_HOUR_BOLT_S = "deHourBoltPersist";

        /**
         * de hour no delay bolt
         */
        public static final String DE_HOUR_BOLT_NO_DELAY = "deHourBoltPersistNoDelay";

		/**
		 * de hour bolt first data persist
		 */
		public static final String DE_HOUR_BOLT_FIRST_DATA = "deHourBoltPersistFirstData";

		/**
		 * de day bolt first data persist
		 */
		public static final String DE_DAY_BOLT_FIRST_DATA = "deDayBoltPersistFirstData";
        /**
         * de day bolt
         */
        public static final String DE_DAY_BOLT_F = "deDayBolt";

		/**
		 * de day bolt
		 */
		public static final String DE_DAY_BOLT_S = "deDayBoltPersist";

		/**
		 * de schedule day bolt
		 */
		public static final String DE_SCHEDULE_DAY_BOLT_S = "deScheduleDayBoltPersist";

		/**
		 * de Dormancy day bolt second
		 */
		public static final String DE_DORMANCY_DAY_BOLT_S = "deDormancyPersistDayBolt";
		/**
		 * de year bolt
		 */
		public static final String DE_YEAR_BOLT_F = "deYearBolt";

		/**
		 * de year bolt
		 */
		public static final String DE_YEAR_BOLT_S = "deYearBoltPersist";

		/**
		 * de month bolt
		 */
		public static final String DE_MONTH_BOLT_F = "deMonthBolt";

		/**
		 * de month bolt
		 */
		public static final String DE_MONTH_BOLT_S = "deMonthBoltPersist";

        /**
         * de day no delay bolt
         */
        public static final String DE_DAY_BOLT_NO_DELAY = "deDayBoltPersistNoDelay";


		/**
         * de fileds key1
         */
        public static final String DE_KEY_F = "m";

        /**
         * de fileds key2
         */
        public static final String DE_KEY_S = "s";
    }

	public static class TraceStream{
		/**
		 * trace hour bolt
		 */
		public static final String TRACE_HOUR_BOLT_F = "traceHourBolt";
		/**
		 * trace hour bolt
		 */
		public static final String TRACE_GDCP3_HOUR_BOLT_F = "traceGdcp3HourBolt";

		/**
		 * trace hour bolt
		 */
		public static final String TRACE_HOUR_BOLT_S = "traceHourBoltPersist";

		/**
		 * trace hour no delay bolt
		 */
		public static final String TRACE_HOUR_BOLT_NO_DELAY = "traceHourBoltPersistNoDelay";

		/**
		 * trace hour bolt first data persist
		 */
		public static final String TRACE_HOUR_BOLT_FIRST_DATA = "traceHourBoltPersistFirstData";

		/**
		 * trace day bolt first data persist
		 */
		public static final String TRACE_DAY_BOLT_FIRST_DATA = "traceDayBoltPersistFirstData";
		/**
		 * trace day bolt
		 */
		public static final String TRACE_DAY_BOLT_F = "traceDayBolt";

		/**
		 * trace day bolt
		 */
		public static final String TRACE_DAY_BOLT_S = "traceDayBoltPersist";

		/**
		 * trace schedule day bolt
		 */
		public static final String TRACE_SCHEDULE_DAY_BOLT_S = "traceScheduleDayBoltPersist";

		/**
		 * trace Dormancy day bolt second
		 */
		public static final String TRACE_DORMANCY_DAY_BOLT_S = "traceDormancyPersistDayBolt";
		/**
		 * trace month bolt
		 */
		public static final String TRACE_MONTH_BOLT_F = "traceMonthBolt";

		/**
		 * trace month bolt
		 */
		public static final String TRACE_MONTH_BOLT_S = "traceMonthBoltPersist";

		/**
		 * trace year bolt
		 */
		public static final String TRACE_YEAR_BOLT_F = "traceYearBolt";

		/**
		 * trace year bolt
		 */
		public static final String TRACE_YEAR_BOLT_S = "traceYearBoltPersist";

		/**
		 * trace day no delay bolt
		 */
		public static final String TRACE_DAY_BOLT_NO_DELAY = "traceDayBoltPersistNoDelay";


		/**
		 * trace fileds key1
		 */
		public static final String TRACE_KEY_F = "m";

		/**
		 * trace fileds key2
		 */
		public static final String TRACE_KEY_S = "s";
	}


	public static class VoltageStream{
		/**
		 * voltage hour bolt
		 */
		public static final String VOLTAGE_HOUR_BOLT_F = "voltageHourBolt";
		/**
		 * voltage hour bolt
		 */
		public static final String VOLTAGE_GDCP3_HOUR_BOLT_F = "voltageGdcp3HourBolt";

		/**
		 * voltage hour bolt
		 */
		public static final String VOLTAGE_HOUR_BOLT_S = "voltageHourBoltPersist";

		/**
		 * voltage hour no delay bolt
		 */
		public static final String VOLTAGE_HOUR_BOLT_NO_DELAY = "voltageHourBoltPersistNoDelay";

		/**
		 * voltage hour bolt first data persist
		 */
		public static final String VOLTAGE_HOUR_BOLT_FIRST_DATA = "voltageHourBoltPersistFirstData";

		/**
		 * voltage day bolt first data persist
		 */
		public static final String VOLTAGE_DAY_BOLT_FIRST_DATA = "voltageDayBoltPersistFirstData";

		/**
		 * voltage day bolt
		 */
		public static final String VOLTAGE_DAY_BOLT_F = "voltageDayBolt";

		/**
		 * voltage day bolt
		 */
		public static final String VOLTAGE_DAY_BOLT_S = "voltageDayBoltPersist";

		/**
		 * voltage schedule day bolt
		 */
		public static final String VOLTAGE_SCHEDULE_DAY_BOLT_S = "voltageScheduleDayBoltPersist";
		/**
		 * voltage Dormancy day bolt second
		 */
		public static final String VOLTAGE_DORMANCY_DAY_BOLT_S = "voltageDormancyPersistDayBolt";
		/**
		 * voltage month bolt
		 */
		public static final String VOLTAGE_MONTH_BOLT_F = "voltageMonthBolt";

		/**
		 * voltage month bolt
		 */
		public static final String VOLTAGE_MONTH_BOLT_S = "voltageMonthBoltPersist";

		/**
		 * voltage year bolt
		 */
		public static final String VOLTAGE_YEAR_BOLT_F = "voltageYearBolt";

		/**
		 * voltage year bolt
		 */
		public static final String VOLTAGE_YEAR_BOLT_S = "voltageYearBoltPersist";

		/**
		 * voltage day no delay bolt
		 */
		public static final String VOLTAGE_DAY_BOLT_NO_DELAY = "voltageDayBoltPersistNoDelay";


		/**
		 * voltage fileds key1
		 */
		public static final String VOLTAGE_KEY_F = "m";

		/**
		 * voltage fileds key2
		 */
		public static final String VOLTAGE_KEY_S = "s";
	}

	public static class TraceDeleteStream{
		/**
		 * trace delete hour bolt
		 */
		public static final String TRACE_DELETE_HOUR_BOLT_F = "traceDeleteHourBolt";
		/**
		 * trace delete hour bolt
		 */
		public static final String TRACE_DELETE_GDCP3_HOUR_BOLT_F = "traceDeleteGdcp3HourBolt";

		/**
		 * trace delete hour bolt
		 */
		public static final String TRACE_DELETE_HOUR_BOLT_S = "traceDeleteHourBoltPersist";

		/**
		 * trace delete hour no delay bolt
		 */
		public static final String TRACE_DELETE_HOUR_BOLT_NO_DELAY = "traceDeleteHourBoltPersistNoDelay";

		/**
		 * trace delete hour bolt first data persist
		 */
		public static final String TRACE_DELETE_HOUR_BOLT_FIRST_DATA = "traceDeleteHourBoltPersistFirstData";

		/**
		 * trace delete day bolt first data persist
		 */
		public static final String TRACE_DELETE_DAY_BOLT_FIRST_DATA = "traceDeleteDayBoltPersistFirstData";

		/**
		 * trace delete day bolt
		 */
		public static final String TRACE_DELETE_DAY_BOLT_F = "traceDeleteDayBolt";

		/**
		 * trace delete day bolt
		 */
		public static final String TRACE_DELETE_DAY_BOLT_S = "traceDeleteDayBoltPersist";

		/**
		 * trace delete schedule day bolt
		 */
		public static final String TRACE_DELETE_SCHEDULE_DAY_BOLT_S = "traceDeleteScheduleDayBoltPersist";
		/**
		 * trace delete Dormancy day bolt second
		 */
		public static final String TRACE_DELETE_DORMANCY_DAY_BOLT_S = "traceDeleteDormancyPersistDayBolt";
		/**
		 * trace delete month bolt
		 */
		public static final String TRACE_DELETE_MONTH_BOLT_F = "traceDeleteMonthBolt";

		/**
		 * trace delete month bolt
		 */
		public static final String TRACE_DELETE_MONTH_BOLT_S = "traceDeleteMonthBoltPersist";

		/**
		 * trace delete year bolt
		 */
		public static final String TRACE_DELETE_YEAR_BOLT_F = "traceDeleteYearBolt";

		/**
		 * trace delete year bolt
		 */
		public static final String TRACE_DELETE_YEAR_BOLT_S = "traceDeleteYearBoltPersist";


		/**
		 * trace delete day no delay bolt
		 */
		public static final String TRACE_DELETE_DAY_BOLT_NO_DELAY = "traceDeleteDayBoltPersistNoDelay";


		/**
		 * trace delete fileds key1
		 */
		public static final String TRACE_DELETE_KEY_F = "m";

		/**
		 * trace delete fileds key2
		 */
		public static final String TRACE_DELETE_KEY_S = "s";
	}

	/**
	 * other day bolt
	 */
	public static final String OTHER_DAY_BOLT = "otherDayBolt";

	/**
	 * fatigue driving hour bolt
	 */
	public static final String FATIGUE_DRIVING_HOUR_BOLT = "fatigueDrivingHourBolt";

	/**
	 * fatigue driving day bolt
	 */
	public static final String FATIGUE_DRIVING_DAY_BOLT = "fatigueDrivingDayBolt";

	/**
	 * status hour bolt
	 */
	public static final String STATUS_HOUR_BOLT = "statusHourBolt";

	/**
	 * status day bolt
	 */
	public static final String STATUS_DAY_BOLT = "statusDayBolt";

	/**
	 * persist hour bolt
	 */
	public static final String PERSIST_HOUR_BOLT = "persistHourBolt";

	/**
	 * persist day bolt
	 */
	public static final String PERSIST_DAY_BOLT = "persistDayBolt";


	public static class ZoneScheduleStream {

		/**
		 * 天定时任务的bolt
		 */
		public static final String ZONE_SCHEDULED_DAY_BOLT_F = "zoneScheduleDayBolt";
		/**
		 * trace delete fileds key1
		 */
		public static final String ZONE_SCHEDULED_KEY_F = "m";


	}
	public static class DormancyStream {

		/**
		 * dormancy的bolt
		 */
		public static final String DORMANCY_DAY_BOLT_F = "zoneDormancyDayBolt";

		/**
		 * dormancy的bolt
		 */
		public static final String DORMANCY_GDCP3_DAY_BOLT_F = "zoneGdcp3DormancyDayBolt";
		/**
		 * dormancy fileds key1
		 */
		public static final String DORMANCY_KEY_F = "m";

		/**
		 * dormancy fileds key2
		 */
		public static final String DORMANCY_KEY_S = "s";


	}


}
