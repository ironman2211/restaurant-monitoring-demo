import moment from 'moment-timezone';
import { pool } from '../config/db_config.js';

const fetchDistinctStoreCount = async () => {
    const result = await pool.query("SELECT COUNT(DISTINCT store_id) AS count FROM store_status");
    return result.rows[0].count;
};
const fetchStores = async (offset, limit) => {
    const result = await pool.query(
      "SELECT DISTINCT store_id FROM store_status ORDER BY store_id LIMIT $1 OFFSET $2",
      [limit, offset]
    );
    return result.rows.map(row => row.store_id);
};
const fetchStoreDetails = async (storeIds) => {
    const storeIdsString = storeIds.join(',');
    
    const [timezones, businessHours, statuses] = await Promise.all([
      pool.query(`SELECT store_id, timezone_str FROM store_timezones WHERE store_id IN (${storeIdsString})`),
      pool.query(`SELECT store_id, day_of_week, start_time_local, end_time_local FROM business_hours WHERE store_id IN (${storeIdsString})`),
      pool.query(`SELECT store_id, timestamp_utc, status FROM store_status WHERE store_id IN (${storeIdsString})`)
    ]);
  
    return {
      timezones: timezones.rows,
      businessHours: businessHours.rows,
      statuses: statuses.rows
    };
};

const getBusinessHours = async (storeId) => {
    const result = await pool.query('SELECT * FROM business_hours WHERE store_id = $1', [storeId]);
    return result.rows;
};

const getStoreStatus = async (storeId) => {
    const result = await pool.query('SELECT * FROM store_status WHERE store_id = $1', [storeId]);
    return result.rows;
};

const getStoreTimezone = async (storeId) => {
    const result = await pool.query('SELECT timezone_str FROM store_timezones WHERE store_id = $1', [storeId]);
    return result.rows[0]?.timezone_str || 'America/Chicago';
};  

const calculateUptimeDowntime = (statusData, businessHours, timezone) => {
    const now = moment.utc();
    const oneHourAgo = moment.utc().subtract(1, 'hours');
    const oneDayAgo = moment.utc().subtract(1, 'days');
    const oneWeekAgo = moment.utc().subtract(7, 'days');

    const convertToStoreTime = (utcTime, timezone) => moment.utc(utcTime).tz(timezone);
    const calculateIntervals = (start, end) => {
    let uptime = 0, downtime = 0;

    const startTime = convertToStoreTime(start, timezone);
    const endTime = convertToStoreTime(end, timezone);

    businessHours.forEach(({ day_of_week, start_time_local, end_time_local }) => {
        const startOfDay = startTime.clone().startOf('week').day(day_of_week);

        const startOfInterval = moment.tz(`${startOfDay.format('YYYY-MM-DD')} ${start_time_local}`, 'YYYY-MM-DD HH:mm:ss', timezone);
        const endOfInterval = moment.tz(`${startOfDay.format('YYYY-MM-DD')} ${end_time_local}`, 'YYYY-MM-DD HH:mm:ss', timezone);
        // If start is after end of interval or end is before start of interval, skip
        if (startTime.isAfter(endOfInterval) || endTime.isBefore(startOfInterval)) {
            return;
        }

        const intervalStart = moment.max(startTime, startOfInterval);
        const intervalEnd = moment.min(endTime, endOfInterval);

        uptime += intervalEnd.diff(intervalStart, 'minutes');
    });

    downtime = endTime.diff(startTime, 'minutes') - uptime;
    return { uptime, downtime };
};
    const calculateForPeriod = (start, end) => {
        let uptime = 0, downtime = 0;
        const sortedData = statusData.sort((a, b) => moment.utc(a.timestamp_utc) - moment.utc(b.timestamp_utc));
        const filteredData = sortedData.filter(({ timestamp_utc }) => moment.utc(timestamp_utc).isBetween(start, end));
        for (let i = 0; i < filteredData.length - 1; i++) {
            const current = filteredData[i];
            const next = filteredData[i + 1];
            const { uptime: periodUptime, downtime: periodDowntime } = calculateIntervals(current.timestamp_utc, next.timestamp_utc);
           if (current.status === 'active') {
                uptime += periodUptime;
                downtime += periodDowntime;
            } else {
                uptime += periodDowntime;
                downtime += periodUptime;
            }
        }
        return { uptime, downtime };
    };

    const { uptime: uptimeLastHour, downtime: downtimeLastHour } = calculateForPeriod(oneHourAgo, now);
    const { uptime: uptimeLastDay, downtime: downtimeLastDay } = calculateForPeriod(oneDayAgo, now);
    const { uptime: uptimeLastWeek, downtime: downtimeLastWeek } = calculateForPeriod(oneWeekAgo, now);

    return {
        uptimeLastHour,
        uptimeLastDay,
        uptimeLastWeek,
        downtimeLastHour,
        downtimeLastDay,
        downtimeLastWeek
    };
};


const convertToCsv = (reportData) => {
    const headers = 'store_id,uptime_last_hour,downtime_last_hour,uptime_last_day,downtime_last_day,uptime_last_week,downtime_last_week\n';
    const rows = reportData.map(row => (
        `${row.store_id},${row.uptime_last_hour},${row.downtime_last_hour},${row.uptime_last_day},${row.downtime_last_day},${row.uptime_last_week},${row.downtime_last_week}`
    ));
    return headers + rows.join('\n');
};

export {
    getBusinessHours,
    getStoreStatus,
    getStoreTimezone,
    calculateUptimeDowntime,
    convertToCsv,
    fetchStoreDetails,
    fetchStores,
    fetchDistinctStoreCount
};
