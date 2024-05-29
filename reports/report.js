import { v4 as uuidv4 } from "uuid";
import { pool } from "../config/db_config.js";
import { log, error } from "console";
import ora from "ora";

import {
  fetchStores,
  fetchStoreDetails,
  calculateUptimeDowntime,
  convertToCsv,
  fetchDistinctStoreCount,
} from "../utils/common.js";

const BATCH_SIZE = 500;

const triggerReport = async (req, res) => {
  const reportId = uuidv4();
  await pool.query(
    "INSERT INTO report_status (report_id, status) VALUES ($1, $2)",
    [reportId, "Running"]
  );
  generateReport(reportId);
  res.json({ report_id: reportId });
};

const getReport = async (req, res) => {
  const { report_id } = req.body;
  const result = await pool.query(
    "SELECT status, result FROM report_status WHERE report_id = $1",
    [report_id]
  );
  const reportStatus = result.rows[0];
  if (reportStatus.status === "Running") {
    res.json({ status: "Running" });
  } else {
    res.json({ status: "Complete", report: reportStatus.result });
  }
};

const generateReport = async (reportId) => {
  let count = 0;
  let startTime = new Date();
  let percentage = 0;
  let totalRecordsToProcess = await fetchDistinctStoreCount();
  const spinner = ora(`Generating Report ${percentage}% `).start();

  try {
    let offset = 0;
    let stores;
    const reportData = [];

    do {
      stores = await fetchStores(offset, BATCH_SIZE);
      offset += BATCH_SIZE;

      const storeDetails = await fetchStoreDetails(stores);

      const storeDataPromises = stores.map(async (storeId) => {
        const timezone =
          storeDetails.timezones.find((tz) => tz.store_id === storeId)
            ?.timezone_str || "America/Chicago";
        const businessHours = storeDetails.businessHours.filter(
          (bh) => bh.store_id === storeId
        );
        const statusData = storeDetails.statuses.filter(
          (sd) => sd.store_id === storeId
        );

        const {
          uptimeLastHour,
          uptimeLastDay,
          uptimeLastWeek,
          downtimeLastHour,
          downtimeLastDay,
          downtimeLastWeek,
        } = calculateUptimeDowntime(statusData, businessHours, timezone);

        count++;

        percentage = Math.floor((count / totalRecordsToProcess) * 100);

        return {
          store_id: storeId,
          uptime_last_hour: uptimeLastHour,
          uptime_last_day: uptimeLastDay,
          uptime_last_week: uptimeLastWeek,
          downtime_last_hour: downtimeLastHour,
          downtime_last_day: downtimeLastDay,
          downtime_last_week: downtimeLastWeek,
        };
      });

      const batchReportData = await Promise.all(storeDataPromises);
      reportData.push(...batchReportData);
      spinner.text = `Generating Report ${percentage}% `;
    } while (stores.length === BATCH_SIZE);

    const reportCsv = convertToCsv(reportData);
    await pool.query(
      "UPDATE report_status SET status = $1, result = $2 WHERE report_id = $3",
      ["Complete", reportCsv, reportId]
    );

    log("Report generation complete.");
  } catch (e) {
    error("Error generating report:", e);

    await pool.query(
      "UPDATE report_status SET status = $1 WHERE report_id = $2",
      ["Error", reportId]
    );
  } finally {
    const endTime = new Date();
    spinner.stop();
    const durationInSeconds = (endTime - startTime) / 1000;
    const durationInMinutes = durationInSeconds / 60;
    log(`Total time taken: ${durationInMinutes} minutes`);
  }
};

export { triggerReport, getReport };
