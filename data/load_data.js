import fs from "fs";
import csv from "csv-parser";
import { pool } from "../config/db_config.js";
import path from "path";
import { fileURLToPath } from "url";
import { error, log } from "console";
import ora from "ora";

const __filename = fileURLToPath(import.meta.url);
const __dirname = path.dirname(__filename);
const BATCH_SIZE = 5000;
const loadCSV = async (filePath, query) => {
  return new Promise((resolve, reject) => {
    const results = [];
    let rowCount = 0;
    let batchCount = 0;
    let startTime = new Date();

    const spinner = ora(`Processed batch ${batchCount}`).start();

    const stream = fs
      .createReadStream(filePath)
      .pipe(csv())
      .on("data", async (data) => {
        rowCount++;
        results.push(Object.values(data));

        if (results.length === BATCH_SIZE) {
          stream.pause();
          await insertBatch(query, results);
          results.length = 0; // Clear the array
          stream.resume();
          batchCount++;
          // log(`Processed batch ${batchCount}`);
          spinner.text = `Processed batch ${batchCount}`;
        }
      })
      .on("end", async () => {
        if (results.length > 0) {
          await insertBatch(query, results);
        }
        const endTime = new Date();
        spinner.stop();
        const durationInSeconds = (endTime - startTime) / 1000;
        const durationInMinutes = durationInSeconds / 60;
        log(`Time taken: ${durationInMinutes} minutes`);
        resolve();
      })
      .on("error", reject);
  });
};
const insertBatch = async (query, batch) => {
  const client = await pool.connect();
  try {
    await client.query("BEGIN");
    for (const values of batch) {
      await client.query(query, values);
    }
    await client.query("COMMIT");
  } catch (error) {
    await client.query("ROLLBACK");
    throw error;
  } finally {
    client.release();
  }
};
const loadData = async () => {
  const dataFolderPath = path.join(__dirname);
  let startTime = new Date();
  try {
    await loadCSV(
      path.join(dataFolderPath, "StoreStatus.csv"),
      "INSERT INTO store_status (store_id, status,timestamp_utc) VALUES ($1, $2, $3)"
    );
    await loadCSV(
      path.join(dataFolderPath, "BusinessHour.csv"),
      "INSERT INTO business_hours (store_id, day_of_week, start_time_local, end_time_local) VALUES ($1, $2, $3, $4)"
    );
    await loadCSV(
      path.join(dataFolderPath, "TimeZone.csv"),
      "INSERT INTO store_timezones (store_id, timezone_str) VALUES ($1, $2)"
    );
    log("Data loaded successfully");
  } catch (err) {
    error("Error loading data:", err);
  } finally {
    const endTime=new Date();
    const durationInSeconds = (endTime - startTime) / 1000;
    const durationInMinutes = durationInSeconds / 60;
    log(`Total time taken: ${durationInMinutes} minutes`);
  }
};

loadData();
