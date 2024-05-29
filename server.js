import express from 'express';
import { createTables } from './config/db_config.js';
import { triggerReport, getReport } from './reports/report.js';

const app = express();

app.use(express.json());

app.post('/trigger_report', triggerReport);

app.get('/get_report', getReport);

const PORT = process.env.PORT || 3000;
app.listen(PORT, async () => {
    await createTables();
    console.log(`Server is running on port ${PORT}`);
});
