const { Pool } = require("pg");
require("dotenv").config();
const format = require("pg-format");

const mainClient = new Pool({ max: 2 });
mainClient.connect();

const getAndStore = async () => {
  console.log(`getting data`)
  const d = await mainClient.query(`
  SELECT
    ticker,
    date_trunc('hour', timestamp) as timestamp,
    (array_agg(open ORDER BY timestamp))[1] as open,
    MAX(high) as high,
    MIN(low) as low,
    (array_agg(close ORDER BY timestamp DESC))[1] as close,
    SUM(volume) as volume
  FROM candles
  GROUP BY ticker, date_trunc('hour', timestamp)
  ORDER by timestamp;
  `);

  console.log(`got ${d.rowCount}`)
  const candles = d.rows.map((c) => [
    c.ticker,
    c.timestamp,
    c.low,
    c.high,
    c.open,
    c.close,
    c.volume,
  ]);
  const insertQuery = `
  INSERT INTO hour_candles (ticker, timestamp, low, high, open, close, volume) 
  VALUES %L 
  ON CONFLICT ON CONSTRAINT hour_candles_pkey
  DO UPDATE SET (ticker, timestamp, low, high, open, close, volume) = (EXCLUDED.ticker, EXCLUDED.timestamp, EXCLUDED.low, EXCLUDED.high, EXCLUDED.open, EXCLUDED.close, EXCLUDED.volume);
`;
  const s = await mainClient.query(format(insertQuery, candles));
  console.log(`inserted ${s.rowCount}`);
};

(async () => {
  await getAndStore()
})()