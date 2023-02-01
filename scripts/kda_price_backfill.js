const { default: axios } = require("axios");
const { DateTime } = require("luxon");
const { Client } = require("pg");
const format = require("pg-format");
require("dotenv").config();

const client = new Client();
client.connect();

const insertQuery = `
INSERT INTO kda_price(timestamp, price)
VALUES %L
ON CONFLICT (timestamp) DO UPDATE SET price = excluded.price
RETURNING *
`;

const API_URL =
  "https://www.kucoin.com/_api/order-book/candles?symbol=KDA-USDT&type=1min";

const INTERVAL = 90000;

// O H L C _ V
(async () => {
  let start = 1652659200;
  const end = DateTime.now()
    .startOf("minute")
    .minus({ minutes: 1 })
    .toSeconds();
  while (start < end) {
    const realEnd = start + INTERVAL > end ? end : start + INTERVAL;
    console.log(`Starting: ${start}`);
    const resp = await axios.get(`${API_URL}&begin=${start}&end=${realEnd}`);
    const data = resp.data;
    const candles = data.data;
    const values = candles.map((candle) => {
      const [strTime, , , , strClose, ,] = candle;
      const timestamp = parseInt(strTime);
      const close = parseFloat(strClose);
      const date = new Date(timestamp * 1000);
      return [date, close];
    });
    console.log(`Trying: ${values.length} rows`);
    const row = await client.query(format(insertQuery, values));

    start = start + INTERVAL;
    console.log(`Added: ${row.rowCount} rows`);
    console.log(`New Start: ${start}`);
  }

  console.log("Done");
  process.exit();
})();
