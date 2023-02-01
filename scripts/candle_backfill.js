const { DateTime } = require("luxon");
const { Client } = require("pg");
const format = require("pg-format");
const { getKDAMap, getNearestKDAPrice, getAllTokens, getCandleOrBuild } = require("../helpers");
require("dotenv").config();

const mainClient = new Client();
mainClient.connect();

const selectQuery = `
 SELECT * FROM transactions WHERE from_token = $1 OR to_token = $2 AND timestamp >= $3 AND timestamp < $4 ORDER BY timestamp ASC`;

const END_DATE = DateTime.now().startOf("minute").minus({ minutes: 1 });

(async () => {
  console.log("Getting KDA Prices");
  const kdaPriceMap = await getKDAMap(mainClient);
  console.log("Built KDA Price Map");

  const tokenMap = await getAllTokens();
  const tokens = Object.keys(tokenMap);

  for (let token of tokens) {
    console.log(`Processing ${token}`);
    const firstCandle = await getCandleOrBuild(mainClient, kdaPriceMap, tokenMap[token], token, 'ASC');
    let firstCandleDate = DateTime.fromJSDate(firstCandle.timestamp, {
      zone: "utc",
    })
      .startOf("minute")
      .plus({ minutes: 1 });
    let start = firstCandleDate;
    let candles = [
      [
        tokenMap[token],
        firstCandle.timestamp,
        parseFloat(firstCandle.low),
        parseFloat(firstCandle.high),
        parseFloat(firstCandle.open),
        parseFloat(firstCandle.close),
        parseFloat(firstCandle.volume),
      ],
    ];
    while (start < END_DATE) {
      const addedEnd = start.plus({ months: 1 });
      let end = addedEnd > END_DATE ? END_DATE : addedEnd;
      console.log(
        `Getting TX of ${token} for ${start.toJSDate()} to ${end.toJSDate()}`
      );

      const transactionsR = await mainClient.query(selectQuery, [
        token,
        token,
        start.toJSDate(),
        end.toJSDate(),
      ]);

      const transactionsMap = transactionsR.rows.reduce((p, row) => {
        const { timestamp, from_token, from_amount, to_amount, volume } = row;
        if (volume < 0.00000001) {
          return p;
        }
        const fromAmount = parseFloat(from_amount);
        const toAmount = parseFloat(to_amount);
        const v = volume;
        const luxonTime = DateTime.fromJSDate(timestamp, { zone: "utc" });
        const minuteStart = luxonTime.startOf("minute").toJSDate();
        const priceInKDA =
          from_token === "coin" ? fromAmount / toAmount : toAmount / fromAmount;

        const kdaPrice = getNearestKDAPrice(kdaPriceMap, minuteStart);
        const priceInUSD = priceInKDA * kdaPrice;
        const price = priceInUSD;

        if (!(minuteStart in p)) {
          p[minuteStart] = {
            volume: parseFloat(v),
            timestamp,
            close: price,
            low: price,
            high: price,
          };
        } else {
          const candle = p[minuteStart];
          candle.volume += parseFloat(volume);
          candle.close = price;
          candle.low = Math.min(candle.low, price);
          candle.high = Math.max(candle.high, price);
          p[minuteStart] = candle;
        }

        return p;
      }, {});
      while (start < end) {
        const prevClose = candles[candles.length - 1][5];
        if (start.toJSDate() in transactionsMap) {
          const info = transactionsMap[start.toJSDate()];
          candles.push([
            tokenMap[token],
            start.toJSDate(),
            Math.min(info.low, prevClose),
            Math.max(info.high, prevClose),
            prevClose,
            info.close,
            info.volume,
          ]);
        } else {
          candles.push([
            tokenMap[token],
            start.toJSDate(),
            prevClose,
            prevClose,
            prevClose,
            prevClose,
            0,
          ]);
        }

        start = start.plus({ minutes: 1 });
      }
    }
    console.log(`Built ${candles.length} candles for ${token} `);
    const insertedCandles = await mainClient.query(
      format(
        `INSERT INTO candles (ticker, timestamp, low, high, open, close, volume) VALUES %L 
        ON CONFLICT ON CONSTRAINT candles_pkey
        DO UPDATE 
        SET (ticker, timestamp, low, high, open, close, volume) = (EXCLUDED.ticker, EXCLUDED.timestamp, EXCLUDED.low, EXCLUDED.high, EXCLUDED.open, EXCLUDED.close, EXCLUDED.volume);`,
        candles
      )
    );

    console.log(
      `Inserted ${insertedCandles.rowCount} candles for token ${token}`
    );
  }
  console.log("Done");
  process.exit();
})();
