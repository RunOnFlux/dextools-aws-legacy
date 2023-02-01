const Pact = require("pact-lang-api");
const { DateTime } = require("luxon");
const format = require("pg-format");
const pairs = require("../tokens.json");

const GAS_PRICE = 0.00000001;
const MAX_CHAIN_ID = 20;
const creationTime = () => Math.round(new Date().getTime() / 1000) - 10;
const TTL = 1000000;

const CHAINWEB_HOSTS = [
  `https://kmdsapactapi_31351.app.runonflux.io`,
  "https://kadena2.app.runonflux.io",
  "https://api.chainweb.com",
];

const getNetwork = (url, chainId) =>
  `${url}/chainweb/0.0/mainnet01/chain/${chainId}/pact`;

const getReserve = (tokenData) => {
  return parseFloat(tokenData.decimal ? tokenData.decimal : tokenData);
};

const makeCMD = (chainId, pactCode, gasLimit) => {
  return {
    pactCode,
    keyPairs: Pact.crypto.genKeyPair(),
    meta: Pact.lang.mkMeta(
      "",
      chainId,
      GAS_PRICE,
      gasLimit,
      creationTime(),
      TTL
    ),
  };
};

const makePactCallWithFallback = async (
  chainId,
  pactCode,
  gasLimit,
  urlIndex = 0
) => {
  if (urlIndex === CHAINWEB_HOSTS.length) {
    throw new Error(`could not fetch from all hosts`);
  }
  const host = CHAINWEB_HOSTS[urlIndex];
  const chainwebUrl = getNetwork(host, chainId);

  return Pact.fetch
    .local(makeCMD(chainId, pactCode, gasLimit), chainwebUrl)
    .then((data) => {
      if (data.result && data.result.status) {
        console.log(`Success: ${chainwebUrl}`);
        return data;
      }
      throw new Error(`failed to fetch from ${chainwebUrl}`);
    })
    .catch((e) => {
      if (e.message !== `failed to fetch from ${chainwebUrl}`) {
        console.log(`Failed: ${chainwebUrl}: ${e}`);
      }
      return makePactCallWithFallback(
        chainId,
        pactCode,
        gasLimit,
        urlIndex + 1
      );
    });
};

const makePactCall = async (chainId, pactCode, gasLimit = 30000000) => {
  return await makePactCallWithFallback(chainId, pactCode, gasLimit, 0);
};

const getTokenAddressFromRef = (spec) => {
  return spec.namespace ? `${spec.namespace}.${spec.name}` : spec.name;
};

const getKDAMap = async (client) => {
  const kdaPriceR = await client.query(
    `SELECT timestamp, price FROM kda_price WHERE timestamp > '2021-08-01 00:00:00'::TIMESTAMP`
  );
  const kdaPriceMap = kdaPriceR.rows.reduce((p, row) => {
    const { timestamp, price } = row;
    p[timestamp] = parseFloat(price);
    return p;
  }, {});

  return kdaPriceMap;
};

const getNearestKDAPrice = (kdaPriceMap, date) => {
  let kdaMinute = date;
  while (!(kdaMinute in kdaPriceMap)) {
    kdaMinute = DateTime.fromJSDate(kdaMinute).minus({ minutes: 1 }).toJSDate();
  }
  return kdaPriceMap[kdaMinute];
};

const getAllTokens = async () => {
  const mainnetPairs = pairs.pairs.mainnet;
  const tokenMap = Object.keys(mainnetPairs).reduce((p, c) => {
    const pairInfo = mainnetPairs[c];
    if (!pairInfo.isVerified) {
      return p;
    }
    const [address0, address1] = c.split(":");
    const { token0, token1 } = pairInfo;
    p[address0] = token0;
    p[address1] = token1;
    return p;
  }, {});
  delete tokenMap['coin'];
  return tokenMap;
};

const buildFirstCandle = async (
  client,
  kdaPriceMap,
  ticker,
  tokenAddress
) => {
  const firstTXN = await client.query(
    `SELECT * FROM transactions WHERE from_token=$1 OR to_token=$2 ORDER BY timestamp asc LIMIT 1`,
    [tokenAddress, tokenAddress]
  );
  const { timestamp, from_token, from_amount, to_amount, volume } =
    firstTXN.rows[0];

  const dateTime = DateTime.fromJSDate(timestamp);
  const kdaPrice = getNearestKDAPrice(
    kdaPriceMap,
    dateTime.startOf("minute").toJSDate()
  );
  const priceInKDA =
    from_token === "coin" ? from_amount / to_amount : to_amount / from_amount;
  const priceInUSD = priceInKDA * kdaPrice;
  const candle = [
    ticker,
    dateTime.startOf("minute").toJSDate(),
    priceInUSD,
    priceInUSD,
    priceInUSD,
    priceInUSD,
    parseFloat(volume),
  ];
  await client.query(
    `INSERT INTO candles (ticker, timestamp, low, high, open, close, volume) VALUES ($1,$2,$3,$4,$5,$6,$7) ON CONFLICT ON CONSTRAINT candles_pkey DO NOTHING`,
    candle
  );
};

const getCandleOrBuild = async (client, kdaPriceMap, ticker, tokenAddress, order) => {
  const hasCandle = await client.query('SELECT * FROM candles WHERE ticker=$1 ORDER BY timestamp LIMIT 1', [ticker]);
  if(hasCandle.rowCount === 0) {
    await buildFirstCandle(client, kdaPriceMap, ticker, tokenAddress);
  }
  const candle = await client.query(`SELECT * FROM candles WHERE ticker=$1 ORDER BY timestamp ${order} LIMIT 1`, [ticker]);
  return candle.rows[0];
};

module.exports = {
  makePactCall,
  getReserve,
  getTokenAddressFromRef,
  getKDAMap,
  getNearestKDAPrice,
  getAllTokens,
  getCandleOrBuild,
};
