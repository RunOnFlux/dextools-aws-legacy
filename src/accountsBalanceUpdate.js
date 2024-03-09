const { PutCommand, GetCommand, ScanCommand } = require('@aws-sdk/lib-dynamodb');
const { DynamoDBClient } = require('@aws-sdk/client-dynamodb');
const { getStoredKadenaTokensByChain } = require('./allKadenaTokenUpdate');
const { makePactCall, getReserve } = require('../helpers/pact');
const { Client } = require('pg');
const {
  sleep,
  constants: { KADENA_CHAINS_COUNT },
} = require('../helpers');
const { parse, stringify } = require('zipson/lib');

const mainClient = new Client();
mainClient.connect();

const ACCOUNTS_CHUNK_SIZE = 20;

const KADENA_ACCOUNTS_TABLE = process.env.KADENA_ACCOUNTS_TABLE || 'kadena-accounts';
const KADENA_ACCOUNTS_BALANCE_TABLE = process.env.KADENA_ACCOUNTS_BALANCE_TABLE || 'kadena-accounts-balance';
const ddbClient = new DynamoDBClient({
  region: 'us-east-1',
  endpoint: process.env.AWS_ENDPOINT || undefined,
});

const getAllTickerLastPrices = async () => {
  const selectQuery = `SELECT cm.ticker, cm.timestamp, cm.close
                        FROM candles cm
                        INNER JOIN (
                          SELECT
                              ticker,
                              MAX(timestamp) as last_timestamp
                          FROM
                              candles
                          GROUP BY
                              ticker
                        ) as latest ON cm.ticker = latest.ticker AND cm.timestamp = latest.last_timestamp;
    `;
  const tokenResponse = await mainClient.query(selectQuery);
  const kdaResponse = await mainClient.query(`SELECT * FROM kda_price ORDER BY timestamp DESC LIMIT 1`);
  const kdaCandle = kdaResponse?.rows[0];
  return [{ ticker: 'KDA', timestamp: kdaCandle?.timestamp, close: kdaCandle?.price }, ...(tokenResponse?.rows ?? [])];
};

const updateAccountsBalance = async () => {
  let lastEvaluatedKey = undefined;
  let accountsResponse = [];
  const lastTokenPrices = await getAllTickerLastPrices();
  const storedTokens = await ddbClient.send(
    new ScanCommand({
      TableName: process.env.TOKENS_TABLE,
    })
  );
  const tokensData = parse(storedTokens?.Items[0]?.cachedValue);
  const getTokenSymbolByModuleName = (module) => (module === 'coin' ? 'KDA' : tokensData[module]?.symbol ?? null);
  const getTokenPriceByModuleName = (module) => lastTokenPrices.find((token) => token.ticker === getTokenSymbolByModuleName(module))?.close ?? 0;
  do {
    accountsResponse = await ddbClient.send(
      new ScanCommand({
        TableName: KADENA_ACCOUNTS_TABLE,
        Limit: ACCOUNTS_CHUNK_SIZE,
        ExclusiveStartKey: lastEvaluatedKey,
      })
    );
    lastEvaluatedKey = accountsResponse.LastEvaluatedKey;
    const accounts = accountsResponse.Items.map((item) => item.account);
    if (accounts.length) {
      const dataToPersist = accounts.reduce((acc, key) => {
        acc[key] = [];
        return acc;
      }, {});
      for (let chainId = 0; chainId < KADENA_CHAINS_COUNT; chainId++) {
        const tokens = await getStoredKadenaTokensByChain(chainId);
        const getTokenAlias = (tokenName) => tokenName.replace(/\./g, '');
        const pactCode = `
            (
              let* (
                    ${accounts
                      .map(
                        (account, j) => `
                      ${tokens?.map((ft) => `(${getTokenAlias(ft)}_${j} (try 0.0 (${ft}.get-balance "${account}")))`).join('\n')}`
                      )
                      .join('\n')}
                  )
                   
                    {${accounts.map(
                      (acc, j) => `
                      "${acc}": {
                        ${tokens?.map((ft) => `"${ft}": ${getTokenAlias(ft)}_${j}`)}
                      }
                      `
                    )}}
            )`;
        try {
          const res = await makePactCall(chainId.toString(), pactCode);
          console.log(`CHAIN ${chainId}`);
          if (res?.result?.status === 'success') {
            Object.keys(res?.result?.data).forEach((accountString) => {
              if (dataToPersist[accountString]) {
                const balances = Object.keys(res?.result?.data[accountString]).map((token) => {
                  const balance = getReserve(res?.result?.data[accountString][token]);
                  const price = getTokenPriceByModuleName(token);
                  const usdBalance = parseFloat((balance * parseFloat(price)).toFixed(2));
                  return {
                    token,
                    balance,
                    price,
                    usdBalance,
                  };
                });
                dataToPersist[accountString].push({
                  chainId,
                  balances,
                  usdValue: balances.reduce((acc, token) => acc + token.usdBalance, 0),
                });
              }
            });
          } else {
            console.error(res?.result?.error?.message);
          }

          // await sleep(300);
        } catch (err) {
          console.error(`ERROR on chain ${chainId} `);
          console.log(err);
        }
      }
      for (const account of Object.keys(dataToPersist)) {
        const item = {
          TableName: KADENA_ACCOUNTS_BALANCE_TABLE,
          Item: {
            account,
            date: new Date().toISOString().split('T')[0],
            totalUsdValue: dataToPersist[account].reduce((total, current) => total + current.usdValue, 0),
            balances: stringify(dataToPersist[account]),
          },
        };
        await ddbClient.send(new PutCommand(item));
      }
    }
  } while (lastEvaluatedKey && accountsResponse?.Items?.length === ACCOUNTS_CHUNK_SIZE);
  console.log('ACCOUNTS BALANCE UPDATE DONE');
};

module.exports = updateAccountsBalance;

/**
 aws dynamodb create-table --table-name kadena-accounts --attribute-definitions AttributeName=account,AttributeType=S --key-schema AttributeName=account,KeyType=HASH --provisioned-throughput ReadCapacityUnits=1,WriteCapacityUnits=1 --endpoint-url http://localhost:8000
 aws dynamodb create-table --table-name kadena-accounts-balance --attribute-definitions AttributeName=account,AttributeType=S AttributeName=date,AttributeType=S --key-schema AttributeName=account,KeyType=HASH AttributeName=date,KeyType=RANGE --provisioned-throughput ReadCapacityUnits=1,WriteCapacityUnits=1 --endpoint-url http://localhost:8000

 */
