const { PutCommand, GetCommand } = require('@aws-sdk/lib-dynamodb');
const { DynamoDBClient } = require('@aws-sdk/client-dynamodb');
const { stringify } = require('zipson');
const pairs = require('../pairs.json');
const { addTokenInfo } = require('../helpers/token');
const { default: axios } = require('axios');

const CACHE_TABLE = process.env.TOKENS_TABLE || false;
const ddbClient = new DynamoDBClient({ region: 'us-east-1', endpoint: process.env.AWS_ENDPOINT || undefined });

const getAllTokens = async () => {
  // TODO: dismiss analytics-api.kaddex.com
  const tokensR = await axios.get('https://analytics-api.kaddex.com/token-data/pairs');
  const allTokens = tokensR.data.reduce((p, c) => {
    const { token1, token2 } = c;
    if (!(token1.code in p)) {
      p[token1.code] = token1;
    }
    if (!(token2.code in p)) {
      p[token2.code] = token2;
    }
    return p;
  }, {});
  delete allTokens.coin;
  return allTokens;
};

const allTokenUpdate = async () => {
  const allTokens = await getAllTokens();
  const finalTokens = await addTokenInfo(allTokens);
  const item = {
    TableName: CACHE_TABLE,
    Item: {
      id: 'TOKENS',
      cachedValue: stringify(finalTokens, { fullPrecisionFloats: true }),
    },
  };
  console.log('UPLOADING');
  await ddbClient.send(new PutCommand(item));
};

module.exports = allTokenUpdate;

/**
 * aws dynamodb create-table --table-name tokens-table --attribute-definitions AttributeName=id,AttributeType=S --key-schema AttributeName=id,KeyType=HASH  --provisioned-throughput ReadCapacityUnits=1,WriteCapacityUnits=1 --endpoint-url http://localhost:8000
 */
