const { PutCommand, GetCommand } = require("@aws-sdk/lib-dynamodb");
const { DynamoDBClient } = require("@aws-sdk/client-dynamodb");
const { stringify } = require("zipson");
const pairs = require("../pairs.json");
const { addTokenInfo } = require("../helpers/token");

const CACHE_TABLE = process.env.TOKENS_TABLE || false;
const ddbClient = new DynamoDBClient({ region: "us-east-1" });

const getAllTokens = async () => {
  const allTokens = pairs.reduce((p,c) => {
    const {token1, token2} = c;
    if(!(token1.code in p)) {
      p[token1.code] = token1;
    }
    if(!(token2.code in p)) {
      p[token2.code] = token2;
    }
    return p;
  }, {})
  delete allTokens.coin
  return allTokens;
};

const allTokenUpdate = async () => {
  const allTokens = await getAllTokens();
  const finalTokens = await addTokenInfo(allTokens);
  const item = {
    TableName: CACHE_TABLE,
    Item: {
      id: "TOKENS",
      cachedValue: stringify(finalTokens, { fullPrecisionFloats: true }),
    },
  };
  console.log("UPLOADING");
  await ddbClient.send(new PutCommand(item));
};

module.exports = allTokenUpdate;