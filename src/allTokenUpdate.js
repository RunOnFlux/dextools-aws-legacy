const { PutCommand, GetCommand } = require("@aws-sdk/lib-dynamodb");
const { DynamoDBClient } = require("@aws-sdk/client-dynamodb");
const { stringify } = require("zipson");
const pairs = require("../pairs.json");
const tokens = require("../token.json");

const CACHE_TABLE = process.env.TOKENS_TABLE || false;
const ddbClient = new DynamoDBClient({ region: "us-east-1" });

const getAllTokens = async () => {
  const mainnetPairs = pairs.pairs.mainnet;
  const tokenMap = Object.keys(mainnetPairs).reduce((p, c) => {
    const pairInfo = mainnetPairs[c];
    if (!pairInfo.isVerified) {
      return p;
    }
    const [address0, address1] = c.split(":");
    const { token0, token1 } = pairInfo;
    p[address0] = tokens[token0];
    p[address1] = tokens[token1];
    return p;
  }, {});
  delete tokenMap["coin"];
  return tokenMap;
};

const allTokenUpdate = async () => {
  const allTokens = await getAllTokens();
  const item = {
    TableName: CACHE_TABLE,
    Item: {
      id: "TOKENS",
      cachedValue: stringify(allTokens, { fullPrecisionFloats: true }),
    },
  };
  console.log("UPLOADING");
  await ddbClient.send(new PutCommand(item));
};

module.exports = allTokenUpdate;