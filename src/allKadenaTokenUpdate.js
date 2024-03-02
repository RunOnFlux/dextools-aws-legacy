const { PutCommand, GetCommand } = require("@aws-sdk/lib-dynamodb");
const { DynamoDBClient } = require("@aws-sdk/client-dynamodb");
const { stringify, parse } = require("zipson");
const { makePactCall } = require("../helpers/pact");
const {
  sleep,
  constants: { MAX_CHAIN_ID },
} = require("../helpers");

const CACHE_TABLE = process.env.KADENA_TOKENS_TABLE || "kadena-tokens-table";
const ddbClient = new DynamoDBClient({
  region: "us-east-1",
  endpoint: process.env.AWS_ENDPOINT || undefined,
});

const getAllKadenaTokens = async () => {
  const pactCode = `(let
    ((all-tokens
       (lambda (contract:object)
         (let*
           ((module-name (at 'name contract))
            (interfaces (if (contains 'interfaces contract) (at 'interfaces contract) (if (contains 'interface contract) (at 'interface contract) [])))
            (is-implementing-fungible-v2 (contains "fungible-v2" interfaces))
           )
         (if is-implementing-fungible-v2 module-name "")
         )
       )
     )
    )
    (filter (!= "") (map (all-tokens) (map (describe-module) (list-modules))))
  )`;
  const validChainModules = {};
  const invalidChainModules = {};
  for (let chainId = 0; chainId < MAX_CHAIN_ID; chainId++) {
    const validChainTokens = [];
    const invalidChainTokens = [];
    try {
      const res = await makePactCall(chainId.toString(), pactCode);
      if (res?.result?.data?.length > 0) {
        console.log(
          `[CHAIN ${chainId}] FOUND ${res?.result?.data?.length} tokens`
        );
        let tokenCount = 1;
        for (const token of res?.result?.data) {
          try {
            const isTokenWorking = await makePactCall(
              chainId.toString(),
              `(${token}.get-balance "k:alice")`
            );
            await sleep(500);
            if (
              isTokenWorking?.result?.status === "success" ||
              isTokenWorking?.result?.error?.message?.includes("row not found")
            ) {
              validChainTokens.push(token);
            } else {
              console.error(
                `[CHAIN ${chainId}] TOKEN ${tokenCount}/${res?.result?.data?.length} ${token} IS NOT VALID`
              );
              invalidChainTokens.push(token);
            }
          } catch (err) {
            console.error(`FETCH ERROR ${token}:`, err);
          }

          tokenCount += 1;
        }
        console.log(
          `[CHAIN ${chainId}] valid TOKENS: ${validChainTokens.length}/${res?.result?.data?.length}`
        );
        validChainModules[chainId] = validChainTokens;
        invalidChainModules[chainId] = invalidChainTokens;
      } else {
        console.error(`ERROR on chain ${chainId} `, res);
      }
    } catch (err) {
      console.error(`ERROR on chain ${chainId} `);
      console.log(err);
      const alreadyExists = await ddbClient.send(
        new GetCommand({
          TableName: CACHE_TABLE,
          Key: { chainId: chainId.toString() },
        })
      );
      const tokens = parse(alreadyExists?.Item?.tokens || "[]");
      validChainModules[chainId] = tokens;
    }

    await sleep(1000);
  }

  console.log(`validModules:`, validChainModules);
  console.log(`invalidModules:`, invalidChainModules);
  return validChainModules;
};

const allKadenaTokenUpdate = async () => {
  const allTokens = await getAllKadenaTokens();
  for (const chainId of Object.keys(allTokens)) {
    const item = {
      TableName: CACHE_TABLE,
      Item: {
        chainId,
        tokens: stringify(allTokens[chainId]),
        lastUpdate: new Date().toISOString(),
      },
    };
    console.log("UPLOADING TOKENS ON CHAIN " + chainId);
    await ddbClient.send(new PutCommand(item));
  }
};

module.exports = allKadenaTokenUpdate;

/**
 aws dynamodb create-table 
    --table-name kadena-tokens-table 
    --attribute-definitions AttributeName=chainId,AttributeType=S 
    --key-schema AttributeName=chainId,KeyType=HASH 
    --provisioned-throughput ReadCapacityUnits=1,WriteCapacityUnits=1 
    --endpoint-url http://localhost:8000

 */
