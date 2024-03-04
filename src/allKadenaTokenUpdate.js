const { PutCommand, GetCommand } = require('@aws-sdk/lib-dynamodb');
const { DynamoDBClient } = require('@aws-sdk/client-dynamodb');
const { stringify, parse } = require('zipson');
const { makePactCall, isValidToken } = require('../helpers/pact');
const {
  sleep,
  constants: { KADENA_CHAINS_COUNT },
} = require('../helpers');

const CACHE_TABLE = process.env.KADENA_TOKENS_TABLE || 'kadena-tokens-table';
const ddbClient = new DynamoDBClient({
  region: 'us-east-1',
  endpoint: process.env.AWS_ENDPOINT || undefined,
});

const updateKadenaTokens = async (chainId) => {
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
  const invalidChainTokens = [];
  try {
    const res = await makePactCall(chainId.toString(), pactCode);
    if (res?.result?.data?.length > 0) {
      console.log(`[CHAIN ${chainId}] FOUND ${res?.result?.data?.length} tokens`);
      const storedTokens = await getStoredKadenaTokensByChain(chainId);
      console.log(`[CHAIN ${chainId}] ${storedTokens?.length} tokens already saved`);
      const difference = res?.result?.data?.filter((t) => !storedTokens.includes(t));
      if (difference.length) {
        console.log(`[CHAIN ${chainId}] FOUNDED ${difference.length} new tokens: ${difference.join(', ')}`);
        const validChainTokens = storedTokens;
        let tokenCount = 1;
        for (const token of difference) {
          try {
            const isTokenWorking = await makePactCall(chainId.toString(), `(${token}.get-balance "k:alice")`);
            await sleep(500);
            if (isTokenWorking?.result?.status === 'success' || isTokenWorking?.result?.error?.message?.includes('row not found')) {
              validChainTokens.push(token);
            } else {
              console.error(`[CHAIN ${chainId}] TOKEN ${token} IS NOT VALID`);
              invalidChainTokens.push(token);
            }
          } catch (err) {
            console.error(`FETCH ERROR ${token}:`, err);
          }

          tokenCount += 1;
        }
        console.log(`[CHAIN ${chainId}] invalid TOKENS: ${invalidChainTokens.length}/${res?.result?.data?.length}`);
        const item = {
          TableName: CACHE_TABLE,
          Item: {
            chainId: chainId.toString(),
            tokens: stringify(validChainTokens),
            lastUpdate: new Date().toISOString(),
          },
        };
        console.log('UPLOADING TOKENS ON CHAIN ' + chainId);
        await ddbClient.send(new PutCommand(item));
      }
    } else {
      console.error(`NO TOKENS FOUNDED ON CHAIN ${chainId} `, res);
    }
  } catch (err) {
    console.error(`ERROR FETCHING TOKENS ON CHAIN ${chainId}`);
    console.log(err);
  }
};

const getStoredKadenaTokensByChain = async (chainId) => {
  const res = await ddbClient.send(
    new GetCommand({
      TableName: CACHE_TABLE,
      Key: { chainId: chainId.toString() },
    })
  );
  return res?.Item?.tokens ? parse(res?.Item?.tokens) : [];
};

const allKadenaTokenUpdate = async () => {
  for (let chainId = 0; chainId < KADENA_CHAINS_COUNT; chainId++) {
    await updateKadenaTokens(chainId);
  }
};

module.exports = { allKadenaTokenUpdate, getStoredKadenaTokensByChain };

/**
 aws dynamodb create-table --table-name kadena-tokens-table --attribute-definitions AttributeName=chainId,AttributeType=S --key-schema AttributeName=chainId,KeyType=HASH --provisioned-throughput ReadCapacityUnits=1,WriteCapacityUnits=1 --endpoint-url http://localhost:8000

 */
