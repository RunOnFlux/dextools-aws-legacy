const { PutCommand, GetCommand } = require('@aws-sdk/lib-dynamodb');
const { DynamoDBClient } = require('@aws-sdk/client-dynamodb');
const { stringify, parse } = require('zipson');
const { getStoredKadenaTokensByChain } = require('./allKadenaTokenUpdate');
const { makePactCall, isValidToken } = require('../helpers/pact');
const {
  sleep,
  constants: { KADENA_CHAINS_COUNT },
} = require('../helpers');

const ACCOUNTS_CHUNK_SIZE = 10;

const accounts = [
  'k:2e6a7275e51156c4e641c377bc8157fc057d20698b83f30ad2f1828a8b74940e',
  // 'k:20c8efe9a8ced11c93bd01d5487b466aad18ceec96fd0b9a8dbb54cb8a4ffb43',
  // 'k:20c8efe9a8ced11c93bd01d5487b466aad18ceec96fd0b9a8dbb54cb8a4ffb41',
  // 'k:20c8efe9a8ced11c93bd01d5487b466aad18ceec96fd0b9a8dbb54cb8a4ffb45',
  // 'k:20c8efe9a8ced11c93bd01d5487b466aad18ceec96fd0b9a8dbb54cb8a4ffb47',
  // 'k:20c8efe9a8ced11c93bd01d5487b466aad18ceec96fd0b9a8dbb54cb8a4ffb49',
];

const CACHE_TABLE = process.env.KADENA_ACCOUNTS_TABLE || 'kadena-accounts';
const ddbClient = new DynamoDBClient({
  region: 'us-east-1',
  endpoint: process.env.AWS_ENDPOINT || undefined,
});

const updateAccountsBalance = async () => {
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
      console.log(res?.result?.data ?? res?.result?.error?.message);
      await sleep(1000);
    } catch (err) {
      console.error(`ERROR on chain ${chainId} `);
      console.log(err);
    }
  }
};

module.exports = updateAccountsBalance;

/**
 aws dynamodb create-table --table-name kadena-accounts --attribute-definitions AttributeName=account,AttributeType=S --key-schema AttributeName=account,KeyType=HASH --provisioned-throughput ReadCapacityUnits=1,WriteCapacityUnits=1 --endpoint-url http://localhost:8000
 aws dynamodb create-table --table-name kadena-accounts-balance --attribute-definitions AttributeName=account,AttributeType=S AttributeName=date,AttributeType=S --key-schema AttributeName=account,KeyType=HASH AttributeName=date,KeyType=RANGE --provisioned-throughput ReadCapacityUnits=1,WriteCapacityUnits=1 --endpoint-url http://localhost:8000

 */
