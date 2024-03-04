const { PutCommand, GetCommand, ScanCommand } = require('@aws-sdk/lib-dynamodb');
const { DynamoDBClient } = require('@aws-sdk/client-dynamodb');
const { stringify, parse } = require('zipson');
const { getStoredKadenaTokensByChain } = require('./allKadenaTokenUpdate');
const { makePactCall } = require('../helpers/pact');
const {
  sleep,
  constants: { KADENA_CHAINS_COUNT },
} = require('../helpers');

const ACCOUNTS_CHUNK_SIZE = 2;

const KADENA_ACCOUNTS_TABLE = process.env.KADENA_ACCOUNTS_TABLE || 'kadena-accounts';
const KADENA_ACCOUNTS_BALANCE_TABLE = process.env.KADENA_ACCOUNTS_BALANCE_TABLE || 'kadena-accounts-balance';
const ddbClient = new DynamoDBClient({
  region: 'us-east-1',
  endpoint: process.env.AWS_ENDPOINT || undefined,
});

const updateAccountsBalance = async () => {
  let lastEvaluatedKey = undefined;
  let accountsResponse = [];
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
              dataToPersist[accountString].push({
                chainId,
                balance: res?.result?.data[accountString],
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
          balances: dataToPersist[account],
        },
      };
      await ddbClient.send(new PutCommand(item));
    }
  } while (lastEvaluatedKey && accountsResponse?.Items?.length === ACCOUNTS_CHUNK_SIZE);
};

module.exports = updateAccountsBalance;

/**
 aws dynamodb create-table --table-name kadena-accounts --attribute-definitions AttributeName=account,AttributeType=S --key-schema AttributeName=account,KeyType=HASH --provisioned-throughput ReadCapacityUnits=1,WriteCapacityUnits=1 --endpoint-url http://localhost:8000
 aws dynamodb create-table --table-name kadena-accounts-balance --attribute-definitions AttributeName=account,AttributeType=S AttributeName=date,AttributeType=S --key-schema AttributeName=account,KeyType=HASH AttributeName=date,KeyType=RANGE --provisioned-throughput ReadCapacityUnits=1,WriteCapacityUnits=1 --endpoint-url http://localhost:8000

 */
