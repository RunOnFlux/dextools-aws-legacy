require('dotenv').config();

const { allKadenaTokenUpdate } = require('./src/allKadenaTokenUpdate');
const updateAccountsBalance = require('./src/accountsBalanceUpdate');
updateAccountsBalance();
