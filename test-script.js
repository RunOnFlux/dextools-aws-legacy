require('dotenv').config();
('use strict');
const { Client } = require('pg');

const allTokenUpdate = require('./src/allTokenUpdate');

// allTokenUpdate();

// const { allKadenaTokenUpdate } = require('./src/allKadenaTokenUpdate');
const updateAccountsBalance = require('./src/accountsBalanceUpdate');
// allKadenaTokenUpdate();
updateAccountsBalance();
