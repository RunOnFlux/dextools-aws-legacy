"use strict";
const { Client } = require("pg");

const allTokenUpdate = require("./src/allTokenUpdate");
const allTokenUpdateHandler = async (event) => {
  await allTokenUpdate();
};

const kdaPriceUpdate = require("./src/kdaPriceUpdate");
const kdaPriceUpdateHandler = async (event) => {
  const client = new Client();
  await client.connect();
  const s = await client.query("SELECT 1+1 AS sum");
  console.log(s);
  await kdaPriceUpdate(client);
};

module.exports = {
  allTokenUpdateHandler,
  kdaPriceUpdateHandler
};
