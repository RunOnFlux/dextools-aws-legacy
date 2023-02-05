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
  await kdaPriceUpdate(client);
};

const candleUpdate = require("./src/candleUpdate");
const candleUpdateHandler = async (event) => {
  const client = new Client();
  await client.connect();
  await candleUpdate(client);
};

module.exports = {
  allTokenUpdateHandler,
  kdaPriceUpdateHandler,
  candleUpdateHandler,
};
