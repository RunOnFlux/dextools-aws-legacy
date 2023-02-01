"use strict";

const allTokenUpdate = require("./src/allTokenUpdate");
const allTokenUpdateHandler = async (event) => {
  await allTokenUpdate();
};

module.exports = {
  allTokenUpdateHandler,
};
