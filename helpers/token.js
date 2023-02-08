const { GraphQLClient, gql } = require("graphql-request");
const { default: axios } = require("axios");
const { DateTime } = require("luxon");
const { makePactCall } = require("./pact");

const { stringify } = require("zipson/lib");
const tokenInfos = require("../tokenInfo.json")

const CDN_PATH = "https://cdn2.kadefi.money";

const getKDSwapCS = async (token) => {
  const gClient = new GraphQLClient(
    "https://kdswap-fd-prod-cpeabrdfgdg9hzen.z01.azurefd.net/graphql/graphql"
  );
  const q = gql`
    query getTokenStats($token: String!) {
      stats(token: $token) {
        circulatingSupply
        totalSupply
        tokenName
      }
    }
  `;
  const data = await gClient.request(q, { token });
  return parseFloat(data.stats.circulatingSupply);
};

const getKISHKBurns = async () => {
  const dataR = await axios.get(
    "https://data.kadefi.money/balance/free.kishu-ken_token-table/k:0000000000000000000000000000000000000000000000000000000000000000"
  );
  return dataR.data.balance;
};

const getKDXBurns = async () => {
  const todayDate = DateTime.now().toFormat("yyyy-LL-dd");
  const dataR = await axios.get(
    `https://analytics-api.kaddex.com/analytics/get-data?dateStart=${todayDate}&dateEnd=${todayDate}`
  );
  const burns = dataR.data[0].burn;
  return Object.keys(burns).reduce((p, key) => {
    p += burns[key];
    return p;
  }, 0);
};

const getKDXCS = async () => {
  const todayDate = DateTime.now().toFormat("yyyy-LL-dd");
  const dataR = await axios.get(
    `https://analytics-api.kaddex.com/analytics/get-data?dateStart=${todayDate}&dateEnd=${todayDate}`
  );
  return dataR.data[0].circulatingSupply.totalSupply;
};

const getFluxCS = async () => {
  const dataR = await axios.get(
    "https://explorer.runonflux.io/api/circulating-supply"
  );
  return dataR.data;
};

const getReserve = (tokenData) => {
  return parseFloat(tokenData.decimal ? tokenData.decimal : tokenData);
};

const getWIZACS = async () => {
  const d = await makePactCall("1", `(free.wiza.get-circulating-supply)`);
  if (d.result && d.result.status === "success") {
    return getReserve(d.result.data);
  }
  throw new Error("failed wiza");
};

const getCirculatingSupply = async () => {
  const [kdl, kds, kdx, flux, wiza] = await Promise.all([
    getKDSwapCS("kdlaunch.token"),
    getKDSwapCS("kdlaunch.kdswap-token"),
    getKDXCS(),
    getFluxCS(),
    getWIZACS(),
  ]);

  const circulatingSupply = {
    KDL: kdl,
    KDS: kds,
    KDX: kdx,
    FLUX: flux,
    WIZA: wiza,
    KISHK: -1,
  };

  return circulatingSupply;
};

const getTotalReductions = async () => {
  const [KDX, KISHK] = await Promise.all([getKDXBurns(), getKISHKBurns()]);
  return {
    KDX,
    KISHK,
  };
};

const addTokenInfo = async (tokenMap) => {
  try {
    console.log("gettting data");
    const [cs, red] = await Promise.all([
      getCirculatingSupply(),
      getTotalReductions(),
    ]);

    const tokenWithInfos = Object.keys(tokenInfos).reduce((p,c) => {
      const tokenInfo = tokenInfos[c];
      const totalSupply = red[c] ? tokenInfo.totalSupply - red[c] : tokenInfo.totalSupply;
      const circulatingSupply = cs[c] === -1 ? totalSupply : cs[c] ? cs[c] : null;
      const tempToken = Object.assign({}, tokenInfo);
      const tokenWithCorrectTs = Object.assign(tempToken, { totalSupply, circulatingSupply });
      p[c] = tokenWithCorrectTs;
      return p;
    }, {})

    const tokens = Object.keys(tokenMap).reduce((p,c) => {
      const token = tokenMap[c];
      if(!(token.symbol in tokenWithInfos)) {
        return p;
      }
      p[c] = {
        ...token,
        ...tokenWithInfos[token.symbol]
      }
      return p;
    }, {});

    return tokens;
  } catch (e) {
    console.log(e.message);
  }
};

module.exports = {
  addTokenInfo
};
