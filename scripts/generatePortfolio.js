import fs from "fs";
import fetch from "node-fetch";

// CONFIG
const SHEET_URL = "https://docs.google.com/spreadsheets/d/e/2PACX-1vQA35OG6zgTd-2p5JmlVE5Yb020ww8sRpvivO3DP4cQaXzKg1CpN6TQSRFsfsB2v6_bi9tWOyxGweQZ/pub?gid=0&single=true&output=csv";
const RISK_FREE_RATE = 0.10;

// Black-Scholes
function normCDF(x) {
  return (1 + Math.erf(x / Math.sqrt(2))) / 2;
}

function blackScholes(S, K, T, r, sigma, type) {
  const d1 =
    (Math.log(S / K) + (r + (sigma * sigma) / 2) * T) /
    (sigma * Math.sqrt(T));
  const d2 = d1 - sigma * Math.sqrt(T);

  if (type === "call") {
    return S * normCDF(d1) - K * Math.exp(-r * T) * normCDF(d2);
  } else {
    return K * Math.exp(-r * T) * normCDF(-d2) - S * normCDF(-d1);
  }
}

// CSV parser
function parseCSV(text) {
  const rows = text.trim().split("\n").map(r => r.split(","));
  const headers = rows[0];

  return rows.slice(1).map(row => {
    const obj = {};
    headers.forEach((h, i) => {
      obj[h.trim()] = row[i];
    });
    return obj;
  });
}

// MAIN
async function main() {
  try {
    const res = await fetch(SHEET_URL);
    const text = await res.text();

    const rows = parseCSV(text);

    const latestSpot = Number(rows[rows.length - 1].spot);

    const clients = rows.map(row => {
      const S = latestSpot;

      const K_put = Number(row.putStrike);
      const K_call = Number(row.callStrike);

      const sigma_put = Number(row.putIV);
      const sigma_call = Number(row.callIV);

      const qty = Number(row.quantity);

      const spotBuy = Number(row.spotBuy);
      const putBuy = Number(row.putBuy);
      const callSell = Number(row.callSell);

      const today = new Date();
      const expiry = new Date(today.getFullYear(), 11, 31);
      const T = (expiry - today) / (365 * 24 * 60 * 60 * 1000);

      const putNow = blackScholes(S, K_put, T, RISK_FREE_RATE, sigma_put, "put");
      const callNow = blackScholes(S, K_call, T, RISK_FREE_RATE, sigma_call, "call");

      const currentValue = (S + putNow - callNow) * qty;
      const initialValue = (spotBuy + putBuy - callSell) * qty;
      const pnl = currentValue - initialValue;

      return {
        clientId: row.clientId,
        password: row.password,

        spot: Math.round(S),
        putNow: Math.round(putNow),
        callNow: Math.round(callNow),

        spotBuy,
        putBuy,
        callSell,

        quantity: qty,

        currentValue: Math.round(currentValue),
        initialValue: Math.round(initialValue),
        pnl: Math.round(pnl)
      };
    });

    const output = {
      lastUpdated: new Date().toISOString(),
      clients
    };

    fs.writeFileSync("data/clients.json", JSON.stringify(output, null, 2));

    console.log("Portfolio updated");
  } catch (err) {
    console.error("Error, keeping old data", err);
    process.exit(0);
  }
}

main();
