import fetch from "node-fetch";
import { mkdir, readFile, writeFile } from "node:fs/promises";
import path from "node:path";
import { fileURLToPath } from "node:url";

const __filename = fileURLToPath(import.meta.url);
const __dirname = path.dirname(__filename);
const OUTPUT_PATH = path.resolve(__dirname, "../data/clients.json");
const SHEET_URL = process.env.GOOGLE_SHEET_CSV_URL;
const DEFAULT_RISK_FREE_RATE = 0.05;
const DEFAULT_TIME_TO_EXPIRY_YEARS = 30 / 365;
const OUTPUT_DECIMALS = 6;
const INDIA_EXPIRY_TIME_SUFFIX = "T15:30:00+05:30";

function normalizeHeader(value) {
  return String(value ?? "")
    .trim()
    .toLowerCase()
    .replace(/[^a-z0-9]+/g, "");
}

function getStringValue(row, keys, fallback = "") {
  for (const key of keys) {
    const value = row[normalizeHeader(key)];
    if (value !== undefined && value !== null && String(value).trim() !== "") {
      return String(value).trim();
    }
  }
  return fallback;
}

function getNumberValue(row, keys, fallback = NaN) {
  const raw = getStringValue(row, keys, "");
  if (!raw) {
    return fallback;
  }

  const sanitized = raw.replace(/,/g, "").trim();
  const value = Number(sanitized);
  return Number.isFinite(value) ? value : fallback;
}

function roundNumber(value, decimals = OUTPUT_DECIMALS) {
  if (!Number.isFinite(value)) {
    return 0;
  }
  return Number(value.toFixed(decimals));
}

function erf(x) {
  const sign = x < 0 ? -1 : 1;
  const absoluteX = Math.abs(x);
  const a1 = 0.254829592;
  const a2 = -0.284496736;
  const a3 = 1.421413741;
  const a4 = -1.453152027;
  const a5 = 1.061405429;
  const p = 0.3275911;

  const t = 1 / (1 + p * absoluteX);
  const y =
    1 -
    (((((a5 * t + a4) * t + a3) * t + a2) * t + a1) *
      t *
      Math.exp(-absoluteX * absoluteX));

  return sign * y;
}

function normalCdf(x) {
  return 0.5 * (1 + erf(x / Math.SQRT2));
}

function blackScholes({ spot, strike, rate, volatility, timeToExpiry, optionType }) {
  if (
    !Number.isFinite(spot) ||
    !Number.isFinite(strike) ||
    !Number.isFinite(rate) ||
    !Number.isFinite(volatility) ||
    !Number.isFinite(timeToExpiry) ||
    spot <= 0 ||
    strike <= 0 ||
    volatility <= 0 ||
    timeToExpiry <= 0
  ) {
    return 0;
  }

  const sqrtT = Math.sqrt(timeToExpiry);
  const sigmaSqrtT = volatility * sqrtT;
  const d1 =
    (Math.log(spot / strike) + (rate + (volatility * volatility) / 2) * timeToExpiry) /
    sigmaSqrtT;
  const d2 = d1 - sigmaSqrtT;
  const discountedStrike = strike * Math.exp(-rate * timeToExpiry);

  if (optionType === "put") {
    return discountedStrike * normalCdf(-d2) - spot * normalCdf(-d1);
  }

  return spot * normalCdf(d1) - discountedStrike * normalCdf(d2);
}

function parseCsv(csvText) {
  const rows = [];
  let currentValue = "";
  let currentRow = [];
  let inQuotes = false;

  for (let index = 0; index < csvText.length; index += 1) {
    const char = csvText[index];
    const nextChar = csvText[index + 1];

    if (char === '"') {
      if (inQuotes && nextChar === '"') {
        currentValue += '"';
        index += 1;
      } else {
        inQuotes = !inQuotes;
      }
      continue;
    }

    if (char === "," && !inQuotes) {
      currentRow.push(currentValue);
      currentValue = "";
      continue;
    }

    if ((char === "\n" || char === "\r") && !inQuotes) {
      if (char === "\r" && nextChar === "\n") {
        index += 1;
      }
      currentRow.push(currentValue);
      rows.push(currentRow);
      currentRow = [];
      currentValue = "";
      continue;
    }

    currentValue += char;
  }

  if (currentValue.length > 0 || currentRow.length > 0) {
    currentRow.push(currentValue);
    rows.push(currentRow);
  }

  if (rows.length === 0) {
    return [];
  }

  const headers = rows[0].map((header) => normalizeHeader(header));
  const records = [];

  for (const values of rows.slice(1)) {
    if (values.every((value) => String(value ?? "").trim() === "")) {
      continue;
    }

    const record = {};
    headers.forEach((header, index) => {
      record[header] = values[index] ?? "";
    });
    records.push(record);
  }

  return records;
}

function parseExpiryDateValue(expiryDateRaw) {
  const normalized = String(expiryDateRaw ?? "").trim();
  if (!normalized) {
    return null;
  }

  const parsedDate = /^\d{4}-\d{2}-\d{2}$/.test(normalized)
    ? new Date(`${normalized}${INDIA_EXPIRY_TIME_SUFFIX}`)
    : new Date(normalized);

  return Number.isNaN(parsedDate.getTime()) ? null : parsedDate;
}

function getTimeToExpiry(row) {
  const timeToExpiryYears = getNumberValue(row, [
    "timeToExpiryYears",
    "timetoexpiryyears",
    "timeToExpiry",
    "timetoexpiry"
  ]);
  if (Number.isFinite(timeToExpiryYears) && timeToExpiryYears > 0) {
    return timeToExpiryYears;
  }

  const daysToExpiry = getNumberValue(row, ["daysToExpiry", "daystoexpiry", "expiryDays"]);
  if (Number.isFinite(daysToExpiry) && daysToExpiry > 0) {
    return daysToExpiry / 365;
  }

  const expiryDateRaw = getStringValue(row, ["expiryDate", "expiry"]);
  if (expiryDateRaw) {
    const expiryDate = parseExpiryDateValue(expiryDateRaw);
    if (expiryDate) {
      const milliseconds = expiryDate.getTime() - Date.now();
      const days = milliseconds / (1000 * 60 * 60 * 24);
      if (days > 0) {
        return days / 365;
      }
    }
  }

  return DEFAULT_TIME_TO_EXPIRY_YEARS;
}

function buildClientRecord(row) {
  const clientId = getStringValue(row, ["clientId", "client", "clientCode"]);
  const password = getStringValue(row, ["password", "passcode"]);
  const spotBuy = getNumberValue(row, ["spotBuy", "spotbuy"]);
  const putBuy = getNumberValue(row, ["putBuy", "putbuy"]);
  const callSell = getNumberValue(row, ["callSell", "callsell"]);
  const putStrike = getNumberValue(row, ["putStrike", "putstrike"]);
  const callStrike = getNumberValue(row, ["callStrike", "callstrike"]);
  const putIV = getNumberValue(row, ["putIV", "putiv"]);
  const callIV = getNumberValue(row, ["callIV", "calliv"]);
  const quantity = getNumberValue(row, ["quantity", "qty"]);
  const spot = getNumberValue(row, ["latestSpot", "latest spot", "spotNow", "spot"]);
  const rate = getNumberValue(row, ["riskFreeRate", "riskfreerate", "rate"], DEFAULT_RISK_FREE_RATE);
  const expiryDateRaw = getStringValue(row, ["expiryDate", "expiry"]);
  const expiryDate = parseExpiryDateValue(expiryDateRaw);
  const timeToExpiry = getTimeToExpiry(row);

  const requiredValues = [spotBuy, putBuy, callSell, putStrike, callStrike, putIV, callIV, quantity, spot];
  const hasInvalidValue = requiredValues.some((value) => !Number.isFinite(value));

  if (!clientId || hasInvalidValue || quantity <= 0 || spot <= 0 || putStrike <= 0 || callStrike <= 0) {
    return null;
  }

  const putNow = blackScholes({
    spot,
    strike: putStrike,
    rate,
    volatility: putIV > 1 ? putIV / 100 : putIV,
    timeToExpiry,
    optionType: "put"
  });

  const callNow = blackScholes({
    spot,
    strike: callStrike,
    rate,
    volatility: callIV > 1 ? callIV / 100 : callIV,
    timeToExpiry,
    optionType: "call"
  });

  const currentValue = (spot + putNow - callNow) * quantity;
  const initialValue = (spotBuy + putBuy - callSell) * quantity;
  const pnl = currentValue - initialValue;

  return {
    clientId,
    password,
    spotBuy: roundNumber(spotBuy),
    putBuy: roundNumber(putBuy),
    callSell: roundNumber(callSell),
    putStrike: roundNumber(putStrike),
    callStrike: roundNumber(callStrike),
    putIV: roundNumber(putIV),
    callIV: roundNumber(callIV),
    expiryDate: expiryDate ? expiryDate.toISOString() : expiryDateRaw,
    timeToExpiryYears: roundNumber(timeToExpiry),
    currentValue: roundNumber(currentValue),
    initialValue: roundNumber(initialValue),
    pnl: roundNumber(pnl),
    spot: roundNumber(spot),
    putNow: roundNumber(putNow),
    callNow: roundNumber(callNow),
    quantity: roundNumber(quantity)
  };
}

async function readExistingJson() {
  try {
    const existingContent = await readFile(OUTPUT_PATH, "utf8");
    return JSON.parse(existingContent);
  } catch {
    return { lastUpdated: "", clients: [] };
  }
}

async function writePortfolio(clients) {
  const output = {
    lastUpdated: new Date().toISOString(),
    clients
  };

  await mkdir(path.dirname(OUTPUT_PATH), { recursive: true });
  await writeFile(OUTPUT_PATH, `${JSON.stringify(output, null, 2)}\n`, "utf8");
}

async function main() {
  if (!SHEET_URL) {
    console.error("GOOGLE_SHEET_CSV_URL is not set. Skipping update.");
    process.exitCode = 1;
    return;
  }

  const existingJson = await readExistingJson();

  let response;
  try {
    response = await fetch(SHEET_URL, {
      headers: {
        Accept: "text/csv"
      }
    });
  } catch (error) {
    console.error("Failed to fetch CSV:", error);
    process.exitCode = 1;
    return;
  }

  if (!response.ok) {
    console.error(`Failed to fetch CSV: ${response.status} ${response.statusText}`);
    process.exitCode = 1;
    return;
  }

  const csvText = await response.text();
  const rows = parseCsv(csvText);
  const clients = rows.map(buildClientRecord).filter(Boolean);

  if (clients.length === 0) {
    console.warn("No valid portfolio rows found. Skipping JSON update.");
    const unchangedOutput = {
      lastUpdated: existingJson.lastUpdated || "",
      clients: Array.isArray(existingJson.clients) ? existingJson.clients : []
    };
    console.log(JSON.stringify(unchangedOutput, null, 2));
    return;
  }

  await writePortfolio(clients);
  console.log(`Updated portfolio JSON for ${clients.length} client(s).`);
}

main().catch((error) => {
  console.error("Portfolio generation failed:", error);
  process.exitCode = 1;
});
