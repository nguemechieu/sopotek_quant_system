export type BrokerType = "crypto" | "forex" | "stocks" | "options" | "futures" | "derivatives" | "paper";
export type CustomerRegion = "us" | "global";
export type MarketType = "auto" | "spot" | "derivative" | "option" | "otc";
export type UserWorkspaceRole = "admin" | "trader" | "viewer";

export type SolanaWorkspaceSettings = {
  wallet_address: string;
  private_key: string;
  rpc_url: string;
  jupiter_api_key: string;
  okx_api_key: string;
  okx_secret: string;
  okx_passphrase: string;
  okx_project_id: string;
};

export type WorkspaceSettings = {
  language: string;
  broker_type: BrokerType;
  exchange: string;
  customer_region: CustomerRegion;
  mode: "live" | "paper";
  market_type: MarketType;
  ibkr_connection_mode: "webapi" | "tws";
  ibkr_environment: "gateway" | "hosted";
  schwab_environment: "sandbox" | "production";
  api_key: string;
  secret: string;
  password: string;
  account_id: string;
  risk_percent: number;
  remember_profile: boolean;
  solana: SolanaWorkspaceSettings;
};

export type WorkspaceSettingsResponse = WorkspaceSettings & {
  created_at: string | null;
  updated_at: string | null;
};

export const BROKER_TYPE_OPTIONS: { label: string; value: BrokerType }[] = [
  { label: "Crypto", value: "crypto" },
  { label: "Forex", value: "forex" },
  { label: "Stocks", value: "stocks" },
  { label: "Options", value: "options" },
  { label: "Futures", value: "futures" },
  { label: "Derivatives", value: "derivatives" },
  { label: "Paper", value: "paper" }
];

export const CUSTOMER_REGION_OPTIONS: { label: string; value: CustomerRegion }[] = [
  { label: "US", value: "us" },
  { label: "Outside US", value: "global" }
];

export const MARKET_VENUE_OPTIONS: { label: string; value: MarketType }[] = [
  { label: "Auto", value: "auto" },
  { label: "Spot", value: "spot" },
  { label: "Derivative", value: "derivative" },
  { label: "Options", value: "option" },
  { label: "OTC", value: "otc" }
];

export const IBKR_CONNECTION_OPTIONS = [
  { label: "Web API", value: "webapi" as const },
  { label: "TWS / IB Gateway", value: "tws" as const }
];

export const IBKR_ENVIRONMENT_OPTIONS = [
  { label: "Client Portal Gateway", value: "gateway" as const },
  { label: "Hosted Web API", value: "hosted" as const }
];

export const SCHWAB_ENVIRONMENT_OPTIONS = [
  { label: "Sandbox", value: "sandbox" as const },
  { label: "Production", value: "production" as const }
];

const CRYPTO_EXCHANGE_MAP: Record<CustomerRegion, string[]> = {
  us: ["binanceus", "coinbase", "solana", "stellar", "kraken", "kucoin", "bybit", "okx", "gateio", "bitget"],
  global: ["binance", "coinbase", "solana", "stellar", "kraken", "kucoin", "bybit", "okx", "gateio", "bitget"]
};

const EXCHANGE_MAP: Record<BrokerType, string[]> = {
  crypto: [],
  forex: ["oanda"],
  stocks: ["alpaca"],
  options: ["schwab"],
  futures: ["ibkr", "amp", "tradovate"],
  derivatives: ["ibkr", "schwab", "amp", "tradovate"],
  paper: ["paper"]
};

export function defaultWorkspaceSettings(): WorkspaceSettings {
  return {
    language: "en",
    broker_type: "paper",
    exchange: "paper",
    customer_region: "us",
    mode: "paper",
    market_type: "auto",
    ibkr_connection_mode: "webapi",
    ibkr_environment: "gateway",
    schwab_environment: "sandbox",
    api_key: "",
    secret: "",
    password: "",
    account_id: "",
    risk_percent: 2,
    remember_profile: true,
    solana: {
      wallet_address: "",
      private_key: "",
      rpc_url: "",
      jupiter_api_key: "",
      okx_api_key: "",
      okx_secret: "",
      okx_passphrase: "",
      okx_project_id: ""
    }
  };
}

export function exchangeOptionsFor(brokerType: BrokerType, customerRegion: CustomerRegion): string[] {
  if (brokerType === "crypto") {
    return [...CRYPTO_EXCHANGE_MAP[customerRegion]];
  }
  return [...(EXCHANGE_MAP[brokerType] ?? [])];
}

export function marketVenueOptionsFor(brokerType: BrokerType, exchange: string): MarketType[] {
  const normalizedExchange = (exchange || "").trim().toLowerCase();

  if (normalizedExchange === "paper" || brokerType === "paper") {
    return ["auto", "spot", "derivative", "option", "otc"];
  }
  if (normalizedExchange === "coinbase") {
    return ["auto", "spot", "derivative"];
  }
  if (["stellar", "solana", "alpaca", "binanceus"].includes(normalizedExchange)) {
    return ["auto", "spot"];
  }
  if (normalizedExchange === "oanda" || brokerType === "forex") {
    return ["auto", "otc"];
  }
  if (normalizedExchange === "ibkr") {
    return ["auto", "derivative", "option"];
  }
  if (normalizedExchange === "schwab" || brokerType === "options") {
    return ["auto", "option"];
  }
  if (["amp", "tradovate"].includes(normalizedExchange) || brokerType === "futures") {
    return ["auto", "derivative"];
  }
  if (brokerType === "derivatives") {
    return ["auto", "derivative", "option"];
  }
  if (brokerType === "crypto") {
    return ["auto", "spot", "derivative", "option"];
  }
  return ["auto", "spot"];
}

export function normalizeWorkspaceSettings(input: Partial<WorkspaceSettings>): WorkspaceSettings {
  const defaults = defaultWorkspaceSettings();
  const merged: WorkspaceSettings = {
    ...defaults,
    ...input,
    exchange: String(input.exchange ?? defaults.exchange).trim().toLowerCase() || defaults.exchange,
    language: String(input.language ?? defaults.language).trim() || defaults.language,
    api_key: String(input.api_key ?? defaults.api_key).trim(),
    secret: String(input.secret ?? defaults.secret).trim(),
    password: String(input.password ?? defaults.password).trim(),
    account_id: String(input.account_id ?? defaults.account_id).trim(),
    risk_percent: Math.max(1, Math.min(100, Number(input.risk_percent ?? defaults.risk_percent) || defaults.risk_percent)),
    solana: {
      ...defaults.solana,
      ...(input.solana ?? {})
    }
  };

  const availableExchanges = exchangeOptionsFor(merged.broker_type, merged.customer_region);
  if (merged.broker_type === "paper") {
    merged.broker_type = "paper";
    merged.exchange = "paper";
    merged.mode = "paper";
  } else if (merged.exchange === "paper" || !availableExchanges.includes(merged.exchange)) {
    merged.exchange = availableExchanges[0] ?? "paper";
  }

  const allowedVenues = marketVenueOptionsFor(merged.broker_type, merged.exchange);
  if (!allowedVenues.includes(merged.market_type)) {
    merged.market_type = allowedVenues[0] ?? "auto";
  }

  return merged;
}

export function workspaceCredentialsReady(settings: WorkspaceSettings): boolean {
  if (settings.broker_type === "paper" || settings.exchange === "paper") {
    return true;
  }
  if (settings.exchange === "solana") {
    const hasWallet = Boolean(settings.solana.wallet_address && settings.solana.private_key);
    const hasOkx = Boolean(settings.solana.okx_api_key && settings.solana.okx_secret && settings.solana.okx_passphrase);
    const hasLegacyJupiter = Boolean(settings.solana.jupiter_api_key);
    return hasWallet || hasOkx || hasLegacyJupiter;
  }
  if (settings.exchange === "oanda") {
    return Boolean(settings.account_id && settings.api_key);
  }
  if (settings.exchange === "schwab") {
    return Boolean(settings.api_key && settings.password);
  }
  if (settings.exchange === "ibkr") {
    return true;
  }
  return Boolean(settings.api_key && settings.secret);
}

export function applyWorkspacePreset(settings: WorkspaceSettings, preset: "paper" | "crypto" | "forex"): WorkspaceSettings {
  if (preset === "paper") {
    return normalizeWorkspaceSettings({
      ...settings,
      broker_type: "paper",
      exchange: "paper",
      mode: "paper",
      market_type: "auto",
      risk_percent: 2
    });
  }
  if (preset === "crypto") {
    const preferredExchange = settings.customer_region === "global" ? "binance" : "binanceus";
    return normalizeWorkspaceSettings({
      ...settings,
      broker_type: "crypto",
      exchange: preferredExchange,
      mode: "live",
      market_type: "spot",
      risk_percent: 2
    });
  }
  return normalizeWorkspaceSettings({
    ...settings,
    broker_type: "forex",
    exchange: "oanda",
    mode: "live",
    market_type: "otc",
    risk_percent: 1
  });
}

export function workspaceBrokerHint(settings: WorkspaceSettings): string {
  if (settings.broker_type === "paper" || settings.exchange === "paper") {
    return "Paper mode keeps execution simulated while preserving the real exchange context for symbols and market data.";
  }
  if (settings.exchange === "solana") {
    return "Use wallet signing for live swaps, and add Jupiter or OKX routing credentials when you want Solana quotes and route assembly.";
  }
  if (settings.exchange === "oanda") {
    return "OANDA live routing needs both the account id and API key before the platform can bind the desk to a real FX account.";
  }
  if (settings.exchange === "schwab") {
    return "Schwab requires an app key plus redirect URI, then the session can complete through the OAuth sign-in flow.";
  }
  if (settings.exchange === "ibkr") {
    return "IBKR can route through Web API or TWS/Gateway, so keep the connection mode aligned with the runtime you actually run.";
  }
  return `Configure ${settings.exchange.toUpperCase()} credentials and risk before the trading workspace starts using this account.`;
}
