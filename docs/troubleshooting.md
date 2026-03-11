# Troubleshooting

## The App Starts But I See Oanda Polling Logs

`Using polling market data for Oanda` is informational in this repo, not an error. Oanda is intentionally using polling in the current application flow.

## Detached Chart Opens Blank

Detached chart rendering was hardened, but if one still looks blank:

1. confirm the chart had data before detaching
2. wait briefly for fresh candle reload
3. reopen the symbol and switch timeframe
4. confirm the symbol is actually returning candles from the connected broker

## No Orderbook Or Heatmap Is Visible

Check these items:
- confirm the symbol is open in a chart
- confirm the broker supports orderbook or depth for that symbol
- wait for the orderbook refresh timer
- verify that bids and asks are actually being returned

## No AI Signals Or AI Trading Looks Idle

Check these items:
- confirm AI trading is enabled
- confirm the selected scope includes the symbol you expect
- confirm the strategy has enough candle history to compute features
- check AI Signal Monitor, Recommendations, and logs for `HOLD` or filtered signals
- verify the behavior guard did not block trading

## Manual Trade Ticket Values Look Wrong

Check:
- whether the broker metadata for the symbol is available yet
- whether amount precision or minimum size is stricter than expected
- whether SL and TP were auto-suggested and then manually overwritten
- whether you are switching between brokers with different lot or precision rules

## Order Was Rejected

Common reasons include:
- insufficient funds or margin
- broker minimum size or precision mismatch
- invalid order type or unsupported venue path
- behavior guard block
- live safety lock or kill switch state

## Telegram Is Not Responding

Check:
- Telegram enabled in settings
- bot token and chat ID configured
- the bot is messaging the expected chat
- network access is available
- use `/help` or `/commands` to restore the keyboard

## OpenAI Or Voice Reply Is Not Working

Check:
- OpenAI API key set in Settings -> Integrations
- OpenAI model set correctly
- if using OpenAI speech, speech provider is set to `OpenAI`
- if using Google recognition, optional recognition packages are installed
- for Windows speech, test another installed voice if the current one sounds poor or fails
- if using OpenAI speech, confirm the OpenAI key works from `Settings -> Integrations -> Test OpenAI`

## DNS / Network Errors During Login

If you see DNS lookup failures or `Cannot connect to host` errors:

- check internet connectivity
- check VPN, proxy, or firewall behavior
- confirm the broker host resolves from the machine
- retry after validating Windows DNS configuration

## qasync Timer KeyError Or Async UI Noise

The repo includes hardening for known qasync timer cleanup races, but if you still see repeated async tracebacks, restart the app and capture the first traceback after restart. That is usually the useful one.

## Trade Log, Open Orders, Or Positions Look Wrong

Check:
- whether the broker supports the relevant fetch path
- whether source data is still pending or open instead of terminal
- whether the session is paper, practice, or live
- whether the journal or analytics window is showing merged local plus broker history rather than only one source

## Chart Trading Or Trade-Level Sync Feels Broken

Check:
- whether the manual trade ticket is still open for that symbol
- whether the chart was detached and then reattached while the ticket was active
- whether entry, SL, and TP values were normalized to broker precision after editing
- whether the symbol in the ticket matches the symbol in the active chart
