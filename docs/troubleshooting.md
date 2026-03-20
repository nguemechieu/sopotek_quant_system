# Troubleshooting

## The App Starts But I See Oanda Polling Logs

`Using polling market data for Oanda` is informational in this repo, not an error. Oanda is intentionally using polling in the current application flow.

## Oanda Says No Candles Were Returned

Check these items:
- restart the app after broker updates so the latest Oanda fallback logic is actually loaded
- confirm the symbol is one Oanda serves on the connected account and pricing division
- switch between `Bid`, `Mid`, and `Ask` candle source if you are matching another platform such as MT4
- try another timeframe to confirm whether the issue is symbol-specific or timeframe-specific
- if the chart still says `No data received.`, capture the exact symbol and timeframe because the app now treats truly empty broker responses honestly instead of inventing filler candles

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

## Recent Trades Tab Stays Empty

Check these items:
- confirm the active broker supports public `fetch_trades(symbol)` for that market
- switch to another symbol to rule out a symbol-specific broker limitation
- wait for the normal order book refresh cycle, because recent trades refresh alongside it
- confirm the connected session still has live ticker data, since the app can only synthesize a fallback feed when quote data is available

## Coinbase Says A Symbol Is Unsupported

Coinbase does not expose every pair-like symbol the rest of the app may know about.

Check these items:
- confirm the symbol exists on the connected Coinbase venue, not just on another broker
- reopen the chart with a Coinbase-native market symbol rather than a forex-style pair such as `EUR/USD`
- refresh markets after login if the symbol list may be stale
- if the symbol is unsupported, the app should now skip order book and recent-trades refreshes instead of raising repeated background task errors

## Depth Chart Or Market Info Looks Blank

Check these items:
- confirm the chart has received candles, because market info uses visible candle context
- confirm the order book has populated first, because depth depends on bid and ask levels
- switch chart tabs and symbols once if you recently detached or reattached the chart window

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

## Chart Shows Loading Forever Or No Data Received

Check:
- whether the broker is returning any candles at all for that symbol and timeframe
- whether the chart asked for more history than the venue actually keeps for that market
- whether the symbol was loaded from another broker session and is stale for the current broker
- whether the shorter-history notice says the broker returned only part of the requested window, which is a real data limitation rather than a drawing bug
- whether malformed rows were dropped during sanitizing, which can happen if the venue sends duplicate timestamps or invalid OHLC values

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
