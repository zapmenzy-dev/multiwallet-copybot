# ==================== BALANCE MANAGER ====================
class RobustBalanceManager:
    """
    Robust pUSD balance checker for Polymarket.
    
    Fetch strategies (in order):
    1. Direct RPC calls to Polygon (multiple nodes)
    2. Polymarket Data API fallback
    3. Local cache
    """

    def __init__(self, cache_seconds: int = 60):
        self.cached_balance: float = BANKROLL_FALLBACK
        self.last_update: float = 0.0
        self.cache_seconds = cache_seconds
        self._breakdown: Dict[str, float] = {}
        self._last_successful_source: str = "fallback"

    # ── RPC Helpers ──────────────────────────────────────────────────────────

    def _call_payload(self, contract: str, wallet: str) -> dict:
        padded = wallet.lower().replace("0x", "").zfill(64)
        return {
            "jsonrpc": "2.0",
            "id": 1,
            "method": "eth_call",
            "params": [{"to": contract, "data": BALANCE_OF_SELECTOR + padded}, "latest"],
        }

    async def _query_contract(
        self,
        session: aiohttp.ClientSession,
        rpc_url: str,
        label: str,
        contract: str,
        decimals: int,
        wallet: str,
    ) -> Optional[float]:
        try:
            payload = self._call_payload(contract, wallet)
            async with session.post(
                rpc_url, 
                json=payload, 
                timeout=aiohttp.ClientTimeout(total=8)
            ) as resp:
                
                if resp.status != 200:
                    logging.debug(f"Balance | {label} → HTTP {resp.status} on {rpc_url}")
                    return None

                data = await resp.json(content_type=None)
                
                if "error" in data:
                    logging.debug(f"Balance | {label} → RPC Error: {data['error']}")
                    return None

                hex_val = data.get("result") or "0x0"
                if hex_val in ("0x", "0x0", None):
                    return 0.0

                amount = int(hex_val, 16) / (10 ** decimals)
                if amount > 0:
                    logging.debug(f"Balance | {label} = ${amount:.6f}")
                return amount

        except asyncio.TimeoutError:
            logging.debug(f"Balance | {label} → Timeout on {rpc_url}")
        except Exception as e:
            logging.debug(f"Balance | {label} → Error on {rpc_url}: {e}")
        return None

    async def _fetch_rpc(self, session: aiohttp.ClientSession, wallet: str) -> Optional[float]:
        """Try multiple RPCs until one succeeds."""
        for rpc_url in POLYGON_RPCS:
            total = 0.0
            breakdown: Dict[str, float] = {}
            rpc_alive = False

            for label, contract, decimals in PUSD_CONTRACTS:
                amount = await self._query_contract(
                    session, rpc_url, label, contract, decimals, wallet
                )
                if amount is None:
                    continue

                rpc_alive = True
                if amount > 0:
                    breakdown[label] = amount
                    total += amount

            if rpc_alive:
                self._breakdown = breakdown
                self._last_successful_source = f"RPC ({rpc_url.split('//')[-1][:20]}...)"
                
                if total > 0:
                    parts = ", ".join(f"{k}=${v:.4f}" for k, v in breakdown.items())
                    logging.info(f"Balance → ${total:.4f} ({parts}) via {self._last_successful_source}")
                else:
                    logging.info(f"Balance → $0.00 via {self._last_successful_source}")
                return total

        return None

    async def _fetch_polymarket_api(self, session: aiohttp.ClientSession, wallet: str) -> Optional[float]:
        """Fallback using Polymarket public API."""
        try:
            url = f"https://data-api.polymarket.com/value?user={wallet}"
            async with session.get(url, timeout=aiohttp.ClientTimeout(total=10)) as resp:
                if resp.status != 200:
                    return None

                data = await resp.json(content_type=None)
                
                if isinstance(data, (int, float)):
                    value = float(data)
                elif isinstance(data, dict):
                    value = float(
                        data.get("portfolioValue") or 
                        data.get("value") or 
                        data.get("balance") or 0
                    )
                else:
                    return None

                if value >= 0:
                    self._last_successful_source = "Polymarket API"
                    logging.info(f"Balance → ${value:.4f} via {self._last_successful_source}")
                    self._breakdown = {"polymarket_api": value}
                    return value
        except Exception as e:
            logging.warning(f"Polymarket API balance fetch failed: {e}")

        return None

    # ── Public Methods ───────────────────────────────────────────────────────

    async def get(self, session: aiohttp.ClientSession, force: bool = False) -> float:
        """Get current available pUSD balance."""
        # Return cache if still fresh
        if not force and (time.time() - self.last_update) < self.cache_seconds:
            return self.cached_balance

        if not YOUR_WALLET or len(YOUR_WALLET) != 42:
            logging.warning("DEPOSIT_WALLET_ADDRESS invalid or not set")
            return self.cached_balance

        logging.debug("Fetching fresh balance...")

        fetched = await self._fetch_rpc(session, YOUR_WALLET)
        if fetched is None:
            fetched = await self._fetch_polymarket_api(session, YOUR_WALLET)

        if fetched is not None:
            self.cached_balance = fetched
            self.last_update = time.time()
        else:
            logging.warning(f"Balance fetch failed — using cached value ${self.cached_balance:.4f}")

        return self.cached_balance

    def get_breakdown(self) -> Dict[str, float]:
        """Return detailed balance breakdown (for dashboard/logging)."""
        return self._breakdown.copy()

    def get_last_source(self) -> str:
        """Return which source provided the last successful balance."""
        return self._last_successful_source

    def adjust(self, delta: float) -> None:
        """Adjust cached balance (use carefully - preferably after confirmed fills)."""
        self.cached_balance = max(0.0, self.cached_balance + delta)
        logging.info(f"Balance adjusted by {delta:+.2f} → ${self.cached_balance:.4f}")
