from __future__ import annotations

import json
import re
import ssl
from dataclasses import dataclass
from datetime import date, datetime, timezone
from typing import Iterable
from urllib.error import HTTPError, URLError
from urllib.request import Request, urlopen


PINGZHONGDATA_URL = "https://fund.eastmoney.com/pingzhongdata/{code}.js?v={ts}"


@dataclass
class PricePoint:
    price_date: date
    nav: float
    pct_change_vs_prev: float | None


@dataclass
class FundPricePayload:
    fund_code: str
    fund_name: str
    history: list[PricePoint]
    source_name: str = "eastmoney_pingzhongdata"

    @property
    def latest(self) -> PricePoint:
        return self.history[-1]


class PriceProviderError(RuntimeError):
    pass


class EastMoneyPriceProvider:
    def fetch_payload(self, fund_code: str) -> FundPricePayload:
        url = PINGZHONGDATA_URL.format(
            code=fund_code,
            ts=datetime.now(tz=timezone.utc).strftime("%Y%m%d%H%M%S"),
        )
        request = Request(
            url,
            headers={
                "User-Agent": (
                    "Mozilla/5.0 (Macintosh; Intel Mac OS X 10_15_7) "
                    "AppleWebKit/537.36 (KHTML, like Gecko) "
                    "Chrome/123.0 Safari/537.36"
                )
            },
        )

        try:
            with urlopen(request, timeout=20) as response:
                body = response.read().decode("utf-8-sig", errors="replace")
        except ssl.SSLCertVerificationError:
            try:
                insecure_context = ssl._create_unverified_context()
                with urlopen(request, timeout=20, context=insecure_context) as response:
                    body = response.read().decode("utf-8-sig", errors="replace")
            except (HTTPError, URLError, ssl.SSLError) as fallback_exc:
                raise PriceProviderError(f"抓取基金 {fund_code} 净值失败: {fallback_exc}") from fallback_exc
        except (HTTPError, URLError) as exc:
            ssl_error = getattr(exc, "reason", None)
            if isinstance(ssl_error, ssl.SSLCertVerificationError):
                try:
                    insecure_context = ssl._create_unverified_context()
                    with urlopen(request, timeout=20, context=insecure_context) as response:
                        body = response.read().decode("utf-8-sig", errors="replace")
                except (HTTPError, URLError, ssl.SSLError) as fallback_exc:
                    raise PriceProviderError(f"抓取基金 {fund_code} 净值失败: {fallback_exc}") from fallback_exc
            else:
                raise PriceProviderError(f"抓取基金 {fund_code} 净值失败: {exc}") from exc

        fund_name = _extract_string_var(body, "fS_name")
        raw_history = _extract_json_var(body, "Data_netWorthTrend")
        history = _parse_history(raw_history)
        if not history:
            raise PriceProviderError(f"基金 {fund_code} 未返回净值历史")

        return FundPricePayload(
            fund_code=fund_code,
            fund_name=fund_name or fund_code,
            history=history,
        )

    def latest_price(self, fund_code: str) -> FundPricePayload:
        return self.fetch_payload(fund_code)

    def get_price_on_or_before(self, fund_code: str, target_date: date) -> PricePoint | None:
        payload = self.fetch_payload(fund_code)
        candidates = [point for point in payload.history if point.price_date <= target_date]
        return candidates[-1] if candidates else None


def _extract_string_var(body: str, var_name: str) -> str | None:
    pattern = rf'var\s+{re.escape(var_name)}\s*=\s*"([^"]*)"'
    match = re.search(pattern, body)
    return match.group(1).strip() if match else None


def _extract_json_var(body: str, var_name: str) -> list[dict]:
    pattern = rf"var\s+{re.escape(var_name)}\s*=\s*(\[[\s\S]*?\]);"
    match = re.search(pattern, body)
    if not match:
        raise PriceProviderError(f"未找到变量 {var_name}")
    return json.loads(match.group(1))


def _parse_history(raw_history: Iterable[dict]) -> list[PricePoint]:
    history: list[PricePoint] = []
    for item in raw_history:
        timestamp_ms = item.get("x")
        nav = item.get("y")
        if timestamp_ms is None or nav is None:
            continue
        history.append(
            PricePoint(
                price_date=datetime.fromtimestamp(
                    float(timestamp_ms) / 1000,
                    tz=timezone.utc,
                ).date(),
                nav=float(nav),
                pct_change_vs_prev=(
                    float(item["equityReturn"]) if item.get("equityReturn") is not None else None
                ),
            )
        )
    history.sort(key=lambda point: point.price_date)
    return history
