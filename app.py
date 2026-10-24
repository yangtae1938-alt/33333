# -*- coding: utf-8 -*-
"""
ZERO-BASE 레이더 v1.0  -  미국 마이크로캡 급등 전 포착 레이더
=====================================================================
한 파일(app.py)로 동작합니다. 폴더/패키지 업로드가 필요 없습니다.

1) 미국 상장 종목 자동 스캔  : NASDAQ / NYSE / NYSE American (티커 입력 불필요)
2) 급등 전·장중 스퀴즈 신호  : 같은 시간대 누적 거래량 배수, 가격 변화, 저유통주, 수급 변화
3) SEC 희석 포렌식           : ATM, S-1/S-3, 424B, EFFECT, 워런트, 전환증권, ELOC, 역병합, 매도 가능 물량
4) 텔레그램 + 아이폰 대시보드 : 점수별 알림, 상세 공시, 클라우드 상시 실행

실행
  대시보드 : streamlit run app.py
  상시 스캔 : python app.py --scan --interval 300      (텔레그램 알림만, UI 없음)
  1회 스캔  : python app.py --scan --once

설정값(Streamlit Cloud: Settings > Secrets / 그 외: 환경변수)
  TELEGRAM_TOKEN, TELEGRAM_CHAT_ID, SEC_USER_AGENT ("이름 이메일" 형식 권장)

주의: 투자 권유가 아닌 정보 도구입니다. 무료 데이터(Nasdaq/Yahoo/SEC)는 지연·누락될 수 있습니다.
"""
from __future__ import annotations

import html as htmllib
import json
import math
import os
import re
import sys
import threading
import time
import datetime as dt
from concurrent.futures import ThreadPoolExecutor, as_completed
from dataclasses import dataclass, field, asdict
from zoneinfo import ZoneInfo

import pandas as pd
import requests

VERSION = "ZERO-BASE v1.0"
ET = ZoneInfo("America/New_York")

BROWSER_HEADERS = {
    "User-Agent": (
        "Mozilla/5.0 (Macintosh; Intel Mac OS X 10_15_7) AppleWebKit/605.1.15 "
        "(KHTML, like Gecko) Version/17.0 Safari/605.1.15"
    ),
    "Accept": "application/json, text/plain, */*",
}

# ----------------------------------------------------------------------------
# 설정 / 공통 유틸
# ----------------------------------------------------------------------------


def get_secret(name: str, default: str = "") -> str:
    """Streamlit secrets -> 환경변수 순으로 값을 찾는다."""
    try:
        import streamlit as st  # noqa

        if name in st.secrets:
            return str(st.secrets[name])
    except Exception:
        pass
    return os.environ.get(name, default)


@dataclass
class Config:
    exchanges: tuple = ("NASDAQ", "NYSE", "NYSE American")
    price_min: float = 0.5
    price_max: float = 20.0
    mcap_max: float = 500e6        # 시가총액 상한 (USD)
    vol_min: int = 200_000         # 최소 당일 거래량 (스크리너 1차 필터)
    shortlist: int = 120           # 분봉 분석 대상 수
    sec_top: int = 25              # SEC 간이 점검 대상 수 (점수 상위)
    alert_min: float = 60.0        # 텔레그램 알림 최소 최종 점수
    deep_alert: bool = True        # 알림 직전 SEC 본문 정밀 점검


def now_et() -> dt.datetime:
    return dt.datetime.now(ET)


def session_label(ts: dt.datetime | None = None) -> str:
    t = ts or now_et()
    if t.weekday() >= 5:
        return "휴장(주말)"
    m = t.hour * 60 + t.minute
    if 4 * 60 <= m < 9 * 60 + 30:
        return "프리마켓"
    if 9 * 60 + 30 <= m < 16 * 60:
        return "정규장"
    if 16 * 60 <= m < 20 * 60:
        return "애프터마켓"
    return "장 마감"


def market_window_open() -> bool:
    return session_label() in ("프리마켓", "정규장", "애프터마켓")


_session = requests.Session()


def http_get(url, *, params=None, headers=None, timeout=20, retries=2):
    last = None
    for i in range(retries + 1):
        try:
            r = _session.get(url, params=params, headers=headers, timeout=timeout)
            if r.status_code == 429:
                time.sleep(1.5 * (i + 1))
                last = RuntimeError("429 rate limited")
                continue
            r.raise_for_status()
            return r
        except requests.RequestException as e:
            last = e
            time.sleep(0.8 * (i + 1))
    raise last if last else RuntimeError("request failed")


def ttl_cache(seconds: int):
    def deco(fn):
        store, lock = {}, threading.Lock()

        def wrap(*args):
            now = time.time()
            with lock:
                hit = store.get(args)
                if hit and now - hit[0] < seconds:
                    return hit[1]
            val = fn(*args)
            with lock:
                store[args] = (now, val)
            return val

        wrap.clear = store.clear
        return wrap

    return deco


def _num(s):
    if s is None:
        return None
    s = str(s).replace("$", "").replace(",", "").replace("%", "").strip()
    try:
        return float(s)
    except ValueError:
        return None


# ----------------------------------------------------------------------------
# 1. 미국 전 종목 자동 스캔 (Nasdaq 스크리너)
# ----------------------------------------------------------------------------
NASDAQ_SCREENER = "https://api.nasdaq.com/api/screener/stocks"
EXCHANGES = {"NASDAQ": "nasdaq", "NYSE": "nyse", "NYSE American": "amex"}
JUNK_NAME = re.compile(
    r"\bwarrants?\b|\brights?\b|\bunits?\b|\bpreferred\b|\bnotes?\b|\bETF\b|\bETN\b|"
    r"\bfund\b|\btrust\b|acquisition corp|depositary",
    re.I,
)


@ttl_cache(600)
def fetch_exchange(label: str) -> pd.DataFrame:
    r = http_get(
        NASDAQ_SCREENER,
        params={
            "tableonly": "true", "limit": "25", "offset": "0",
            "download": "true", "exchange": EXCHANGES[label],
        },
        headers=BROWSER_HEADERS,
        timeout=30,
    )
    data = r.json().get("data") or {}
    rows = data.get("rows") or (data.get("table") or {}).get("rows") or []
    recs = [
        {
            "symbol": (x.get("symbol") or "").strip().upper(),
            "name": x.get("name") or "",
            "price": _num(x.get("lastsale")),
            "pct": _num(x.get("pctchange")),
            "volume": _num(x.get("volume")),
            "mcap": _num(x.get("marketCap")),
            "sector": x.get("sector") or "",
            "exchange": label,
        }
        for x in rows
    ]
    return pd.DataFrame(recs)


def load_universe(cfg: Config):
    """전 종목 목록을 자동 갱신하고 1차 필터를 적용한다."""
    frames, warns = [], []
    for label in cfg.exchanges:
        try:
            frames.append(fetch_exchange(label))
        except Exception as e:  # noqa
            warns.append(f"{label} 목록 실패: {e}")
    frames = [f for f in frames if not f.empty]
    if not frames:
        return pd.DataFrame(), warns
    df = pd.concat(frames, ignore_index=True).drop_duplicates("symbol")
    total = len(df)
    m = df.symbol.str.match(r"^[A-Z]{1,5}$") & ~df.name.str.contains(JUNK_NAME, na=False)
    m &= df.price.between(cfg.price_min, cfg.price_max)
    m &= df.volume.fillna(0) >= cfg.vol_min
    m &= df.mcap.isna() | (df.mcap == 0) | (df.mcap <= cfg.mcap_max)
    df = df[m].copy()
    warns.append(f"전체 {total:,}종목 중 1차 필터 통과 {len(df):,}종목")
    return df, warns


def pick_shortlist(df: pd.DataFrame, n: int) -> pd.DataFrame:
    """등락률 상위 절반 + 거래량 상위 절반을 합쳐 분봉 분석 대상을 정한다."""
    if df.empty:
        return df
    half = max(1, n // 2)
    a = df.sort_values("pct", ascending=False).head(half)
    b = df.sort_values("volume", ascending=False).head(n - half)
    return pd.concat([a, b]).drop_duplicates("symbol")


# ----------------------------------------------------------------------------
# 2. 급등 전·장중 스퀴즈 신호 (Yahoo 분봉, 같은 시간대 비교)
# ----------------------------------------------------------------------------


def yahoo_bars(sym: str, rng: str = "1mo", interval: str = "5m") -> pd.DataFrame:
    last = None
    for host in ("query1", "query2"):
        try:
            r = http_get(
                f"https://{host}.finance.yahoo.com/v8/finance/chart/{sym.replace('.', '-')}",
                params={"range": rng, "interval": interval, "includePrePost": "true"},
                headers=BROWSER_HEADERS,
                retries=1,
            )
            res = r.json()["chart"]["result"][0]
            break
        except Exception as e:  # noqa
            last = e
    else:
        raise last
    ts = res.get("timestamp") or []
    if not ts:
        return pd.DataFrame(columns=["t", "close", "volume"])
    q = res["indicators"]["quote"][0]
    tz = res.get("meta", {}).get("exchangeTimezoneName", "America/New_York")
    df = pd.DataFrame(
        {
            "t": pd.to_datetime(ts, unit="s", utc=True).tz_convert(tz),
            "close": q.get("close"),
            "volume": q.get("volume"),
        }
    ).dropna()
    return df


ALPACA_URL = "https://data.alpaca.markets/v2/stocks/bars"


def alpaca_keys():
    return get_secret("ALPACA_KEY"), get_secret("ALPACA_SECRET")


def alpaca_bars_batch(symbols, days: int = 21, timeframe: str = "15Min") -> dict:
    """여러 종목의 15분봉을 한 번에 받는다 (IEX 피드, 무료). 비율 비교용."""
    key, sec = alpaca_keys()
    start = (dt.datetime.now(dt.timezone.utc) - dt.timedelta(days=days)).strftime("%Y-%m-%dT00:00:00Z")
    hdr = {"APCA-API-KEY-ID": key, "APCA-API-SECRET-KEY": sec}
    raw, token = {}, None
    for _ in range(60):
        params = {"symbols": ",".join(symbols), "timeframe": timeframe, "start": start,
                  "limit": 10000, "feed": "iex", "adjustment": "raw", "sort": "asc"}
        if token:
            params["page_token"] = token
        j = http_get(ALPACA_URL, params=params, headers=hdr, timeout=40, retries=1).json()
        for sym, bars in (j.get("bars") or {}).items():
            raw.setdefault(sym, []).extend(bars)
        token = j.get("next_page_token")
        if not token:
            break
    out = {}
    for sym, bars in raw.items():
        if not bars:
            continue
        d = pd.DataFrame(bars)
        out[sym] = pd.DataFrame({
            "t": pd.to_datetime(d["t"], utc=True).dt.tz_convert("America/New_York"),
            "close": d["c"], "volume": d["v"],
        }).dropna()
    return out


def intraday_stats(df: pd.DataFrame, days_back: int = 20):
    """
    장중 누적 거래량을 '전일 전체 거래량'이 아니라
    '과거 같은 시각까지의 누적 거래량'과 비교한다.
    """
    if df is None or df.empty:
        return None
    df = df.copy()
    df["d"] = df.t.dt.date
    df["m"] = df.t.dt.hour * 60 + df.t.dt.minute
    days = sorted(df.d.unique())
    if len(days) < 4:
        return None
    today, prev = days[-1], days[:-1][-days_back:]
    td = df[df.d == today]
    cutoff = int(td.m.max())
    today_cum = float(td.volume.sum())

    prev_cum, prev_full = [], []
    for d in prev:
        dd = df[df.d == d]
        prev_cum.append(float(dd[dd.m <= cutoff].volume.sum()))
        prev_full.append(float(dd.volume.sum()))
    nz = [x for x in prev_cum if x > 0]
    avg_cum = sum(nz) / len(nz) if nz else 0.0
    med_cum = sorted(nz)[len(nz) // 2] if nz else 0.0
    base = (avg_cum + med_cum) / 2 if nz else 0.0   # 평균·중앙값 평균 (이상치 완화)

    rvol_tod = today_cum / base if base > 0 else None
    rvol_prev = today_cum / prev_full[-1] if prev_full and prev_full[-1] > 0 else None
    avg_full = sum(prev_full) / len(prev_full) if prev_full else 0
    rvol_full = today_cum / avg_full if avg_full > 0 else None

    # 수급 변화: 최근 3일 평균 거래량 / 그 이전 평균
    trend3 = None
    if len(prev_full) >= 6:
        recent, earlier = prev_full[-3:], prev_full[:-3]
        e = sum(earlier) / len(earlier)
        trend3 = (sum(recent) / 3) / e if e > 0 else None

    # 전일 정규장 종가 대비 등락
    pd_ = df[df.d == prev[-1]]
    reg = pd_[(pd_.m >= 570) & (pd_.m < 960)]
    prev_close = float((reg if not reg.empty else pd_).close.iloc[-1])
    last = float(td.close.iloc[-1])
    pct = (last / prev_close - 1) * 100 if prev_close > 0 else None

    return {
        "last": last,
        "pct": pct,
        "today_vol": today_cum,
        "rvol_tod": rvol_tod,
        "rvol_prev": rvol_prev,
        "rvol_full": rvol_full,
        "trend3": trend3,
        "bar_time": td.t.iloc[-1].strftime("%m/%d %H:%M"),
        "n_prev_days": len(prev),
    }


def stage_of(pct) -> str:
    if pct is None:
        return "단계 불명"
    if pct < -20:
        return "급락 중"
    if pct <= 20:
        return "급등 전 가능"
    if pct <= 60:
        return "상승 진행 중"
    return "이미 급등"


def score_signal(s: dict, mcap, price) -> tuple[float, dict, float | None]:
    """0~100 점수. 급등 '전' 단계(거래량 선행, 가격 초기)에 가점."""
    parts = {}
    r = s.get("rvol_tod") or 0
    parts["시간대 거래량"] = min(r / 6.0, 1.0) * 35

    p = s.get("pct")
    if p is None:
        pp = 0.0
    elif p < 0:
        pp = max(0.0, 6 + p * 0.3)
    elif p <= 15:
        pp = 8 + (p / 15) * 12            # 8~20 (초기 상승 최고점)
    elif p <= 40:
        pp = 20 - (p - 15) / 25 * 10      # 이미 오른 종목은 감점
    else:
        pp = max(2.0, 10 - (p - 40) / 20 * 8)
    parts["가격 변화"] = pp

    shares = (mcap / price) if (mcap and price) else None
    if shares is None:
        fl = 0
    elif shares < 10e6:
        fl = 25
    elif shares < 25e6:
        fl = 20
    elif shares < 50e6:
        fl = 14
    elif shares < 100e6:
        fl = 8
    else:
        fl = 3
    parts["저유통주(추정)"] = fl

    dv = (price or 0) * s.get("today_vol", 0)
    parts["유동성"] = 10 if dv >= 2e6 else 7 if dv >= 5e5 else 4 if dv >= 1.5e5 else 1

    t3 = s.get("trend3")
    parts["수급 변화"] = min(max((t3 or 1) - 1, 0) / 1.5, 1.0) * 10

    total = min(sum(parts.values()), 100)
    if p is not None:
        if p > 60:
            total = min(total, 55)      # 이미 급등한 종목은 A/B 등급 제외
        elif p < -20:
            total = min(total, 40)      # 급락 종목은 급등 전 후보가 아님
    return round(total, 1), parts, shares


# ----------------------------------------------------------------------------
# 3. SEC 희석 포렌식
# ----------------------------------------------------------------------------
_sec_lock = threading.Lock()
_sec_last = [0.0]


def sec_ua() -> str:
    return get_secret("SEC_USER_AGENT", "ZeroBaseRadar contact@example.com")


def sec_get(url, **kw):
    """SEC는 초당 10회 제한 + User-Agent 필수."""
    with _sec_lock:
        wait = 0.12 - (time.time() - _sec_last[0])
        if wait > 0:
            time.sleep(wait)
        _sec_last[0] = time.time()
    return http_get(url, headers={"User-Agent": sec_ua()}, **kw)


@ttl_cache(86400)
def sec_ticker_map() -> dict:
    j = sec_get("https://www.sec.gov/files/company_tickers.json").json()
    return {v["ticker"].upper(): str(v["cik_str"]).zfill(10) for v in j.values()}


@ttl_cache(1800)
def sec_filings(cik: str) -> pd.DataFrame:
    j = sec_get(f"https://data.sec.gov/submissions/CIK{cik}.json").json()
    df = pd.DataFrame(j["filings"]["recent"])
    for c in ("items", "primaryDocument", "primaryDocDescription"):
        if c not in df.columns:
            df[c] = ""
    return df


# (키, 폼 정규식, 위험점수, 설명)
FORM_RULES = [
    ("S1", r"^S-1", 20, "S-1 등록 (유상증자/재판매 등록)"),
    ("S3", r"^S-3", 10, "S-3 셸프 등록"),
    ("424B", r"^424B[2345]", 15, "424B 투자설명서 보충 (ATM/공모 가능)"),
    ("EFFECT", r"^EFFECT$", 10, "EFFECT (등록 효력 발생)"),
]
ITEM_RULES = [
    ("3.02", 10, "8-K Item 3.02 (비등록 증권 발행)"),
    ("5.03", 8, "8-K Item 5.03 (정관 변경, 역병합 가능)"),
]
# 본문 키워드 (키, 정규식, 점수, 설명)
KEYWORD_RULES = [
    ("ATM", r"at[- ]the[- ]market|equity distribution agreement|controlled equity offering", 25, "ATM 프로그램"),
    ("ELOC", r"equity line|equity purchase agreement|standby equity|committed equity|equity financing agreement", 20, "ELOC (지분 매입 약정)"),
    ("CONV", r"convertible (?:promissory )?notes?|convertible preferred|conversion price", 15, "전환증권"),
    ("WARR", r"pre-funded warrants?|warrants? to purchase", 10, "워런트"),
    ("RSPLIT", r"reverse (?:stock )?split", 15, "역병합 언급"),
    ("GOING", r"substantial doubt (?:about|regarding) (?:its|our|the company.s) ability to continue", 10, "계속기업 불확실성"),
]
CAP_PATTERNS = [
    r"aggregate offering price of (?:up to )?\$\s?[\d,\.]+(?:\s?(?:million|billion))?",
    r"up to \$\s?[\d,\.]+(?:\s?(?:million|billion))?",
    r"up to [\d,]{6,} shares of (?:our |the Company.s )?common stock",
    r"[\d,]{6,} shares of (?:our |the Company.s )?common stock (?:issuable|underlying|offered|to be sold|that may be (?:offered|sold|resold))",
]


def filing_url(cik: str, acc: str, doc: str) -> str:
    return f"https://www.sec.gov/Archives/edgar/data/{int(cik)}/{acc.replace('-', '')}/{doc}"


def _doc_text(url: str, limit: int = 3_000_000) -> str:
    r = sec_get(url, timeout=30)
    raw = r.content[:limit].decode("utf-8", errors="ignore")
    raw = re.sub(r"(?is)<(script|style).*?>.*?</\1>", " ", raw)
    raw = re.sub(r"<[^>]+>", " ", raw)
    return re.sub(r"\s+", " ", htmllib.unescape(raw))


def _snippets(text: str, patterns, max_n=6, ctx=70):
    out, seen = [], set()
    for pat in patterns:
        for m in re.finditer(pat, text, re.I):
            s = text[max(0, m.start() - ctx): m.end() + ctx].strip()
            k = m.group(0).lower()
            if k in seen:
                continue
            seen.add(k)
            out.append(s)
            if len(out) >= max_n:
                return out
    return out


def forensics(sym: str, deep: bool = False, mcap=None) -> dict:
    out = {"symbol": sym, "risk": 0, "flags": [], "filings": pd.DataFrame(),
           "capacity": [], "baby_shelf": None, "error": None}
    try:
        cik = sec_ticker_map().get(sym.upper())
        if not cik:
            out["error"] = "SEC CIK를 찾지 못함"
            return out
        fl = sec_filings(cik)
    except Exception as e:  # noqa
        out["error"] = f"SEC 조회 실패: {e}"
        return out

    cut180 = (dt.date.today() - dt.timedelta(days=180)).isoformat()
    cut365 = (dt.date.today() - dt.timedelta(days=365)).isoformat()
    recs = fl.to_dict("records")
    flags, seen = [], set()

    for r in recs:
        if r["filingDate"] < cut180:
            continue
        form = r["form"]
        for key, pat, pts, label in FORM_RULES:
            if key not in seen and re.match(pat, form):
                seen.add(key)
                flags.append((label, pts, f"{form} · {r['filingDate']}"))
        if form.startswith("8-K"):
            for item, pts, label in ITEM_RULES:
                if item not in seen and item in str(r.get("items", "")):
                    seen.add(item)
                    flags.append((label, pts, f"{form} · {r['filingDate']}"))

    # 본문 정밀 점검 (최근 희석 관련 서류 몇 건)
    if deep:
        targets = [r for r in recs if r["filingDate"] >= cut180 and r["primaryDocument"]
                   and (r["form"].startswith(("S-1", "S-3", "424B", "8-K")))][:4]
        periodic = [r for r in recs if r["form"] in ("10-Q", "10-K") and r["primaryDocument"]][:1]
        found, caps = set(), []
        for r in periodic + targets:
            try:
                text = _doc_text(filing_url(cik, r["accessionNumber"], r["primaryDocument"]))
            except Exception:
                continue
            for key, pat, pts, label in KEYWORD_RULES:
                if key in found:
                    continue
                if re.search(pat, text, re.I):
                    found.add(key)
                    flags.append((label, pts, f"본문 · {r['form']} {r['filingDate']}"))
            if r["form"].startswith(("S-1", "S-3", "424B")) or "ATM" in found:
                for s in _snippets(text, CAP_PATTERNS):
                    if s not in caps:
                        caps.append(s)
        out["capacity"] = caps[:8]

    if mcap:
        if mcap < 75e6:
            out["baby_shelf"] = (f"시총 ${mcap/1e6:,.0f}M < $75M → 베이비쉘프 적용 가능성 높음. "
                                 f"12개월 발행 상한 ≈ 비계열 유통시총의 1/3 (최대 약 ${mcap/3/1e6:,.1f}M)")
        else:
            out["baby_shelf"] = ("시총 $75M 이상 → 상한 제한이 없을 수 있음 "
                                 "(비계열 유통시총 기준으로 직접 확인 필요)")

    rel = [r for r in recs if r["filingDate"] >= cut365 and (
        r["form"].startswith(("S-1", "S-3", "424B", "8-K", "10-Q", "10-K")) or r["form"] in ("EFFECT", "RW"))][:25]
    out["filings"] = pd.DataFrame(
        [{"날짜": r["filingDate"], "폼": r["form"], "항목": r.get("items", ""),
          "링크": filing_url(cik, r["accessionNumber"], r["primaryDocument"]) if r["primaryDocument"] else ""}
         for r in rel]
    )
    out["flags"] = flags
    out["risk"] = min(100, sum(f[1] for f in flags))
    return out


# ----------------------------------------------------------------------------
# 전체 파이프라인
# ----------------------------------------------------------------------------
GRADES = [(75, "A"), (60, "B"), (45, "C")]


def grade_of(score: float) -> str:
    for th, g in GRADES:
        if score >= th:
            return g
    return "D"


_scan_errors: dict = {}
_err_lock = threading.Lock()


def _note_error(msg: str):
    with _err_lock:
        _scan_errors[msg] = _scan_errors.get(msg, 0) + 1


def analyze_symbol(row: dict, bars=None, use_alpaca: bool = False):
    try:
        if use_alpaca:
            if bars is None or bars.empty:
                _note_error("Alpaca 데이터 없음 (IEX 거래 없는 종목)")
                return None
            s = intraday_stats(bars)
        else:
            s = intraday_stats(yahoo_bars(row["symbol"]))
    except Exception as e:  # noqa
        _note_error(f"{type(e).__name__}: {str(e)[:110]}")
        return None
    if not s:
        _note_error("분봉 데이터 부족(거래일 4일 미만 또는 비어 있음)")
        return None
    if use_alpaca and row.get("volume"):
        s["today_vol"] = float(row["volume"])   # IEX 거래량은 일부라 전체 거래량으로 대체
    price = s["last"] or row.get("price")
    score, parts, shares = score_signal(s, row.get("mcap"), price)
    return {
        "symbol": row["symbol"], "name": row["name"], "exchange": row["exchange"],
        "price": price, "pct": s["pct"], "rvol_tod": s["rvol_tod"],
        "rvol_prev": s["rvol_prev"], "rvol_full": s["rvol_full"], "trend3": s["trend3"],
        "today_vol": s["today_vol"], "mcap": row.get("mcap"), "shares_est": shares,
        "signal": score, "parts": parts, "bar_time": s["bar_time"],
    }


def run_scan(cfg: Config, progress=None):
    warns = []
    uni, w = load_universe(cfg)
    warns += w
    if uni.empty:
        return pd.DataFrame(), warns
    short = pick_shortlist(uni, cfg.shortlist)
    rows = short.to_dict("records")
    results, done = [], 0
    _scan_errors.clear()
    use_alpaca = all(alpaca_keys())
    if use_alpaca:
        warns.append("데이터 출처: Alpaca(IEX) — 거래량은 같은 시간대 비율 비교용")
        syms = [r["symbol"] for r in rows]
        bars = {}
        batches = [syms[i:i + 40] for i in range(0, len(syms), 40)]
        for bi, b in enumerate(batches):
            if progress:
                progress(0.6 * bi / len(batches), f"분봉 내려받기 {bi + 1}/{len(batches)}")
            try:
                bars.update(alpaca_bars_batch(b))
            except Exception as e:  # noqa
                _note_error(f"Alpaca {type(e).__name__}: {str(e)[:110]}")
        for i, r in enumerate(rows):
            if progress:
                progress(0.6 + 0.1 * (i + 1) / len(rows), f"분봉 분석 {i + 1}/{len(rows)}")
            res = analyze_symbol(r, bars.get(r["symbol"]), True)
            if res:
                results.append(res)
    else:
        with ThreadPoolExecutor(max_workers=4) as ex:
            futs = [ex.submit(analyze_symbol, r) for r in rows]
            for f in as_completed(futs):
                done += 1
                if progress:
                    progress(done / len(futs) * 0.7, f"분봉 분석 {done}/{len(futs)}")
                res = f.result()
                if res:
                    results.append(res)
    if not results:
        warns.append("분봉 데이터를 가져오지 못했습니다.")
        for msg, n in sorted(_scan_errors.items(), key=lambda x: -x[1])[:3]:
            warns.append(f"원인({n}건): {msg}")
        if not use_alpaca and any("429" in m for m in _scan_errors):
            warns.append("Yahoo가 클라우드 접속을 막았습니다. Secrets에 ALPACA_KEY / ALPACA_SECRET 을 넣으면 해결됩니다.")
        return pd.DataFrame(), warns
    df = pd.DataFrame(results).sort_values("signal", ascending=False).reset_index(drop=True)

    # SEC 간이 점검: 상위 종목만 (제출 목록 1회 호출)
    df["risk"] = 0
    df["sec_err"] = ""
    top = df.head(cfg.sec_top)
    for i, (idx, r) in enumerate(top.iterrows()):
        if progress:
            progress(0.7 + 0.3 * (i + 1) / len(top), f"SEC 점검 {i + 1}/{len(top)}")
        f = forensics(r.symbol, deep=False, mcap=r.mcap)
        df.loc[idx, "risk"] = f["risk"]
        df.loc[idx, "sec_err"] = f["error"] or ""
    df["final"] = (df.signal - df.risk * 0.4).clip(lower=0).round(1)
    df["grade"] = df.final.map(grade_of)
    df = df.sort_values("final", ascending=False).reset_index(drop=True)
    return df, warns


# ----------------------------------------------------------------------------
# 4. 텔레그램 알림
# ----------------------------------------------------------------------------
STATE_PATH = os.environ.get("RADAR_STATE", "alerts_state.json")


def _load_state() -> dict:
    try:
        with open(STATE_PATH, "r", encoding="utf-8") as f:
            return json.load(f)
    except Exception:
        return {}


def _save_state(state: dict):
    try:
        with open(STATE_PATH, "w", encoding="utf-8") as f:
            json.dump(state, f)
    except Exception:
        pass


def tg_send(text: str) -> tuple[bool, str]:
    token, chat = get_secret("TELEGRAM_TOKEN"), get_secret("TELEGRAM_CHAT_ID")
    if not token or not chat:
        return False, "TELEGRAM_TOKEN / TELEGRAM_CHAT_ID 가 설정되지 않았습니다."
    try:
        r = requests.post(
            f"https://api.telegram.org/bot{token}/sendMessage",
            json={"chat_id": chat, "text": text, "parse_mode": "HTML",
                  "disable_web_page_preview": True},
            timeout=15,
        )
        return (r.ok, "전송 완료" if r.ok else r.text[:200])
    except Exception as e:  # noqa
        return False, str(e)


def fmt_alert(r: dict, f: dict | None = None) -> str:
    icon = {"A": "🔴", "B": "🟠", "C": "🟡"}.get(r["grade"], "⚪")
    esc = htmllib.escape
    pct = r["pct"]
    risk_txt = "희석위험 확인불가" if r.get("sec_err") else f"희석위험 {r['risk']:.0f}"
    lines = [
        f"{icon} <b>{esc(r['symbol'])}</b> · 등급 {r['grade']} · 최종 {r['final']:.0f}점",
        f"{esc(str(r['name']))[:40]} ({esc(r['exchange'])})",
        f"가격 ${r['price']:.2f}" + (f" ({pct:+.1f}%)" if pct is not None else "") + f" · {stage_of(pct)}",
        f"시간대 거래량 {r['rvol_tod']:.1f}배 · 시그널 {r['signal']:.0f} · {risk_txt}"
        if r["rvol_tod"] else f"시그널 {r['signal']:.0f} · {risk_txt}",
    ]
    if r.get("sec_err"):
        lines.append(f"⚠️ SEC 조회 실패: {esc(str(r['sec_err'])[:80])}")
    if r["price"] < 1:
        lines.append("⚠️ $1 미만: 상장 유지·희석 위험 큼")
    if f:
        for label, pts, src in f["flags"][:6]:
            lines.append(f"• {esc(label)} (+{pts}) — {esc(src)}")
        if f.get("capacity"):
            lines.append("매도 가능 물량 단서: " + esc(f["capacity"][0][:140]))
    lines.append(f"https://finance.yahoo.com/quote/{r['symbol']}")
    lines.append(f"https://www.sec.gov/cgi-bin/browse-edgar?action=getcompany&type=&company=&CIK={r['symbol']}")
    return "\n".join(lines)


def send_alerts(df: pd.DataFrame, cfg: Config) -> list[str]:
    """alert_min 이상 후보 중, 오늘 같은 등급으로 보낸 적 없는 것만 전송."""
    log, state = [], _load_state()
    today = now_et().strftime("%Y-%m-%d")
    state = {k: v for k, v in state.items() if k.startswith(today)}
    for _, r in df[df.final >= cfg.alert_min].iterrows():
        key = f"{today}|{r.symbol}|{r.grade}"
        if key in state:
            continue
        rd = r.to_dict()
        f = forensics(r.symbol, deep=True, mcap=r.mcap) if cfg.deep_alert else None
        if f and f["error"]:
            rd["sec_err"] = f["error"]
        if f and not f["error"]:
            rd["sec_err"] = ""
            rd["risk"] = f["risk"]
            rd["final"] = round(max(0, rd["signal"] - f["risk"] * 0.4), 1)
            rd["grade"] = grade_of(rd["final"])
            if rd["final"] < cfg.alert_min:
                continue
        ok, msg = tg_send(fmt_alert(rd, f))
        log.append(f"{r.symbol}: {msg}")
        if ok:
            state[key] = int(time.time())
    _save_state(state)
    return log


# ----------------------------------------------------------------------------
# 상시 실행 모드 (UI 없음)  python app.py --scan --interval 300
# ----------------------------------------------------------------------------


def run_headless(interval: int = 300, once: bool = False):
    cfg = Config()
    print(f"[{VERSION}] headless 시작 (interval={interval}s)", flush=True)
    while True:
        if once or market_window_open():
            try:
                df, warns = run_scan(cfg)
                for w in warns:
                    print("·", w, flush=True)
                if not df.empty:
                    top = df.head(5)[["symbol", "final", "grade", "rvol_tod", "pct"]]
                    print(top.to_string(index=False), flush=True)
                    for line in send_alerts(df, cfg):
                        print("alert", line, flush=True)
            except Exception as e:  # noqa
                print("scan error:", e, flush=True)
        else:
            print(f"[{now_et():%H:%M} ET] 휴장 시간 — 대기", flush=True)
        if once:
            break
        time.sleep(interval)


# ----------------------------------------------------------------------------
# 아이폰 대시보드 (Streamlit)
# ----------------------------------------------------------------------------
def fmt_m(x):
    return "-" if x is None or (isinstance(x, float) and math.isnan(x)) else f"{x/1e6:,.1f}M"


def main_ui():
    import streamlit as st

    st.set_page_config(page_title="ZERO-BASE 레이더", page_icon="📡", layout="centered")
    st.title("📡 ZERO-BASE 레이더")
    st.caption(f"{VERSION} · 미국 마이크로캡 급등 전 포착 · 현재 {now_et():%m/%d %H:%M} ET ({session_label()})")

    if "cfg" not in st.session_state:
        st.session_state.cfg = Config()
    cfg: Config = st.session_state.cfg

    with st.expander("⚙️ 설정", expanded=False):
        cfg.exchanges = tuple(st.multiselect("거래소", list(EXCHANGES), default=list(cfg.exchanges)))
        c1, c2 = st.columns(2)
        cfg.price_min = c1.number_input("최소 가격($)", 0.1, 100.0, cfg.price_min, 0.1)
        cfg.price_max = c2.number_input("최대 가격($)", 0.5, 500.0, cfg.price_max, 0.5)
        cfg.mcap_max = st.number_input("시총 상한(백만$)", 10, 10000, int(cfg.mcap_max / 1e6), 50) * 1e6
        cfg.vol_min = int(st.number_input("최소 거래량(주)", 10_000, 10_000_000, cfg.vol_min, 50_000))
        cfg.shortlist = int(st.slider("분봉 분석 종목 수", 40, 300, cfg.shortlist, 10))
        cfg.alert_min = st.slider("텔레그램 알림 최소 점수", 30, 95, int(cfg.alert_min), 5)
        cfg.deep_alert = st.checkbox("알림 전 SEC 본문 정밀 점검", cfg.deep_alert)
        auto = st.checkbox("자동 갱신 (페이지 열려 있을 때)", False)
        every = st.slider("갱신 간격(분)", 3, 30, 5)
        use_tg = st.checkbox("스캔 후 텔레그램 알림 전송", False)
        if st.button("텔레그램 테스트 메시지"):
            ok, msg = tg_send(f"📡 {VERSION} 테스트 메시지")
            (st.success if ok else st.error)(msg)
        if "contact@example.com" in sec_ua():
            st.warning("SEC_USER_AGENT 를 '이름 이메일' 형식으로 Secrets에 설정하세요. (SEC 요구사항)")

    last = st.session_state.get("last_scan", 0)
    due = auto and (time.time() - last) > every * 60
    go = st.button("🔎 지금 스캔", type="primary", use_container_width=True)

    if go or due:
        bar = st.progress(0.0, text="목록 갱신 중…")
        df, warns = run_scan(cfg, lambda p, t: bar.progress(min(p, 1.0), text=t))
        bar.empty()
        st.session_state.update(result=df, warns=warns, last_scan=time.time(),
                                scanned_at=f"{now_et():%m/%d %H:%M} ET")
        if use_tg and not df.empty:
            for line in send_alerts(df, cfg):
                st.toast(line)

    df = st.session_state.get("result")
    if df is None:
        st.info("‘지금 스캔’을 누르면 NASDAQ·NYSE·NYSE American 전 종목을 자동으로 훑습니다.")
    else:
        for w in st.session_state.get("warns", []):
            st.caption("· " + w)
        if df.empty:
            st.warning("후보가 없습니다. 필터를 완화하거나 장 시간에 다시 시도하세요.")
        else:
            st.caption(f"마지막 스캔 {st.session_state.get('scanned_at')} · 후보 {len(df)}개")
            view = pd.DataFrame({
                "등급": df.grade, "티커": df.symbol, "점수": df.final,
                "가격": df.price.round(2), "등락%": df.pct.round(1),
                "시간대배수": df.rvol_tod.round(1), "전일대비": df.rvol_prev.round(1),
                "시총M": (df.mcap / 1e6).round(0), "희석위험": df.risk,
            })
            st.dataframe(view.head(40), use_container_width=True, hide_index=True)

            st.subheader("상세 공시")
            sym = st.selectbox("종목 선택", list(df.symbol.head(40)))
            row = df[df.symbol == sym].iloc[0]
            m1, m2, m3 = st.columns(3)
            m1.metric("최종 점수", f"{row.final:.0f}", f"시그널 {row.signal:.0f}")
            m2.metric("시간대 거래량", f"{row.rvol_tod:.1f}배" if row.rvol_tod else "-")
            m3.metric("추정 유통주식", fmt_m(row.shares_est))
            st.caption("유통주식은 시총÷가격으로 추정한 값입니다. 정확한 float은 별도 확인이 필요합니다.")
            st.write({k: round(v, 1) for k, v in row.parts.items()})

            if st.button("🧪 SEC 희석 포렌식 실행", use_container_width=True):
                with st.spinner("SEC 공시 본문 점검 중… (10~30초)"):
                    f = forensics(sym, deep=True, mcap=row.mcap)
                st.session_state[f"f_{sym}"] = f
            f = st.session_state.get(f"f_{sym}")
            if f:
                if f["error"]:
                    st.error(f["error"])
                else:
                    st.metric("희석 위험도", f"{f['risk']}/100")
                    if f["flags"]:
                        for label, pts, src in f["flags"]:
                            st.write(f"• **{label}** (+{pts}) — {src}")
                    else:
                        st.success("최근 180일 내 뚜렷한 희석 신호 없음")
                    if f["baby_shelf"]:
                        st.info(f["baby_shelf"])
                    if f["capacity"]:
                        st.write("**매도 가능 물량 단서 (공시 본문 발췌)**")
                        for s in f["capacity"]:
                            st.caption("…" + s + "…")
                    if not f["filings"].empty:
                        st.dataframe(
                            f["filings"], hide_index=True, use_container_width=True,
                            column_config={"링크": st.column_config.LinkColumn("링크", display_text="열기")},
                        )

    st.divider()
    st.caption("정보 제공용 도구이며 투자 권유가 아닙니다. 무료 데이터는 지연·누락될 수 있습니다.")

    if auto:
        time.sleep(20)
        st.rerun()


if __name__ == "__main__":
    if "--scan" in sys.argv:
        itv = 300
        if "--interval" in sys.argv:
            try:
                itv = int(sys.argv[sys.argv.index("--interval") + 1])
            except Exception:
                pass
        run_headless(interval=itv, once="--once" in sys.argv)
    else:
        main_ui()
