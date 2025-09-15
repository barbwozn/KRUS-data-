# -*- coding: utf-8 -*-
import os, glob, re, unicodedata
import pandas as pd
from typing import List, Optional

INPUT_DIR   = r"/content/KRUS-data-/dane_excel_kwartalne"
OUTPUT_CSV  = r"./master_records.csv"

# ── nazwy specjalne ──
REGION_NAMES = {"region","województwo","wojewodztwo","woj","kraj","państwo","panstwo"}
PERIOD_NAMES = {"okres","period","kwartał","kwartal","rok","miesiąc","miesiac","year","month","okres według stanu"}
REGION_PATTERNS = [r"\bwoj(e|ewództw|ewodztw)o", r"\bregion\b", r"\bkraj\b", r"\bpa(ns)?two\b"]
PERIOD_PATTERNS = [
    r"\bokres\b", r"\bperiod\b", r"\bkwarta(ł|l)\b", r"\brok\b", r"\bmiesi(?:ąc|ac)\b",
    r"okres\s+wedlug\s+stanu", r"okres\s+wed[oó]ug\s+stanu"
]

# ── rozpoznawanie okresów ──
PERIOD_Q_RE   = re.compile(r"^\d{4}[-/]?q[1-4]$", re.IGNORECASE)         # 2025-Q1, 2025Q1
PERIOD_ISO_RE = re.compile(r"^\d{4}([-/])\d{2}(\1\d{2})?$")               # 2025-03-31, 2025/03
PERIOD_PL_RE  = re.compile(r"^\d{2}\.\d{2}\.\d{4}$")                      # 31.03.2025
YEAR_ONLY_RE  = re.compile(r"^\d{4}$")                                    # 2025

# ── mapa polskich miesięcy ──
MONTHS_MAP = {
    "stycznia":"01","lutego":"02","marca":"03","kwietnia":"04","maja":"05","czerwca":"06",
    "lipca":"07","sierpnia":"08","września":"09","wrzesnia":"09","października":"10","pazdziernika":"10",
    "listopada":"11","grudnia":"12"
}

# ── utils ──
def normalize_polish_dates(text: str) -> str:
    if not isinstance(text, str):
        return text
    def repl(m):
        d = m.group(1).zfill(2)
        mm = MONTHS_MAP.get(m.group(2).lower())
        y = m.group(3)
        return f"{d}.{mm}.{y}" if mm else m.group(0)
    pat = r"\b(\d{1,2})\s+(stycznia|lutego|marca|kwietnia|maja|czerwca|lipca|sierpnia|września|wrzesnia|października|pazdziernika|listopada|grudnia)\s+(\d{4})\s*r?\.?,?"
    return re.sub(pat, repl, text, flags=re.IGNORECASE)

def read_csv_any(path: str) -> pd.DataFrame:
    for enc in ("utf-8-sig","utf-8","cp1250","iso-8859-2"):
        try:
            return pd.read_csv(path, encoding=enc)
        except Exception:
            pass
    return pd.read_csv(path)

def strip_accents(s: str) -> str:
    return "".join(c for c in unicodedata.normalize("NFD", s or "") if unicodedata.category(c) != "Mn")

def norm(s: str) -> str:
    s = strip_accents(s).lower().strip()
    return re.sub(r"\s+", " ", s)

def find_special_col(columns: List[str], exact_set: set, patterns: List[str]) -> Optional[str]:
    for c in columns:
        if norm(c) in exact_set:
            return c
    for c in columns:
        nc = norm(c)
        if any(re.search(p, nc) for p in patterns):
            return c
    return None

def clean_value(v) -> str:
    if pd.isna(v): return ""
    try:
        if isinstance(v, float) and v.is_integer(): return str(int(v))
    except Exception:
        pass
    s = str(v)
    if s.endswith(".0") and s.replace(".0","").isdigit(): return s.replace(".0","")
    return s

def is_period_token(s: str) -> bool:
    if not isinstance(s, str): return False
    t = s.strip()
    return bool(
        PERIOD_Q_RE.match(t) or
        PERIOD_ISO_RE.match(t) or
        PERIOD_PL_RE.match(normalize_polish_dates(t)) or
        YEAR_ONLY_RE.match(t)
    )

def period_year(s: str) -> Optional[int]:
    """Ekstrakcja roku odporna na śmieci/taby i różne formaty."""
    if not isinstance(s, str) or not s.strip():
        return None
    t = s.strip()
    # spróbuj DD.MM.YYYY
    m = re.search(r"\b(\d{2})\.(\d{2})\.(\d{4})\b", t)
    if m:
        return int(m.group(3))
    # rok na początku (YYYY..., łapie YYYY, YYYY-Qn, YYYY-MM itp.)
    m = re.match(r"^(\d{4})", t)
    if m:
        return int(m.group(1))
    # ostatecznie: jakiekolwiek cztery cyfry w środku
    m = re.search(r"(\d{4})", t)
    return int(m.group(1)) if m else None

def table_has_any_period(df: pd.DataFrame) -> bool:
    # nagłówki
    if any(is_period_token(str(c)) for c in df.columns):
        return True
    # wartości
    for c in df.columns:
        ser = df[c].astype(str)
        if ser.map(is_period_token).any():
            return True
    return False

# ── główne przetwarzanie pojedynczego pliku ──
def process_file(path: str) -> pd.DataFrame:
    df = read_csv_any(path)
    df.columns = [c.strip() for c in df.columns]

    # 1) myślniki -> "brak danych"
    df = df.replace(regex={r"^\s*[-–—]\s*$": "brak danych"})

    dataset = os.path.splitext(os.path.basename(path))[0]
    has_any_period_in_table = table_has_any_period(df)

    # 2) identyfikacja kolumn specjalnych
    region_col = find_special_col(list(df.columns), REGION_NAMES, REGION_PATTERNS)
    period_col = find_special_col(list(df.columns), PERIOD_NAMES, PERIOD_PATTERNS)
    typ_col    = next((c for c in df.columns if norm(c) == "typ"), None)

    id_vars = []
    if region_col: id_vars.append(region_col)
    if period_col: id_vars.append(period_col)
    if typ_col:    id_vars.append(typ_col)

    # 3) value_vars, z fallbackiem
    value_vars = [c for c in df.columns if c not in id_vars]
    if not value_vars:
        soft_id = [x for x in [region_col, typ_col] if x]
        value_vars = [c for c in df.columns if c not in soft_id]
        if not value_vars:
            value_vars = list(df.columns)

    # 4) melt
    long = df.melt(id_vars=id_vars, value_vars=value_vars,
                   var_name="measure", value_name="value")

    # 5) mapowanie i wymuszenie typów tekstowych
    long["dataset"] = dataset
    long["region"]  = long[region_col] if region_col else ""
    long["period"]  = long[period_col] if period_col else ""
    long["typ"]     = long[typ_col]    if typ_col    else ""
    for c in ["measure","value","region","period","typ","dataset"]:
        long[c] = long[c].astype("string")

    # 6) ustawianie period z measure / id_vars (bez czyszczenia innych kolumn)
    mask_measure_period = long["measure"].astype(str).map(is_period_token)
    long.loc[mask_measure_period & (long["period"].isna() | (long["period"] == "")), "period"] = long.loc[mask_measure_period, "measure"]

    for idc in id_vars:
        vals = long[idc].astype(str)
        mask_row_period = vals.map(is_period_token)
        long.loc[mask_row_period & (long["period"].isna() | (long["period"] == "")), "period"] = vals[mask_row_period]

    # 7) czyszczenie i normalizacja (strip + normalizacja PL dat + redukcja białych znaków)
    for col in ["measure","value","region","period","typ"]:
        long[col] = long[col].map(clean_value)
        long[col] = long[col].map(normalize_polish_dates)
        long[col] = long[col].astype(str).str.replace(r"\s+", " ", regex=True).str.strip()

    # 8) polityka okresów
    if not has_any_period_in_table:
        long["period"] = "2025"  # brak jakichkolwiek dat w źródle

    yrs_all = long["period"].map(period_year)
    dominant = yrs_all[yrs_all.isin([2024, 2025])].mode()
    default_year = int(dominant.iloc[0]) if not dominant.empty else 2025
    long.loc[long["period"].isna() | (long["period"] == ""), "period"] = str(default_year)

    # 9) filtr: tylko 2024 i 2025
    yrs = long["period"].map(period_year)
    kept = long.loc[yrs.isin([2024, 2025])].copy()

    if kept.empty:
        # diagnostyka kiedy 0 po filtrze
        yrs_dbg = long["period"].map(period_year).value_counts(dropna=False).sort_index()
        print(f"[INFO] {os.path.basename(path)} → 0 po filtrze; rozkład lat w 'period': {dict(yrs_dbg)}")

    return kept[["dataset","measure","value","region","period","typ"]]

# ── główny przebieg ──
parts: List[pd.DataFrame] = []
for path in sorted(glob.glob(os.path.join(INPUT_DIR, "*.csv"))):
    try:
        part = process_file(path)
        parts.append(part)
        print(f"[OK] {os.path.basename(path)} → {len(part)} rekordów")
    except Exception as e:
        print(f"[BŁĄD] {os.path.basename(path)}: {e}")

if parts:
    master = pd.concat(parts, ignore_index=True)

    # 10) po merge'u uzupełnij globalnie puste kategorie
    # typ → ogółem
    master["typ"] = master["typ"].fillna("").astype(str).str.strip()
    master.loc[master["typ"] == "", "typ"] = "ogółem"

    # region → ogółem
    master["region"] = master["region"].fillna("").astype(str).str.strip()
    master.loc[master["region"] == "", "region"] = "ogółem"

    # period → 2025 (na wszelki wypadek)
    master["period"] = master["period"].fillna("").astype(str).str.strip()
    master.loc[master["period"] == "", "period"] = "2025"

    # sanity check
    for c in ["region","typ","period"]:
        n_empty = (master[c].isna() | (master[c].astype(str).str.strip() == "")).sum()
        if n_empty:
            print(f"[WARN] Po uzupełnieniach w '{c}' nadal {n_empty} pustych.")

    master.to_csv(OUTPUT_CSV, index=False, encoding="utf-8-sig")
    print(f"Zapisano: {OUTPUT_CSV} ({len(master)} rekordów)")
else:
    print("Brak danych wejściowych.")
