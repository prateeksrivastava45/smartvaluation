"""
SmartValuation — Fundamental Stock Analysis · India
Backend v1.0
DCF Valuation Engine using Screener.in data
"""

from flask import Flask, jsonify, request
from flask_cors import CORS
import requests
from bs4 import BeautifulSoup
import re
import time
import json
from datetime import datetime

app = Flask(__name__)
CORS(app)

# ── CONSTANTS ────────────────────────────────────────────────────────────────

# Risk Free Rate — RBI 10Y G-Sec yield (update quarterly)
RFR_INDIA        = 6.72   # % — as of March 2025
RFR_LAST_UPDATED = 'March 2025'

# Equity Risk Premium — Damodaran India 2025
ERP_INDIA        = 7.50   # %

# Damodaran Unlevered Beta by Sector (India dataset, Jan 2025)
DAMODARAN_BETA = {
    'IT / Software':           0.82,
    'Pharma / Healthcare':     0.72,
    'FMCG / Consumer':         0.53,
    'Banking / Finance':       0.37,
    'Infrastructure / Const':  0.72,
    'Steel / Metals':          0.82,
    'Auto / Auto Ancillary':   0.82,
    'Energy / Oil & Gas':      0.75,
    'Telecom':                 0.65,
    'Cement':                  0.72,
    'Real Estate':             0.70,
    'Diversified':             0.75,
    'General / Other':         0.75,
}

# Sector detection keywords → sector name
SECTOR_KEYWORDS = {
    'IT / Software':          ['software', 'it ', 'technology', 'infosys', 'tcs', 'wipro', 'tech mahindra', 'hcl'],
    'Pharma / Healthcare':    ['pharma', 'drug', 'hospital', 'healthcare', 'medicine', 'biotech'],
    'FMCG / Consumer':        ['fmcg', 'consumer', 'food', 'beverage', 'personal care', 'household', 'tobacco', 'hindustan unilever', 'hul', 'nestle', 'britannia', 'dabur', 'marico', 'godrej'],
    'Banking / Finance':      ['bank', 'finance', 'nbfc', 'insurance', 'financial services', 'lending'],
    'Infrastructure / Const': ['infrastructure', 'construction', 'engineering', 'epc', 'power', 'roads', 'l&t', 'larsen', 'toubro', 'bhel', 'abb', 'siemens', 'thermax'],
    'Steel / Metals':         ['steel', 'metal', 'iron', 'aluminium', 'copper', 'zinc', 'mining'],
    'Auto / Auto Ancillary':  ['auto', 'automobile', 'vehicle', 'tyre', 'ancillary', 'motor'],
    'Energy / Oil & Gas':     ['oil', 'gas', 'energy', 'petroleum', 'refinery', 'ongc', 'reliance'],
    'Telecom':                ['telecom', 'telecomm', 'airtel', 'jio', 'vodafone', 'communication'],
    'Cement':                 ['cement', 'ultratech', 'ambuja', 'acc', 'shree cement'],
    'Real Estate':            ['real estate', 'realty', 'property', 'housing', 'reit'],
    'Diversified':            ['diversified', 'conglomerate', 'tata sons', 'birla'],
}

HEADERS = {
    'User-Agent': 'Mozilla/5.0 (Windows NT 10.0; Win64; x64) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/120.0.0.0 Safari/537.36'
}

# Simple in-memory cache
_cache = {}

def cache_get(key):
    if key in _cache:
        data, ts = _cache[key]
        if time.time() - ts < 3600:  # 1 hour
            return data
    return None

def cache_set(key, data):
    _cache[key] = (data, time.time())


# ── HELPER FUNCTIONS ─────────────────────────────────────────────────────────

def clean_number(text):
    """Convert Screener number string to float. Returns None if invalid."""
    if not text:
        return None
    # Remove commas, spaces
    text = text.replace(',', '').replace(' ', '').strip()
    # Handle percentage
    if text.endswith('%'):
        text = text[:-1]
    # Handle negative in brackets like (1,234)
    if text.startswith('(') and text.endswith(')'):
        text = '-' + text[1:-1]
    try:
        val = float(text)
        return val
    except:
        return None

def detect_sector(company_name, industry_text=''):
    """Detect sector from company name and industry text."""
    text = (company_name + ' ' + industry_text).lower()
    for sector, keywords in SECTOR_KEYWORDS.items():
        for kw in keywords:
            if kw in text:
                return sector
    return 'General / Other'

def get_unlevered_beta(sector):
    """Get Damodaran unlevered beta for sector."""
    return DAMODARAN_BETA.get(sector, DAMODARAN_BETA['General / Other'])

def hamada_relever(unlevered_beta, tax_rate, debt, equity):
    """
    Hamada equation: βL = βU × (1 + (1-t) × D/E)
    debt and equity in same units (Cr)
    """
    if equity <= 0:
        return unlevered_beta
    de_ratio = debt / equity
    levered_beta = unlevered_beta * (1 + (1 - tax_rate) * de_ratio)
    return round(levered_beta, 4)

def safe_cagr(values, years):
    """
    Calculate CAGR from a list of values over given years.
    Returns 0 if calculation not possible.
    """
    # Filter out None and zero/negative values
    valid = [(i, v) for i, v in enumerate(values) if v is not None and v > 0]
    if len(valid) < 2:
        return 0.0
    start_val = valid[0][1]
    end_val   = valid[-1][1]
    n         = valid[-1][0] - valid[0][0]
    if n <= 0 or start_val <= 0:
        return 0.0
    try:
        cagr = (end_val / start_val) ** (1 / n) - 1
        # Cap at reasonable bounds
        return max(min(cagr, 0.50), -0.30)
    except:
        return 0.0


# ── STEP 1: SCREENER SCRAPER ─────────────────────────────────────────────────

def scrape_screener(symbol):
    """
    Scrape Screener.in for a given NSE symbol.
    Returns structured financial data dict.
    """
    symbol = symbol.upper().replace('.NS', '').replace('.BO', '')
    url    = f'https://www.screener.in/company/{symbol}/consolidated/'

    print(f'  Scraping Screener for {symbol}...')
    try:
        r = requests.get(url, headers=HEADERS, timeout=20)
        if r.status_code == 404:
            # Try standalone (non-consolidated)
            url = f'https://www.screener.in/company/{symbol}/'
            r   = requests.get(url, headers=HEADERS, timeout=20)
        if r.status_code != 200:
            return None, f'Screener returned status {r.status_code}'
    except Exception as e:
        return None, f'Network error: {str(e)}'

    soup = BeautifulSoup(r.text, 'html.parser')

    # ── Parse all financial tables ──────────────────────────────
    tables = {}
    for section in soup.find_all('section', class_='card'):
        heading = section.find('h2')
        if not heading:
            continue
        h     = heading.get_text(strip=True)
        table = section.find('table')
        if not table:
            continue

        # Get year headers
        thead = table.find('thead')
        years = []
        if thead:
            ths   = thead.find_all('th')
            years = [th.get_text(strip=True) for th in ths[1:]]

        # Get row data
        rows_data = {}
        tbody = table.find('tbody')
        if tbody:
            for tr in tbody.find_all('tr'):
                cells    = tr.find_all('td')
                if not cells:
                    continue
                row_name = cells[0].get_text(strip=True)
                values   = [clean_number(c.get_text(strip=True)) for c in cells[1:len(years)+1]]
                rows_data[row_name] = dict(zip(years, values))

        tables[h] = {'years': years, 'rows': rows_data}

    # ── Parse top ratios ────────────────────────────────────────
    top_ratios = {}
    ratios_ul  = soup.find('ul', id='top-ratios')
    if ratios_ul:
        for li in ratios_ul.find_all('li'):
            name_el  = li.find('span', class_='name')
            value_el = li.find('span', class_='number')
            if name_el and value_el:
                name = name_el.get_text(strip=True)
                val  = value_el.get_text(strip=True)
                top_ratios[name] = val

    # ── Parse company name ──────────────────────────────────────
    company_name = symbol
    name_tag = soup.find('h1', class_='h2')
    if not name_tag:
        name_tag = soup.find('h1')
    if name_tag:
        company_name = name_tag.get_text(strip=True)

    # ── Parse industry/sector ───────────────────────────────────
    industry = ''
    for a in soup.find_all('a'):
        href = a.get('href', '')
        if '/industry/' in href or '/sector/' in href:
            industry = a.get_text(strip=True)
            break

    return {
        'symbol':       symbol,
        'company_name': company_name,
        'industry':     industry,
        'tables':       tables,
        'top_ratios':   top_ratios,
    }, None


# ── STEP 2: EXTRACT FINANCIALS ───────────────────────────────────────────────

def extract_financials(screener_data):
    """
    Extract clean financial arrays from scraped Screener data.
    Uses last 5 years of annual data.
    Returns dict with arrays indexed by year.
    """
    tables     = screener_data['tables']
    top_ratios = screener_data['top_ratios']

    # Get P&L table
    pl = tables.get('Profit & Loss', {})
    pl_years = pl.get('years', [])
    pl_rows  = pl.get('rows', {})

    # Get Balance Sheet table
    bs = tables.get('Balance Sheet', {})
    bs_years = bs.get('years', [])
    bs_rows  = bs.get('rows', {})

    # Get Cash Flow table
    cf = tables.get('Cash Flows', {})
    cf_years = cf.get('years', [])
    cf_rows  = cf.get('rows', {})

    # ── Filter to last 5 annual years (exclude TTM, Sep 2025 etc) ──
    def get_annual_years(years, n=5):
        annual = [y for y in years if y.startswith('Mar') or y.startswith('Dec')
                  and not y in ['TTM']]
        # Take last n
        return annual[-n:] if len(annual) >= n else annual

    pl_annual = get_annual_years(pl_years, 6)  # 6 for CAGR calculation
    cf_annual = get_annual_years(cf_years, 6)

    # ── Extract arrays ──────────────────────────────────────────
    def get_row_values(rows, row_name, years):
        row = rows.get(row_name, {})
        return [row.get(y) for y in years]

    revenue     = get_row_values(pl_rows, 'Sales+',           pl_annual)
    ebit        = get_row_values(pl_rows, 'Operating Profit',  pl_annual)
    interest    = get_row_values(pl_rows, 'Interest',          pl_annual)
    depreciation= get_row_values(pl_rows, 'Depreciation',      pl_annual)
    pbt         = get_row_values(pl_rows, 'Profit before tax', pl_annual)
    tax_pct     = get_row_values(pl_rows, 'Tax %',             pl_annual)
    net_profit  = get_row_values(pl_rows, 'Net Profit+',       pl_annual)

    # Borrowings — try multiple row names (banks use 'Borrowing', others use 'Borrowings+')
    bs_annual_years = get_annual_years(bs_years, 6)
    borrowings = get_row_values(bs_rows, 'Borrowings+', bs_annual_years)
    if all(v is None for v in borrowings):
        borrowings = get_row_values(bs_rows, 'Borrowing', bs_annual_years)
    if all(v is None for v in borrowings):
        borrowings = get_row_values(bs_rows, 'Total Debt', bs_annual_years)
    equity_cap  = get_row_values(bs_rows, 'Equity Capital', bs_annual_years)
    reserves    = get_row_values(bs_rows, 'Reserves',       bs_annual_years)

    op_cf       = get_row_values(cf_rows, 'Cash from Operating Activity+', cf_annual)
    inv_cf      = get_row_values(cf_rows, 'Cash from Investing Activity+', cf_annual)

    # ── Tax rate — 3 year average ───────────────────────────────
    tax_vals = [t for t in tax_pct[-3:] if t is not None and 0 < t < 80]
    tax_rate = (sum(tax_vals) / len(tax_vals) / 100) if tax_vals else 0.25

    # ── Top ratios ──────────────────────────────────────────────
    def parse_ratio(key):
        val = top_ratios.get(key, '')
        if not val:
            return None
        return clean_number(val)

    current_price = parse_ratio('Current Price')
    market_cap    = parse_ratio('Market Cap')   # in Cr
    book_value    = parse_ratio('Book Value')
    pe_ratio      = parse_ratio('Stock P/E')
    roe           = parse_ratio('ROE')
    roce          = parse_ratio('ROCE')
    face_value    = parse_ratio('Face Value')

    # Shares outstanding = Market Cap (Cr) × 1e7 / Price
    shares = None
    if market_cap and current_price and current_price > 0:
        shares = (market_cap * 1e7) / current_price  # total shares

    # Most recent debt and equity book value
    recent_debt   = next((v for v in reversed(borrowings) if v is not None), 0) or 0
    recent_eq_cap = next((v for v in reversed(equity_cap) if v is not None), 0) or 0
    recent_res    = next((v for v in reversed(reserves)   if v is not None), 0) or 0
    book_equity   = recent_eq_cap + recent_res  # in Cr

    # Cash — try to get from balance sheet Other Assets or Cash row
    bs_annual = get_annual_years(bs_years, 6)
    cash_rows = ['Cash & Equivalents', 'Cash and Cash Equivalents',
                 'Cash & Bank Balances', 'Cash']
    recent_cash = 0
    for cr in cash_rows:
        cash_data = get_row_values(bs_rows, cr, bs_annual)
        val = next((v for v in reversed(cash_data) if v is not None), None)
        if val:
            recent_cash = val
            break

    # Net debt = Total Borrowings - Cash
    net_debt = max(recent_debt - recent_cash, 0)  # floor at 0

    # Recent interest and ebit
    recent_interest = next((v for v in reversed(interest) if v is not None), 0) or 0
    recent_ebit     = next((v for v in reversed(ebit)     if v is not None), None)

    return {
        'years':         pl_annual,
        'cf_years':      cf_annual,
        'revenue':       revenue,
        'ebit':          ebit,
        'interest':      interest,
        'depreciation':  depreciation,
        'pbt':           pbt,
        'tax_pct':       tax_pct,
        'net_profit':    net_profit,
        'borrowings':    borrowings,
        'op_cf':         op_cf,
        'inv_cf':        inv_cf,
        'tax_rate':      tax_rate,
        'current_price': current_price,
        'market_cap_cr': market_cap,
        'book_value':    book_value,
        'pe_ratio':      pe_ratio,
        'roe':           roe,
        'roce':          roce,
        'face_value':    face_value,
        'shares':        shares,
        'recent_debt':   recent_debt,
        'recent_cash':   recent_cash,
        'net_debt':      net_debt,
        'book_equity':   book_equity,
        'recent_interest': recent_interest,
        'recent_ebit':   recent_ebit,
    }


# ── STEP 3: CALCULATE FCFF ───────────────────────────────────────────────────

def calculate_fcff(fin):
    """
    Calculate historical FCFF for each year.
    FCFF = EBIT × (1 - tax_rate) + D&A - Normalized CapEx

    CapEx = |Cash from Investing Activity| per year
    Normalized CapEx = min(actual, 3yr average) to avoid
    distortion from one-off acquisitions.
    All values in Cr.
    """
    years   = fin['cf_years']
    results = []

    # Pre-calculate CapEx normalization benchmark
    # Use median of all years — more robust than average for outlier detection
    inv_vals_all = sorted([abs(v) for v in fin['inv_cf'] if v is not None])
    if len(inv_vals_all) >= 4:
        # Median of all available years
        mid = len(inv_vals_all) // 2
        median_capex = (inv_vals_all[mid-1] + inv_vals_all[mid]) / 2
    elif inv_vals_all:
        median_capex = sum(inv_vals_all) / len(inv_vals_all)
    else:
        median_capex = None

    for i, yr in enumerate(years):
        # Match year in P&L
        pl_idx = None
        for j, py in enumerate(fin['years']):
            if py == yr:
                pl_idx = j
                break

        if pl_idx is None:
            continue

        ebit_val = fin['ebit'][pl_idx]         if pl_idx < len(fin['ebit'])         else None
        da_val   = fin['depreciation'][pl_idx] if pl_idx < len(fin['depreciation'])  else None
        inv_val  = fin['inv_cf'][i]             if i < len(fin['inv_cf'])             else None

        if ebit_val is None:
            continue

        tax   = fin['tax_rate']
        nopat = ebit_val * (1 - tax)
        da    = da_val if da_val is not None else 0

        # Normalize CapEx — cap at 1.5× median to smooth acquisition spikes
        actual_capex = abs(inv_val) if inv_val is not None else 0
        if median_capex and actual_capex > 1.5 * median_capex:
            capex      = round(median_capex * 1.2, 2)  # allow 20% above median
            capex_note = f'normalized (actual {actual_capex:.0f} Cr capped at {capex:.0f} Cr)'
        else:
            capex      = actual_capex
            capex_note = 'actual'

        fcff = nopat + da - capex

        results.append({
            'year':       yr,
            'revenue':    fin['revenue'][pl_idx] if pl_idx < len(fin['revenue']) else None,
            'ebit':       round(ebit_val, 2),
            'nopat':      round(nopat, 2),
            'da':         round(da, 2),
            'capex':      round(capex, 2),
            'capex_note': capex_note,
            'fcff':       round(fcff, 2),
        })

    return results


# ── STEP 4: WACC CALCULATOR ──────────────────────────────────────────────────

def calculate_wacc(fin, sector, rfr_override=None, wacc_override=None):
    """
    Calculate WACC using:
    - Damodaran unlevered beta + Hamada re-levering
    - Actual cost of debt from financials
    - Actual capital structure from market cap + borrowings
    """
    tax_rate = fin['tax_rate']

    # ── Risk Free Rate ──────────────────────────────────────────
    rfr = (rfr_override / 100) if rfr_override else (RFR_INDIA / 100)
    rfr_source = f'User override: {rfr*100:.2f}%' if rfr_override else \
                 f'RBI 10Y G-Sec yield: {RFR_INDIA}% (last updated {RFR_LAST_UPDATED})'

    # ── Beta ────────────────────────────────────────────────────
    unlevered_beta  = get_unlevered_beta(sector)
    market_cap_cr   = fin['market_cap_cr'] or 0
    debt_cr         = fin['recent_debt']   or 0
    book_equity_cr  = fin['book_equity']   or market_cap_cr

    # Use book equity for D/E in Hamada (standard practice)
    levered_beta = hamada_relever(unlevered_beta, tax_rate, debt_cr, book_equity_cr)

    # ── Cost of Equity (CAPM) ───────────────────────────────────
    erp = ERP_INDIA / 100
    coe = rfr + levered_beta * erp

    # ── Cost of Debt ────────────────────────────────────────────
    recent_interest = fin['recent_interest'] or 0
    if debt_cr > 0 and recent_interest > 0:
        kd        = recent_interest / debt_cr
        kd_source = f'Interest Expense ({recent_interest:.0f} Cr) / Total Debt ({debt_cr:.0f} Cr)'
        # Sanity check — cap between 4% and 20%
        kd = max(min(kd, 0.20), 0.04)
    else:
        kd        = 0.09  # fallback
        kd_source = 'Estimated 9% — insufficient debt data'

    kd_aftertax = kd * (1 - tax_rate)

    # ── Capital Structure ───────────────────────────────────────
    total_v = market_cap_cr + debt_cr
    if total_v > 0:
        we = market_cap_cr / total_v
        wd = debt_cr       / total_v
        cs_source = f'Market Cap / (Market Cap + Borrowings) = {market_cap_cr:,.0f} / {market_cap_cr + debt_cr:,.0f}'
    else:
        we = 0.8
        wd = 0.2
        cs_source = 'Estimated (market cap unavailable)'

    # ── Final WACC ──────────────────────────────────────────────
    if wacc_override:
        wacc      = wacc_override / 100
        wacc_note = f'User override: {wacc_override}%'
    else:
        wacc      = we * coe + wd * kd_aftertax
        wacc      = max(wacc, 0.08)  # floor at 8%
        wacc_note = 'WACC = (E/V)×Ke + (D/V)×Kd×(1-t)'

    return {
        'rfr':              round(rfr * 100, 2),
        'rfr_source':       rfr_source,
        'unlevered_beta':   round(unlevered_beta, 3),
        'sector':           sector,
        'de_ratio':         round(debt_cr / book_equity_cr, 3) if book_equity_cr > 0 else 0,
        'levered_beta':     round(levered_beta, 3),
        'beta_formula':     f'βL = {unlevered_beta} × (1 + (1-{tax_rate:.2f}) × {debt_cr:.0f}/{book_equity_cr:.0f})',
        'erp':              round(erp * 100, 2),
        'erp_source':       'Damodaran India ERP — January 2025',
        'cost_of_equity':   round(coe * 100, 2),
        'coe_formula':      f'Ke = {rfr*100:.2f}% + {levered_beta:.3f} × {erp*100:.2f}%',
        'interest_cr':      round(recent_interest, 2),
        'debt_cr':          round(debt_cr, 2),
        'cost_of_debt':     round(kd * 100, 2),
        'kd_source':        kd_source,
        'tax_rate':         round(tax_rate * 100, 2),
        'tax_source':       '3-year average effective tax rate from P&L',
        'kd_aftertax':      round(kd_aftertax * 100, 2),
        'equity_weight':    round(we * 100, 2),
        'debt_weight':      round(wd * 100, 2),
        'cs_source':        cs_source,
        'market_cap_cr':    round(market_cap_cr, 2),
        'wacc':             round(wacc * 100, 2),
        'wacc_decimal':     round(wacc, 4),
        'wacc_note':        wacc_note,
    }


# ── STEP 5: DCF ENGINE ───────────────────────────────────────────────────────

def run_dcf(historical_fcff, wacc_data, fin, tgr_pct, years=5):
    """
    Run 3-scenario DCF:
    Bear = 5yr FCFF CAGR
    Base = 3yr FCFF CAGR
    Bull = last 1yr FCF growth

    Returns full projection tables + IV per share for each scenario.
    """
    wacc    = wacc_data['wacc_decimal']
    tgr     = min(tgr_pct / 100, 0.065)  # cap at 6.5%
    shares  = fin['shares']

    if not shares or shares <= 0:
        return None, 'Could not determine shares outstanding'

    if wacc <= tgr:
        return None, f'WACC ({wacc*100:.1f}%) must be greater than TGR ({tgr*100:.1f}%)'

    # ── Extract historical FCFF values ──────────────────────────
    fcff_vals  = [r['fcff'] for r in historical_fcff]
    base_fcff  = fcff_vals[-1] if fcff_vals else None

    if base_fcff is None:
        return None, 'No FCFF data available'

    # ── Calculate growth rates ──────────────────────────────────
    # Bear: 5yr CAGR
    bear_growth = safe_cagr(fcff_vals, len(fcff_vals) - 1)

    # Base: 3yr CAGR
    last3 = fcff_vals[-3:] if len(fcff_vals) >= 3 else fcff_vals
    base_growth = safe_cagr(last3, len(last3) - 1)

    # Bull: last 1yr growth
    if len(fcff_vals) >= 2 and fcff_vals[-2] and fcff_vals[-2] > 0:
        bull_growth = (fcff_vals[-1] - fcff_vals[-2]) / fcff_vals[-2]
        bull_growth = max(min(bull_growth, 0.40), -0.10)
    else:
        bull_growth = base_growth * 1.3

    scenarios = {
        'bear': {'growth': bear_growth, 'label': 'Bear Case'},
        'base': {'growth': base_growth, 'label': 'Base Case'},
        'bull': {'growth': bull_growth, 'label': 'Bull Case'},
    }

    results = {}

    for scenario_key, scenario in scenarios.items():
        g = scenario['growth']

        # ── Project FCFs ────────────────────────────────────────
        projections = []
        current_fcff = base_fcff

        for yr in range(1, years + 1):
            projected_fcff = current_fcff * (1 + g)
            pv_fcff        = projected_fcff / ((1 + wacc) ** yr)
            projections.append({
                'year':           f'Year {yr}',
                'growth_rate':    round(g * 100, 2),
                'fcff':           round(projected_fcff, 2),
                'pv_factor':      round(1 / ((1 + wacc) ** yr), 4),
                'pv_fcff':        round(pv_fcff, 2),
            })
            current_fcff = projected_fcff

        sum_pv_fcfs = sum(p['pv_fcff'] for p in projections)

        # ── Terminal Value ──────────────────────────────────────
        terminal_fcff  = projections[-1]['fcff'] * (1 + tgr)
        terminal_value = terminal_fcff / (wacc - tgr)
        pv_terminal    = terminal_value / ((1 + wacc) ** years)
        tv_pct_of_ev   = (pv_terminal / (sum_pv_fcfs + pv_terminal)) * 100 if (sum_pv_fcfs + pv_terminal) > 0 else 0

        # ── Enterprise to Equity Bridge ─────────────────────────
        enterprise_value = sum_pv_fcfs + pv_terminal
        net_debt         = fin['net_debt']
        equity_value     = enterprise_value - net_debt

        # Handle negative equity value
        if equity_value <= 0:
            iv_per_share  = 0
            equity_warning = True
        else:
            iv_per_share   = (equity_value * 1e7) / shares
            equity_warning = False

        results[scenario_key] = {
            'label':           scenario['label'],
            'growth_rate':     round(g * 100, 2),
            'projections':     projections,
            'sum_pv_fcfs':     round(sum_pv_fcfs, 2),
            'terminal_fcff':   round(terminal_fcff, 2),
            'terminal_value':  round(terminal_value, 2),
            'pv_terminal':     round(pv_terminal, 2),
            'tv_pct_of_ev':    round(tv_pct_of_ev, 2),
            'enterprise_value':round(enterprise_value, 2),
            'net_debt':        round(net_debt, 2),
            'equity_value':    round(equity_value, 2),
            'iv_per_share':    round(iv_per_share, 2),
            'tv_warning':      tv_pct_of_ev > 70,
            'equity_warning':  equity_warning,
        }

    # Build growth rate explanations
    hist_years = [r['year'] for r in historical_fcff]
    fcff_labels = [r['year'] for r in historical_fcff if r['fcff'] is not None]
    bear_label = f"{fcff_labels[0]} to {fcff_labels[-1]} ({len(fcff_labels)-1}yr CAGR)" if len(fcff_labels) >= 2 else "5yr CAGR"
    base_label = f"{fcff_labels[-3]} to {fcff_labels[-1]} (3yr CAGR)" if len(fcff_labels) >= 3 else "3yr CAGR"
    bull_label = f"{fcff_labels[-2]} to {fcff_labels[-1]} (1yr growth)" if len(fcff_labels) >= 2 else "1yr growth"

    return {
        'scenarios':    results,
        'tgr':          round(tgr * 100, 2),
        'wacc':         round(wacc * 100, 2),
        'years':        years,
        'base_fcff_cr':    round(base_fcff, 2),
        'bear_growth':     round(bear_growth * 100, 2),
        'base_growth':     round(base_growth * 100, 2),
        'bull_growth':     round(bull_growth * 100, 2),
        'bear_growth_label': bear_label,
        'base_growth_label': base_label,
        'bull_growth_label': bull_label,
        'shares_cr':       round(shares / 1e7, 2),
    }, None


# ── STEP 6: SENSITIVITY TABLE ────────────────────────────────────────────────

def build_sensitivity(historical_fcff, fin, wacc_base, tgr_base, years=5):
    """
    Build 3×3 sensitivity grid.
    Rows: WACC ± 1%
    Cols: TGR ± 0.5%
    """
    shares = fin['shares']
    if not shares or shares <= 0:
        return None

    fcff_vals   = [r['fcff'] for r in historical_fcff]
    base_fcff   = fcff_vals[-1] if fcff_vals else None
    if not base_fcff:
        return None

    last3       = fcff_vals[-3:] if len(fcff_vals) >= 3 else fcff_vals
    base_growth = safe_cagr(last3, len(last3) - 1)

    wacc_range = [wacc_base - 1, wacc_base, wacc_base + 1]
    tgr_range  = [tgr_base - 0.5, tgr_base, tgr_base + 0.5]

    grid = []
    for wacc_pct in wacc_range:
        row = []
        for tgr_pct in tgr_range:
            wacc = wacc_pct / 100
            tgr  = min(tgr_pct / 100, 0.065)
            if wacc <= tgr or wacc <= 0:
                row.append(None)
                continue
            # Project FCF
            current = base_fcff
            pv_sum  = 0
            for yr in range(1, years + 1):
                current = current * (1 + base_growth)
                pv_sum += current / ((1 + wacc) ** yr)
            # Terminal
            tv    = current * (1 + tgr) / (wacc - tgr)
            pv_tv = tv / ((1 + wacc) ** years)
            ev    = pv_sum + pv_tv
            eq    = ev - fin['net_debt']
            if eq <= 0:
                row.append(0)
                continue
            iv    = (eq * 1e7) / shares
            row.append(round(iv, 0))
        grid.append(row)

    return {
        'wacc_range': [round(w, 1) for w in wacc_range],
        'tgr_range':  [round(t, 1) for t in tgr_range],
        'grid':       grid,
    }


# ── VERDICT ──────────────────────────────────────────────────────────────────

def get_verdict(iv, cmp):
    """Return verdict and upside/downside %."""
    if not iv or not cmp or cmp <= 0:
        return 'N/A', 0
    upside = ((iv - cmp) / cmp) * 100
    if upside > 15:
        verdict = 'UNDERVALUED'
    elif upside < -15:
        verdict = 'OVERVALUED'
    else:
        verdict = 'FAIRLY VALUED'
    return verdict, round(upside, 2)


# ── WARNINGS & ASSUMPTIONS ───────────────────────────────────────────────────

def build_warnings(fin, wacc_data, dcf_result, historical_fcff, sector):
    warnings = []

    # 1. CapEx normalization
    normalized_years = [r for r in historical_fcff if r.get('capex_note','').startswith('normalized')]
    if normalized_years:
        yrs = ', '.join(r['year'] for r in normalized_years)
        warnings.append({
            'level': 'caution', 'code': 'CAPEX_NORMALIZED',
            'title': 'CapEx Normalized in Some Years',
            'message': (
                f"Investing cash outflow in {yrs} was unusually high (likely acquisitions or one-time investments). "
                f"We capped it at 1.2x the historical median to avoid understating Free Cash Flow. "
                f"Actual figures are shown in the Historical FCFF table. "
                f"This may overstate FCFF if the company is genuinely in a heavy investment phase."
            )
        })

    # 2. CapEx proxy (always)
    warnings.append({
        'level': 'info', 'code': 'CAPEX_PROXY',
        'title': 'CapEx is a Proxy, Not Actual',
        'message': (
            "CapEx is estimated from total Cash from Investing Activities — this includes acquisitions, "
            "investments in subsidiaries, and asset sales, not just capital expenditure. "
            "Screener.in does not provide standalone CapEx. This may overstate or understate true CapEx."
        )
    })

    # 3. Working capital omitted (always)
    warnings.append({
        'level': 'info', 'code': 'NWC_OMITTED',
        'title': 'Change in Working Capital Excluded',
        'message': (
            "FCFF = EBIT x (1-Tax) + D&A - CapEx. Change in Net Working Capital (ΔNWC) is excluded "
            "because Screener.in does not provide clean current asset/liability breakdowns. "
            "For working-capital-intensive sectors (retail, FMCG, manufacturing), this may overstate FCFF."
        )
    })

    # 4. RFR hardcoded (always)
    warnings.append({
        'level': 'info', 'code': 'RFR_HARDCODED',
        'title': 'Risk-Free Rate is Not Live',
        'message': (
            f"RFR is set to {RFR_INDIA}% (RBI 10Y G-Sec yield, last updated {RFR_LAST_UPDATED}). "
            f"No reliable free API exists for live Indian bond yields. Updated quarterly. "
            f"Override this in Advanced Settings if needed."
        )
    })

    # 5. Beta sector-based (always)
    warnings.append({
        'level': 'info', 'code': 'BETA_SECTOR',
        'title': 'Beta is Sector-Based, Not Stock-Specific',
        'message': (
            f"Unlevered Beta ({wacc_data['unlevered_beta']}) is from Damodaran's Jan 2025 India sector table "
            f"for '{wacc_data['sector']}', re-levered using actual D/E via Hamada equation. "
            f"More theoretically sound than a 5-year price regression, but does not reflect company-specific risk. "
            f"You can override WACC manually in Advanced Settings."
        )
    })

    # 6. ERP hardcoded (always)
    warnings.append({
        'level': 'info', 'code': 'ERP_HARDCODED',
        'title': 'Equity Risk Premium is Fixed at Damodaran 2025',
        'message': (
            f"ERP = {ERP_INDIA}% — Damodaran India estimate, January 2025. Updates once per year. "
            f"A higher ERP lowers IV; a lower ERP raises IV. "
            f"This reflects country risk, market volatility, and macro conditions."
        )
    })

    # 7. Cash not available
    if fin['recent_cash'] == 0 and fin['recent_debt'] > 0:
        warnings.append({
            'level': 'caution', 'code': 'CASH_UNAVAILABLE',
            'title': 'Cash Balance Not Found — Using Gross Debt as Net Debt',
            'message': (
                f"Cash & equivalents could not be extracted from Screener.in balance sheet. "
                f"Net Debt equals Gross Debt ({fin['recent_debt']:,.0f} Cr), which understates equity value. "
                f"For cash-rich companies, actual IV per share may be higher than shown."
            )
        })

    # 8. Terminal value dominance
    for sk in ['bear', 'base', 'bull']:
        s = dcf_result['scenarios'][sk]
        if s['tv_warning']:
            warnings.append({
                'level': 'caution', 'code': f'TV_DOMINANCE_{sk.upper()}',
                'title': f'Terminal Value Dominates — {sk.title()} Case ({s["tv_pct_of_ev"]:.1f}%)',
                'message': (
                    f"Terminal value is {s['tv_pct_of_ev']:.1f}% of Enterprise Value in the {sk.title()} Case "
                    f"(threshold: 70%). Most of the valuation rests on long-term growth assumptions (TGR). "
                    f"Small changes in TGR will significantly change IV per share — check the Sensitivity Table."
                )
            })

    # 9. Negative equity
    for sk in ['bear', 'base', 'bull']:
        s = dcf_result['scenarios'][sk]
        if s.get('equity_warning'):
            warnings.append({
                'level': 'important', 'code': f'NEGATIVE_EQUITY_{sk.upper()}',
                'title': f'Enterprise Value < Net Debt — {sk.title()} Case',
                'message': (
                    f"In the {sk.title()} Case, EV is less than Net Debt, producing negative equity. "
                    f"Shown as Rs 0. May indicate over-leverage or financial subsidiary debt distorting the balance sheet. "
                    f"Use this result with extreme caution."
                )
            })

    # 10. Financial subsidiary debt
    if sector == 'Infrastructure / Const' and fin['recent_debt'] > 50000:
        warnings.append({
            'level': 'important', 'code': 'FINANCIAL_SUBSIDIARY_DEBT',
            'title': 'Borrowings May Include Financial Subsidiary Debt',
            'message': (
                f"Total Borrowings of {fin['recent_debt']:,.0f} Cr is very high for an infra/engineering company. "
                f"This likely includes debt from financial services subsidiaries (e.g. L&T Finance) "
                f"which borrow to lend — not typical corporate debt. "
                f"This inflates Net Debt and suppresses equity value in the DCF. "
                f"Consider standalone (non-consolidated) financials for a cleaner picture."
            )
        })

    # 11. Tax rate averaged (always)
    warnings.append({
        'level': 'info', 'code': 'TAX_AVERAGED',
        'title': 'Tax Rate is a 3-Year Average',
        'message': (
            f"Effective tax rate used: {wacc_data['tax_rate']:.1f}% — 3-year average from annual P&L. "
            f"Single-year rates can be distorted by deferred tax, MAT credits, or loss years. "
            f"Averaging smooths this volatility."
        )
    })

    # 12. Growth rates historical (always)
    warnings.append({
        'level': 'info', 'code': 'GROWTH_HISTORICAL',
        'title': 'Growth Rates Are Purely Historical',
        'message': (
            "Bear/Base/Bull growth rates are derived from historical FCFF CAGR over 5, 3, and 1 years respectively. "
            "Past growth does not guarantee future performance. "
            "Industry cycles, competition, regulation, and macro conditions can significantly alter future cash flows."
        )
    })

    return warnings


def build_assumptions(fin, wacc_data):
    return [
        {'parameter': 'Risk-Free Rate (Rf)',       'value': f"{wacc_data['rfr']}%",             'source': f"RBI 10Y G-Sec yield — last updated {RFR_LAST_UPDATED}", 'type': 'hardcoded'},
        {'parameter': 'Equity Risk Premium (ERP)', 'value': f"{wacc_data['erp']}%",             'source': 'Damodaran India ERP — January 2025', 'type': 'hardcoded'},
        {'parameter': 'Unlevered Beta (BU)',        'value': str(wacc_data['unlevered_beta']),  'source': f"Damodaran India sector table — '{wacc_data['sector']}' (Jan 2025)", 'type': 'sector_table'},
        {'parameter': 'Levered Beta (BL)',          'value': str(wacc_data['levered_beta']),    'source': 'Hamada: BL = BU x (1 + (1-t) x D/E)', 'type': 'calculated'},
        {'parameter': 'D/E Ratio for Beta',         'value': str(wacc_data['de_ratio']),        'source': 'Book Equity (Equity Capital + Reserves) from Screener.in Balance Sheet', 'type': 'calculated'},
        {'parameter': 'Cost of Equity (Ke)',        'value': f"{wacc_data['cost_of_equity']}%", 'source': f"CAPM: Ke = Rf + BL x ERP", 'type': 'calculated'},
        {'parameter': 'Cost of Debt (Kd)',          'value': f"{wacc_data['cost_of_debt']}%",   'source': wacc_data['kd_source'], 'type': 'calculated'},
        {'parameter': 'After-Tax Cost of Debt',     'value': f"{wacc_data['kd_aftertax']}%",   'source': f"Kd x (1 - Tax Rate)", 'type': 'calculated'},
        {'parameter': 'Capital Structure',          'value': f"{wacc_data['equity_weight']}% E / {wacc_data['debt_weight']}% D", 'source': wacc_data['cs_source'], 'type': 'calculated'},
        {'parameter': 'Effective Tax Rate',         'value': f"{wacc_data['tax_rate']}%",       'source': '3-year average from Screener.in P&L Tax % row', 'type': 'calculated'},
        {'parameter': 'WACC',                       'value': f"{wacc_data['wacc']}%",           'source': 'WACC = (E/V) x Ke + (D/V) x Kd x (1-t)', 'type': 'calculated'},
        {'parameter': 'FCFF Formula',               'value': 'EBIT x (1-t) + D&A - CapEx',     'source': 'Standard FCFF. ΔNWC excluded — unavailable from Screener.in', 'type': 'model_assumption'},
        {'parameter': 'CapEx Source',               'value': '|Cash from Investing Activity|',  'source': 'Proxy. Capped at 1.2x median to normalize acquisition years.', 'type': 'model_assumption'},
        {'parameter': 'Net Debt',                   'value': f"Rs {fin['net_debt']:,.0f} Cr",   'source': f"Borrowings ({fin['recent_debt']:,.0f} Cr) minus Cash ({fin['recent_cash']:,.0f} Cr)", 'type': 'calculated'},
        {'parameter': 'Shares Outstanding',         'value': f"{round(fin['shares']/1e7,2) if fin['shares'] else 'N/A'} Cr", 'source': 'Market Cap / Current Price from Screener.in top ratios', 'type': 'calculated'},
        {'parameter': 'TGR Cap',                    'value': '6.5% maximum',                   'source': 'Capped at estimated long-run India GDP growth rate', 'type': 'model_assumption'},
        {'parameter': 'WACC Floor',                 'value': '8% minimum',                     'source': 'Prevents unrealistically low discount rates', 'type': 'model_assumption'},
        {'parameter': 'Data Source',                'value': 'Screener.in consolidated',        'source': 'Consolidated P&L, Balance Sheet, Cash Flows. Includes subsidiaries.', 'type': 'data_source'},
    ]



# ── DAMODARAN SECTOR MULTIPLES (India, January 2025) ─────────────────────────
# Source: Damodaran Online — http://pages.stern.nyu.edu/~adamodar/
# Updated: January 2025

DAMODARAN_PE = {
    'IT / Software':           28.5,
    'Pharma / Healthcare':     30.2,
    'FMCG / Consumer':         52.0,
    'Banking / Finance':       18.0,
    'Infrastructure / Const':  22.0,
    'Steel / Metals':          12.5,
    'Auto / Auto Ancillary':   22.0,
    'Energy / Oil & Gas':      10.5,
    'Telecom':                 20.0,
    'Cement':                  32.0,
    'Real Estate':             28.0,
    'Diversified':             24.0,
    'General / Other':         25.0,
}

DAMODARAN_EV_EBITDA = {
    'IT / Software':           20.0,
    'Pharma / Healthcare':     18.0,
    'FMCG / Consumer':         35.0,
    'Banking / Finance':       None,   # Not applicable
    'Infrastructure / Const':  12.0,
    'Steel / Metals':           7.5,
    'Auto / Auto Ancillary':   12.0,
    'Energy / Oil & Gas':       8.0,
    'Telecom':                 10.0,
    'Cement':                  14.0,
    'Real Estate':             18.0,
    'Diversified':             14.0,
    'General / Other':         14.0,
}

DAMODARAN_PB = {
    'IT / Software':            8.0,
    'Pharma / Healthcare':      4.5,
    'FMCG / Consumer':         12.0,
    'Banking / Finance':        2.8,
    'Infrastructure / Const':   2.5,
    'Steel / Metals':           1.8,
    'Auto / Auto Ancillary':    3.5,
    'Energy / Oil & Gas':       1.6,
    'Telecom':                  3.0,
    'Cement':                   4.0,
    'Real Estate':              2.5,
    'Diversified':              3.0,
    'General / Other':          3.0,
}

# Sector weights for composite valuation
# DCF / P/E / EV_EBITDA / P/B
SECTOR_WEIGHTS = {
    'IT / Software':           {'dcf': 0.50, 'pe': 0.30, 'ev_ebitda': 0.20, 'pb': 0.00},
    'Pharma / Healthcare':     {'dcf': 0.45, 'pe': 0.30, 'ev_ebitda': 0.25, 'pb': 0.00},
    'FMCG / Consumer':         {'dcf': 0.40, 'pe': 0.40, 'ev_ebitda': 0.20, 'pb': 0.00},
    'Banking / Finance':       {'dcf': 0.00, 'pe': 0.40, 'ev_ebitda': 0.00, 'pb': 0.60},
    'Infrastructure / Const':  {'dcf': 0.35, 'pe': 0.25, 'ev_ebitda': 0.40, 'pb': 0.00},
    'Steel / Metals':          {'dcf': 0.20, 'pe': 0.20, 'ev_ebitda': 0.60, 'pb': 0.00},
    'Auto / Auto Ancillary':   {'dcf': 0.35, 'pe': 0.30, 'ev_ebitda': 0.35, 'pb': 0.00},
    'Energy / Oil & Gas':      {'dcf': 0.30, 'pe': 0.20, 'ev_ebitda': 0.50, 'pb': 0.00},
    'Telecom':                 {'dcf': 0.25, 'pe': 0.20, 'ev_ebitda': 0.55, 'pb': 0.00},
    'Cement':                  {'dcf': 0.25, 'pe': 0.20, 'ev_ebitda': 0.55, 'pb': 0.00},
    'Real Estate':             {'dcf': 0.35, 'pe': 0.25, 'ev_ebitda': 0.40, 'pb': 0.00},
    'Diversified':             {'dcf': 0.40, 'pe': 0.30, 'ev_ebitda': 0.30, 'pb': 0.00},
    'General / Other':         {'dcf': 0.40, 'pe': 0.30, 'ev_ebitda': 0.30, 'pb': 0.00},
}

DAMODARAN_LAST_UPDATED = 'January 2025'


# ── STEP 8: P/E BASED VALUATION ──────────────────────────────────────────────

def calculate_pe_valuation(fin, sector, is_bank=False):
    """
    P/E Based Valuation:
    IV = Normalized EPS × Sector Median P/E (Damodaran)
    EPS normalized as 3-year average net profit / shares
    """
    result = {
        'applicable': True,
        'na_reason': None,
        'sector_pe': None,
        'normalized_eps': None,
        'iv_per_share': None,
        'upside': None,
        'verdict': None,
        'method': 'Normalized EPS × Sector Median P/E',
        'warnings': [],
        'assumptions': [],
    }

    shares = fin.get('shares')
    cmp = fin.get('current_price')
    net_profit = fin.get('net_profit', [])
    sector_pe = DAMODARAN_PE.get(sector, DAMODARAN_PE['General / Other'])

    result['sector_pe'] = sector_pe

    # Check applicability
    if not shares or shares <= 0:
        result['applicable'] = False
        result['na_reason'] = 'Shares outstanding could not be determined.'
        return result

    if not cmp or cmp <= 0:
        result['applicable'] = False
        result['na_reason'] = 'Current market price unavailable.'
        return result

    # Normalize EPS — 3 year average net profit
    valid_profits = [v for v in net_profit[-3:] if v is not None]
    if not valid_profits:
        result['applicable'] = False
        result['na_reason'] = 'Net profit data unavailable from Screener.in'
        return result

    # Check for losses
    if all(v <= 0 for v in valid_profits):
        result['applicable'] = False
        result['na_reason'] = 'Company has reported losses in all recent years. P/E valuation not meaningful for loss-making companies.'
        return result

    # Use only profitable years for normalization
    profitable_profits = [v for v in valid_profits if v > 0]
    avg_profit_cr = sum(profitable_profits) / len(profitable_profits)
    normalized_eps = round((avg_profit_cr * 1e7) / shares, 2)

    if normalized_eps <= 0:
        result['applicable'] = False
        result['na_reason'] = 'Normalized EPS is zero or negative.'
        return result

    iv = round(normalized_eps * sector_pe, 2)
    upside = round(((iv - cmp) / cmp) * 100, 2)
    verdict = 'UNDERVALUED' if upside > 15 else 'OVERVALUED' if upside < -15 else 'FAIRLY VALUED'

    result['normalized_eps'] = normalized_eps
    result['iv_per_share'] = iv
    result['upside'] = upside
    result['verdict'] = verdict
    result['avg_profit_cr'] = round(avg_profit_cr, 2)
    result['years_used'] = len(profitable_profits)

    # Warnings
    if len(profitable_profits) < len(valid_profits):
        result['warnings'].append({
            'title': 'Loss Years Excluded from EPS Normalization',
            'message': f'{len(valid_profits) - len(profitable_profits)} loss year(s) excluded from EPS average. P/E valuation uses only profitable years.',
            'level': 'caution'
        })

    if sector in ['Steel / Metals', 'Energy / Oil & Gas', 'Cement']:
        result['warnings'].append({
            'title': 'P/E Less Reliable for Cyclical Sector',
            'message': f'{sector} is a cyclical sector. Earnings fluctuate with commodity cycles. P/E valuation may overstate or understate IV significantly.',
            'level': 'caution'
        })

    result['warnings'].append({
        'title': 'Sector Median P/E May Not Reflect Company Premium/Discount',
        'message': f'Using Damodaran sector median P/E of {sector_pe}x for {sector}. High-quality companies often trade at a premium to sector median. Adjust manually if needed.',
        'level': 'info'
    })

    # Assumptions
    result['assumptions'] = [
        {'parameter': 'Sector Median P/E', 'value': f'{sector_pe}x', 'source': f'Damodaran India — {sector} ({DAMODARAN_LAST_UPDATED})', 'type': 'sector_table'},
        {'parameter': 'Normalized EPS', 'value': f'₹{normalized_eps}', 'source': f'{len(profitable_profits)}-year average net profit ÷ shares outstanding', 'type': 'calculated'},
        {'parameter': 'Net Profit Used', 'value': f'₹{avg_profit_cr:,.0f} Cr (avg)', 'source': 'Screener.in P&L — Net Profit+ row', 'type': 'data_source'},
    ]

    return result


# ── STEP 9: EV/EBITDA BASED VALUATION ────────────────────────────────────────

def calculate_ev_ebitda_valuation(fin, sector, is_bank=False):
    """
    EV/EBITDA Based Valuation:
    EV = EBITDA × Sector EV/EBITDA Multiple
    Equity Value = EV - Net Debt
    IV = Equity Value / Shares
    """
    result = {
        'applicable': True,
        'na_reason': None,
        'sector_multiple': None,
        'ebitda_cr': None,
        'ev_cr': None,
        'iv_per_share': None,
        'upside': None,
        'verdict': None,
        'method': 'EBITDA × Sector EV/EBITDA Multiple − Net Debt ÷ Shares',
        'warnings': [],
        'assumptions': [],
    }

    if is_bank:
        result['applicable'] = False
        result['na_reason'] = 'EV/EBITDA not applicable to banks — interest income is operating revenue, not a financing cost. EBITDA is not a meaningful metric for financial institutions.'
        return result

    sector_multiple = DAMODARAN_EV_EBITDA.get(sector)
    if sector_multiple is None:
        result['applicable'] = False
        result['na_reason'] = f'EV/EBITDA multiple not available for {sector}.'
        return result

    result['sector_multiple'] = sector_multiple

    shares = fin.get('shares')
    cmp = fin.get('current_price')
    net_debt = fin.get('net_debt', 0)
    recent_ebit = fin.get('recent_ebit')
    depreciation = fin.get('depreciation', [])

    if not shares or shares <= 0 or not cmp:
        result['applicable'] = False
        result['na_reason'] = 'Insufficient data — shares or price unavailable.'
        return result

    if recent_ebit is None:
        result['applicable'] = False
        result['na_reason'] = 'EBIT data unavailable from Screener.in'
        return result

    # EBITDA = EBIT + Depreciation (most recent year)
    recent_da = next((v for v in reversed(depreciation) if v is not None), None)
    if recent_da is None:
        result['applicable'] = False
        result['na_reason'] = 'Depreciation data unavailable — cannot compute EBITDA'
        return result

    ebitda = recent_ebit + recent_da

    if ebitda <= 0:
        result['applicable'] = False
        result['na_reason'] = 'EBITDA is zero or negative. EV/EBITDA valuation not meaningful.'
        return result

    ev = ebitda * sector_multiple
    equity_value = ev - net_debt
    if equity_value <= 0:
        result['applicable'] = False
        result['na_reason'] = 'EV < Net Debt — enterprise value is less than net debt. EV/EBITDA valuation not meaningful.'
        return result

    iv = round((equity_value * 1e7) / shares, 2)
    upside = round(((iv - cmp) / cmp) * 100, 2)
    verdict = 'UNDERVALUED' if upside > 15 else 'OVERVALUED' if upside < -15 else 'FAIRLY VALUED'

    result['ebitda_cr'] = round(ebitda, 2)
    result['ebit_cr'] = round(recent_ebit, 2)
    result['da_cr'] = round(recent_da, 2)
    result['ev_cr'] = round(ev, 2)
    result['equity_value_cr'] = round(equity_value, 2)
    result['net_debt_cr'] = round(net_debt, 2)
    result['iv_per_share'] = iv
    result['upside'] = upside
    result['verdict'] = verdict

    # Warnings
    result['warnings'].append({
        'title': 'EV/EBITDA Ignores CapEx Intensity Differences',
        'message': f'Two companies with same EBITDA but different CapEx needs have the same EV/EBITDA-derived value. Use alongside DCF for capital-heavy sectors.',
        'level': 'caution'
    })

    if sector in ['IT / Software', 'FMCG / Consumer']:
        result['warnings'].append({
            'title': 'EV/EBITDA Less Relevant for Asset-Light Sectors',
            'message': f'{sector} companies are typically asset-light. EV/EBITDA is more meaningful for capital-intensive industries. Weight DCF and P/E more heavily.',
            'level': 'info'
        })

    result['warnings'].append({
        'title': 'Single Year EBITDA Used',
        'message': 'EBITDA is based on the most recent financial year only. A single year may not reflect normalized earnings power. Consider cyclical companies carefully.',
        'level': 'info'
    })

    # Assumptions
    result['assumptions'] = [
        {'parameter': 'Sector EV/EBITDA Multiple', 'value': f'{sector_multiple}x', 'source': f'Damodaran India — {sector} ({DAMODARAN_LAST_UPDATED})', 'type': 'sector_table'},
        {'parameter': 'EBITDA', 'value': f'₹{ebitda:,.0f} Cr', 'source': 'EBIT + Depreciation — most recent year from Screener.in', 'type': 'calculated'},
        {'parameter': 'Net Debt Deducted', 'value': f'₹{net_debt:,.0f} Cr', 'source': 'Borrowings minus Cash from Screener.in Balance Sheet', 'type': 'calculated'},
    ]

    return result


# ── STEP 10: PRICE-TO-BOOK VALUATION ─────────────────────────────────────────

def calculate_pb_valuation(fin, sector, is_bank=False):
    """
    P/B Based Valuation:
    IV = Book Value per Share × Sector Median P/B (Damodaran)
    Primary method for banks. Secondary for others.
    """
    result = {
        'applicable': True,
        'na_reason': None,
        'sector_pb': None,
        'book_value_per_share': None,
        'iv_per_share': None,
        'upside': None,
        'verdict': None,
        'method': 'Book Value per Share × Sector Median P/B',
        'is_primary': is_bank,
        'warnings': [],
        'assumptions': [],
    }

    cmp = fin.get('current_price')
    book_value = fin.get('book_value')  # per share from Screener top ratios
    sector_pb = DAMODARAN_PB.get(sector, DAMODARAN_PB['General / Other'])

    result['sector_pb'] = sector_pb

    if not cmp or cmp <= 0:
        result['applicable'] = False
        result['na_reason'] = 'Current market price unavailable.'
        return result

    if not book_value or book_value <= 0:
        result['applicable'] = False
        result['na_reason'] = 'Book value per share not available from Screener.in'
        return result

    iv = round(book_value * sector_pb, 2)
    upside = round(((iv - cmp) / cmp) * 100, 2)
    verdict = 'UNDERVALUED' if upside > 15 else 'OVERVALUED' if upside < -15 else 'FAIRLY VALUED'

    result['book_value_per_share'] = book_value
    result['iv_per_share'] = iv
    result['upside'] = upside
    result['verdict'] = verdict

    # Warnings
    if not is_bank:
        result['warnings'].append({
            'title': 'P/B Less Relevant for Asset-Light Companies',
            'message': 'Book value understates true value for companies with significant intangible assets (brands, IP, software). P/B is most meaningful for banks, NBFCs, and asset-heavy industries.',
            'level': 'caution'
        })

    result['warnings'].append({
        'title': 'Sector Median P/B May Not Reflect Quality Premium',
        'message': f'Using Damodaran sector median P/B of {sector_pb}x. High-ROE companies typically trade at a significant premium. Current P/B: {round(cmp/book_value, 2)}x.',
        'level': 'info'
    })

    if is_bank:
        result['warnings'].append({
            'title': 'Book Value Quality Depends on NPA Recognition',
            'message': 'Bank book values can be overstated if non-performing assets are inadequately provisioned. Verify NPA ratio and provision coverage before relying on P/B valuation.',
            'level': 'caution'
        })

    # Assumptions
    result['assumptions'] = [
        {'parameter': 'Sector Median P/B', 'value': f'{sector_pb}x', 'source': f'Damodaran India — {sector} ({DAMODARAN_LAST_UPDATED})', 'type': 'sector_table'},
        {'parameter': 'Book Value per Share', 'value': f'₹{book_value}', 'source': 'Screener.in top ratios — Book Value field', 'type': 'data_source'},
        {'parameter': 'Current P/B', 'value': f'{round(cmp/book_value, 2)}x', 'source': 'CMP ÷ Book Value per Share', 'type': 'calculated'},
    ]

    return result


# ── STEP 11: COMPOSITE VALUATION SCORE ───────────────────────────────────────

def calculate_composite_valuation(dcf_weighted_iv, pe_result, ev_ebitda_result, pb_result, fin, sector, is_bank=False):
    """
    Composite valuation combining all methods with sector-based Damodaran weights.
    Bear/Base/Bull composite range also calculated.
    """
    cmp = fin.get('current_price')
    weights = SECTOR_WEIGHTS.get(sector, SECTOR_WEIGHTS['General / Other'])

    methods = {}
    active_methods = {}

    # DCF
    if dcf_weighted_iv and dcf_weighted_iv.get('valid') and dcf_weighted_iv.get('iv'):
        methods['dcf'] = {
            'name': 'Weighted DCF',
            'iv': dcf_weighted_iv['iv'],
            'weight': weights['dcf'],
            'applicable': not is_bank,
            'na_reason': 'DCF not applicable to banks — debt is operating input, not financing.' if is_bank else None,
        }
        if not is_bank and weights['dcf'] > 0:
            active_methods['dcf'] = methods['dcf']
    else:
        methods['dcf'] = {
            'name': 'Weighted DCF',
            'iv': None,
            'weight': weights['dcf'],
            'applicable': False,
            'na_reason': 'DCF IV could not be calculated — insufficient or negative FCF data.',
        }

    # P/E
    methods['pe'] = {
        'name': 'P/E Valuation',
        'iv': pe_result['iv_per_share'] if pe_result['applicable'] else None,
        'weight': weights['pe'],
        'applicable': pe_result['applicable'],
        'na_reason': pe_result['na_reason'] if not pe_result['applicable'] else None,
    }
    if pe_result['applicable'] and weights['pe'] > 0:
        active_methods['pe'] = methods['pe']

    # EV/EBITDA
    methods['ev_ebitda'] = {
        'name': 'EV/EBITDA Valuation',
        'iv': ev_ebitda_result['iv_per_share'] if ev_ebitda_result['applicable'] else None,
        'weight': weights['ev_ebitda'],
        'applicable': ev_ebitda_result['applicable'],
        'na_reason': ev_ebitda_result['na_reason'] if not ev_ebitda_result['applicable'] else None,
    }
    if ev_ebitda_result['applicable'] and weights['ev_ebitda'] > 0:
        active_methods['ev_ebitda'] = methods['ev_ebitda']

    # P/B
    methods['pb'] = {
        'name': 'P/B Valuation',
        'iv': pb_result['iv_per_share'] if pb_result['applicable'] else None,
        'weight': weights['pb'],
        'applicable': pb_result['applicable'],
        'na_reason': pb_result['na_reason'] if not pb_result['applicable'] else None,
    }
    if pb_result['applicable'] and weights['pb'] > 0:
        active_methods['pb'] = methods['pb']

    # Redistribute weights among applicable methods
    total_applicable_weight = sum(m['weight'] for m in active_methods.values())

    if total_applicable_weight == 0 or not active_methods:
        return {
            'applicable': False,
            'na_reason': 'No valuation methods produced valid results.',
            'methods': methods,
            'weights_used': weights,
            'composite_iv': None,
            'composite_upside': None,
            'composite_verdict': None,
        }

    # Normalize weights to sum to 1
    adjusted_weights = {}
    for key, m in active_methods.items():
        adjusted_weights[key] = round(m['weight'] / total_applicable_weight, 4)

    # Composite IV
    composite_iv = round(sum(
        adjusted_weights[k] * active_methods[k]['iv']
        for k in active_methods
    ), 2)

    composite_upside = round(((composite_iv - cmp) / cmp) * 100, 2)
    composite_verdict = ('UNDERVALUED' if composite_upside > 15 else
                         'OVERVALUED' if composite_upside < -15 else 'FAIRLY VALUED')

    # Bear/Bull composite range — use ±15% on composite as approximation
    # More accurately: use bear DCF and low-end multiples
    bear_ivs = []
    bull_ivs = []
    if methods['dcf'].get('applicable') and dcf_weighted_iv:
        # Bear: use bear scenario DCF IV if available
        pass  # Will be passed from caller

    # Simple range: composite ± weighted standard deviation
    all_ivs = [m['iv'] for m in active_methods.values() if m['iv'] is not None]
    if len(all_ivs) >= 2:
        composite_low  = round(min(all_ivs), 2)
        composite_high = round(max(all_ivs), 2)
    else:
        composite_low  = composite_iv
        composite_high = composite_iv

    # Build contribution table
    contributions = {}
    for key in ['dcf', 'pe', 'ev_ebitda', 'pb']:
        m = methods[key]
        if key in adjusted_weights and m['iv'] is not None:
            contributions[key] = {
                'name': m['name'],
                'weight_original': round(weights[key] * 100, 1),
                'weight_adjusted': round(adjusted_weights[key] * 100, 1),
                'iv': m['iv'],
                'contribution': round(adjusted_weights[key] * m['iv'], 2),
                'applicable': True,
                'na_reason': None,
            }
        else:
            contributions[key] = {
                'name': m['name'],
                'weight_original': round(weights[key] * 100, 1),
                'weight_adjusted': 0,
                'iv': None,
                'contribution': 0,
                'applicable': m['applicable'],
                'na_reason': m['na_reason'],
            }

    return {
        'applicable': True,
        'methods': contributions,
        'sector': sector,
        'weights_source': f'Damodaran sector classification — {sector} ({DAMODARAN_LAST_UPDATED})',
        'composite_iv': composite_iv,
        'composite_low': composite_low,
        'composite_high': composite_high,
        'composite_upside': composite_upside,
        'composite_verdict': composite_verdict,
        'weights_note': 'Weights redistributed proportionally among applicable methods only. Original weights from Damodaran sector classification.',
    }


# ── API ENDPOINTS ─────────────────────────────────────────────────────────────

@app.route('/api/health', methods=['GET'])
def health():
    return jsonify({'status': 'ok', 'app': 'SmartValuation v1.0', 'time': datetime.now().isoformat()})

@app.route('/api/clear-cache', methods=['POST'])
def clear_cache():
    _cache.clear()
    return jsonify({'status': 'cache cleared'})

@app.route('/api/search', methods=['GET'])
def search():
    """Quick search by ticker or company name."""
    q = request.args.get('q', '').strip().upper()
    if len(q) < 2:
        return jsonify([])

    COMMON_STOCKS = [
        {'ticker':'RELIANCE','name':'Reliance Industries'},
        {'ticker':'TCS','name':'Tata Consultancy Services'},
        {'ticker':'HDFCBANK','name':'HDFC Bank'},
        {'ticker':'INFY','name':'Infosys'},
        {'ticker':'HINDUNILVR','name':'Hindustan Unilever'},
        {'ticker':'ICICIBANK','name':'ICICI Bank'},
        {'ticker':'BHARTIARTL','name':'Bharti Airtel'},
        {'ticker':'ITC','name':'ITC Limited'},
        {'ticker':'KOTAKBANK','name':'Kotak Mahindra Bank'},
        {'ticker':'LT','name':'Larsen & Toubro'},
        {'ticker':'SBIN','name':'State Bank of India'},
        {'ticker':'AXISBANK','name':'Axis Bank'},
        {'ticker':'ASIANPAINT','name':'Asian Paints'},
        {'ticker':'MARUTI','name':'Maruti Suzuki'},
        {'ticker':'SUNPHARMA','name':'Sun Pharmaceutical'},
        {'ticker':'TATAMOTORS','name':'Tata Motors'},
        {'ticker':'WIPRO','name':'Wipro'},
        {'ticker':'ULTRACEMCO','name':'UltraTech Cement'},
        {'ticker':'TITAN','name':'Titan Company'},
        {'ticker':'BAJFINANCE','name':'Bajaj Finance'},
        {'ticker':'TATASTEEL','name':'Tata Steel'},
        {'ticker':'ONGC','name':'ONGC'},
        {'ticker':'NTPC','name':'NTPC'},
        {'ticker':'POWERGRID','name':'Power Grid Corporation'},
        {'ticker':'NESTLEIND','name':'Nestle India'},
        {'ticker':'DRREDDY','name':'Dr Reddys Laboratories'},
        {'ticker':'CIPLA','name':'Cipla'},
        {'ticker':'HCLTECH','name':'HCL Technologies'},
        {'ticker':'TECHM','name':'Tech Mahindra'},
        {'ticker':'BAJAJFINSV','name':'Bajaj Finserv'},
        {'ticker':'ADANIENT','name':'Adani Enterprises'},
        {'ticker':'ADANIPORTS','name':'Adani Ports'},
        {'ticker':'JSWSTEEL','name':'JSW Steel'},
        {'ticker':'HINDALCO','name':'Hindalco Industries'},
        {'ticker':'COALINDIA','name':'Coal India'},
        {'ticker':'VEDL','name':'Vedanta'},
        {'ticker':'BPCL','name':'Bharat Petroleum'},
        {'ticker':'IOC','name':'Indian Oil Corporation'},
        {'ticker':'HPCL','name':'Hindustan Petroleum'},
        {'ticker':'GAIL','name':'GAIL India'},
        {'ticker':'INDUSINDBK','name':'IndusInd Bank'},
        {'ticker':'ZOMATO','name':'Zomato'},
        {'ticker':'DMART','name':'Avenue Supermarts DMart'},
        {'ticker':'TRENT','name':'Trent Tata Retail'},
        {'ticker':'PAGEIND','name':'Page Industries Jockey'},
        {'ticker':'MUTHOOTFIN','name':'Muthoot Finance'},
        {'ticker':'CHOLAFIN','name':'Cholamandalam Finance'},
        {'ticker':'RECLTD','name':'REC Limited'},
        {'ticker':'PFC','name':'Power Finance Corporation'},
        {'ticker':'IRFC','name':'Indian Railway Finance'},
        {'ticker':'APOLLOHOSP','name':'Apollo Hospitals'},
        {'ticker':'DIVISLAB','name':'Divis Laboratories'},
        {'ticker':'BIOCON','name':'Biocon'},
        {'ticker':'AUROPHARMA','name':'Aurobindo Pharma'},
        {'ticker':'TORNTPHARM','name':'Torrent Pharmaceuticals'},
        {'ticker':'LUPIN','name':'Lupin'},
        {'ticker':'PIDILITIND','name':'Pidilite Industries'},
        {'ticker':'HAVELLS','name':'Havells India'},
        {'ticker':'VOLTAS','name':'Voltas'},
        {'ticker':'DIXON','name':'Dixon Technologies'},
        {'ticker':'BAJAJ-AUTO','name':'Bajaj Auto'},
        {'ticker':'HEROMOTOCO','name':'Hero MotoCorp'},
        {'ticker':'EICHERMOT','name':'Eicher Motors Royal Enfield'},
        {'ticker':'TVSMOTOR','name':'TVS Motor'},
        {'ticker':'M&M','name':'Mahindra & Mahindra'},
        {'ticker':'ASHOKLEY','name':'Ashok Leyland'},
        {'ticker':'MRF','name':'MRF Tyres'},
        {'ticker':'APOLLOTYRE','name':'Apollo Tyres'},
        {'ticker':'SHREECEM','name':'Shree Cement'},
        {'ticker':'ACC','name':'ACC Cement'},
        {'ticker':'AMBUJACEM','name':'Ambuja Cements'},
        {'ticker':'SAIL','name':'Steel Authority of India'},
        {'ticker':'NMDC','name':'NMDC'},
        {'ticker':'JINDALSTEL','name':'Jindal Steel & Power'},
        {'ticker':'MARICO','name':'Marico'},
        {'ticker':'DABUR','name':'Dabur India'},
        {'ticker':'COLPAL','name':'Colgate Palmolive India'},
        {'ticker':'BRITANNIA','name':'Britannia Industries'},
        {'ticker':'TATACONSUM','name':'Tata Consumer Products'},
        {'ticker':'POLYCAB','name':'Polycab India'},
        {'ticker':'CDSL','name':'CDSL'},
        {'ticker':'BSE','name':'BSE Limited'},
        {'ticker':'NAUKRI','name':'Info Edge Naukri'},
        {'ticker':'INDIAMART','name':'IndiaMART InterMESH'},
        {'ticker':'RVNL','name':'Rail Vikas Nigam'},
        {'ticker':'BEL','name':'Bharat Electronics'},
        {'ticker':'HAL','name':'Hindustan Aeronautics'},
        {'ticker':'BHEL','name':'Bharat Heavy Electricals'},
        {'ticker':'MAZAGON','name':'Mazagon Dock Shipbuilders'},
        {'ticker':'IRCTC','name':'Indian Railway Catering IRCTC'},
        {'ticker':'INDIGO','name':'IndiGo InterGlobe Aviation'},
        {'ticker':'DLF','name':'DLF Real Estate'},
        {'ticker':'GODREJPROP','name':'Godrej Properties'},
        {'ticker':'OBEROIRLTY','name':'Oberoi Realty'},
        {'ticker':'MPHASIS','name':'Mphasis'},
        {'ticker':'LTIM','name':'LTIMindtree'},
        {'ticker':'PERSISTENT','name':'Persistent Systems'},
        {'ticker':'COFORGE','name':'Coforge'},
        {'ticker':'TATAPOWER','name':'Tata Power'},
        {'ticker':'IGL','name':'Indraprastha Gas'},
        {'ticker':'ABB','name':'ABB India'},
        {'ticker':'SIEMENS','name':'Siemens India'},
        {'ticker':'SRF','name':'SRF Limited'},
        {'ticker':'DEEPAKNTR','name':'Deepak Nitrite'},
        {'ticker':'TATACHEM','name':'Tata Chemicals'},
        {'ticker':'UPL','name':'UPL Limited'},
        {'ticker':'PIIND','name':'PI Industries'},
    ]

    results = []
    seen = set()
    # Priority 1: ticker starts with query
    for s in COMMON_STOCKS:
        if s['ticker'].startswith(q) and s['ticker'] not in seen:
            results.append(s); seen.add(s['ticker'])
    # Priority 2: any word in name starts with query
    for s in COMMON_STOCKS:
        if s['ticker'] not in seen:
            if any(w.startswith(q) for w in s['name'].upper().split()):
                results.append(s); seen.add(s['ticker'])
    # Priority 3: query appears anywhere in ticker or name
    for s in COMMON_STOCKS:
        if s['ticker'] not in seen:
            if q in s['ticker'] or q in s['name'].upper():
                results.append(s); seen.add(s['ticker'])
    return jsonify(results[:8])


@app.route('/api/analyse', methods=['POST'])
def analyse():
    """Main DCF valuation endpoint."""
    try:
        body        = request.get_json()
        ticker      = body.get('ticker', '').strip().upper()
        rfr_ovr     = body.get('rfr')       # optional override
        wacc_ovr    = body.get('wacc')      # optional override
        tgr_input   = float(body.get('tgr', 5.0))
        proj_years  = int(body.get('years', 5))

        if not ticker:
            return jsonify({'error': 'Ticker is required'}), 400

        # Check cache
        cache_key = f'{ticker}_{tgr_input}_{proj_years}_{rfr_ovr}_{wacc_ovr}'
        cached    = cache_get(cache_key)
        if cached:
            cached['from_cache'] = True
            return jsonify(cached)

        # ── Step 1: Scrape Screener ──────────────────────────────
        screener_data, err = scrape_screener(ticker)
        if err:
            return jsonify({'error': f'Data fetch failed: {err}'}), 500

        # ── Step 2: Extract financials ───────────────────────────
        fin = extract_financials(screener_data)

        if not fin['current_price']:
            return jsonify({'error': 'Could not fetch current price'}), 500

        # ── Step 3: Detect sector ────────────────────────────────
        sector = detect_sector(
            screener_data['company_name'],
            screener_data['industry']
        )

        # ── Step 3b: Detect if bank/NBFC — DCF not applicable ───
        is_bank = sector == 'Banking / Finance'

        # Also detect from P&L structure — banks have 'Financing Profit' not 'Operating Profit'
        pl_rows = screener_data['tables'].get('Profit & Loss', {}).get('rows', {})
        if 'Financing Profit' in pl_rows or 'Net Interest Income' in pl_rows:
            is_bank = True
            sector  = 'Banking / Finance'

        if is_bank:
            # For banks — run P/E and P/B, skip DCF and EV/EBITDA
            pe_result       = calculate_pe_valuation(fin, sector, is_bank=True)
            ev_ebitda_result= calculate_ev_ebitda_valuation(fin, sector, is_bank=True)
            pb_result       = calculate_pb_valuation(fin, sector, is_bank=True)
            composite       = calculate_composite_valuation(
                None, pe_result, ev_ebitda_result, pb_result, fin, sector, is_bank=True
            )

            # Collect all warnings and assumptions
            all_warnings = []
            all_assumptions = []
            all_warnings.append({'title': 'DCF Not Applicable — Banking / NBFC', 'message': 'DCF (FCFF) valuation is not applicable to banks and NBFCs. Customer deposits appear as "debt" which distorts WACC and free cash flow. P/E and P/B are the standard valuation methods for financial institutions.', 'level': 'important'})
            all_warnings.append({'title': 'EV/EBITDA Not Applicable — Banking / NBFC', 'message': 'Interest income is operating revenue for banks, not a financing cost. EBITDA is not a meaningful metric for financial institutions.', 'level': 'important'})
            for r in [pe_result, pb_result]:
                all_warnings.extend(r.get('warnings', []))
                all_assumptions.extend(r.get('assumptions', []))

            return jsonify({
                'error':          None,
                'is_bank':        True,
                'ticker':         ticker,
                'company_name':   screener_data['company_name'],
                'industry':       screener_data['industry'],
                'sector':         sector,
                'current_price':  fin['current_price'],
                'market_cap_cr':  fin['market_cap_cr'],
                'shares_cr':      round(fin['shares'] / 1e7, 2) if fin['shares'] else None,
                'book_value':     fin['book_value'],
                'total_debt_cr':  fin['recent_debt'],
                'net_debt_cr':    fin['net_debt'],
                'roe':            fin['roe'],
                'roce':           fin['roce'],
                'pe_ratio':       fin['pe_ratio'],
                'timestamp':      datetime.now().strftime('%d %b %Y %H:%M'),
                'data_source':    'Screener.in (consolidated financials)',
                'pe_valuation':   pe_result,
                'ev_ebitda_valuation': ev_ebitda_result,
                'pb_valuation':   pb_result,
                'composite':      composite,
                'warnings':       all_warnings,
                'assumptions':    all_assumptions,
            })

        # ── Step 4: Calculate FCFF ───────────────────────────────
        historical_fcff = calculate_fcff(fin)
        if not historical_fcff:
            return jsonify({'error': 'Could not calculate FCFF — insufficient data'}), 500

        # ── Step 5: Calculate WACC ───────────────────────────────
        wacc_data = calculate_wacc(fin, sector, rfr_ovr, wacc_ovr)

        # ── Step 6: Run DCF ──────────────────────────────────────
        dcf_result, dcf_err = run_dcf(
            historical_fcff, wacc_data, fin, tgr_input, proj_years
        )
        if dcf_err:
            return jsonify({'error': f'DCF failed: {dcf_err}'}), 500

        # ── Step 7: Sensitivity table ────────────────────────────
        sensitivity = build_sensitivity(
            historical_fcff, fin,
            wacc_data['wacc'], tgr_input, proj_years
        )

        # ── Step 8: Verdicts ─────────────────────────────────────
        cmp = fin['current_price']
        verdicts = {}
        for s in ['bear', 'base', 'bull']:
            iv               = dcf_result['scenarios'][s]['iv_per_share']
            verdict, upside  = get_verdict(iv, cmp)
            verdicts[s]      = {'verdict': verdict, 'upside': upside}

        # ── Build response ───────────────────────────────────────
        response = {
            'from_cache':       False,
            'timestamp':        datetime.now().strftime('%d %b %Y %H:%M'),
            'ticker':           ticker,
            'company_name':     screener_data['company_name'],
            'industry':         screener_data['industry'],
            'sector':           sector,
            'current_price':    cmp,
            'market_cap_cr':    fin['market_cap_cr'],
            'shares_cr':        round(fin['shares'] / 1e7, 2) if fin['shares'] else None,
            'book_value':       fin['book_value'],
            'total_debt_cr':    fin['recent_debt'],
            'cash_cr':          fin['recent_cash'],
            'net_debt_cr':      fin['net_debt'],
            'roe':              fin['roe'],
            'roce':             fin['roce'],
            'pe_ratio':         fin['pe_ratio'],
            'tax_rate':         round(fin['tax_rate'] * 100, 2),
            'historical_fcff':  historical_fcff,
            'wacc':             wacc_data,
            'dcf':              dcf_result,
            'sensitivity':      sensitivity,
            'verdicts':         verdicts,
            'rfr_default':      RFR_INDIA,
            'erp_default':      ERP_INDIA,
            'data_source':      'Screener.in (consolidated financials)',

            # ── Warnings & Assumptions ───────────────────────────
            'warnings': build_warnings(fin, wacc_data, dcf_result, historical_fcff, sector),
            'assumptions': build_assumptions(fin, wacc_data),
        }

        # ── Probability-Weighted IV ──────────────────────────────
        bear_sc = dcf_result['scenarios']['bear']
        base_sc = dcf_result['scenarios']['base']
        bull_sc = dcf_result['scenarios']['bull']

        bear_iv = bear_sc['iv_per_share']
        base_iv = base_sc['iv_per_share']
        bull_iv = bull_sc['iv_per_share']

        # Check which scenarios have valid (non-zero equity warning) IV
        bear_valid = not bear_sc.get('equity_warning', False)
        base_valid = not base_sc.get('equity_warning', False)
        bull_valid = not bull_sc.get('equity_warning', False)
        valid_count = sum([bear_valid, base_valid, bull_valid])

        if valid_count == 0:
            # All scenarios fail — weighted IV not meaningful
            response['weighted_iv'] = {
                'iv':        None,
                'upside':    None,
                'verdict':   'N/A',
                'valid':     False,
                'reason':    'All 3 scenarios show EV < Net Debt. Company debt exceeds projected enterprise value in all cases. Weighted IV is not meaningful.',
                'weights':   {'bear': 0.25, 'base': 0.50, 'bull': 0.25},
            }
        elif valid_count < 3:
            # Some scenarios fail — redistribute weights among valid ones only
            raw_weights = {
                'bear': 0.25 if bear_valid else 0,
                'base': 0.50 if base_valid else 0,
                'bull': 0.25 if bull_valid else 0,
            }
            total_w = sum(raw_weights.values())
            adj_weights = {k: round(v/total_w, 4) for k,v in raw_weights.items()}
            weighted_iv = round(
                adj_weights['bear'] * bear_iv +
                adj_weights['base'] * base_iv +
                adj_weights['bull'] * bull_iv, 2
            )
            weighted_upside = round(((weighted_iv - fin['current_price']) / fin['current_price']) * 100, 2)
            weighted_verdict = ('UNDERVALUED' if weighted_upside > 15 else
                               'OVERVALUED'  if weighted_upside < -15 else 'FAIRLY VALUED')
            invalid_scenarios = [s.upper() for s, v in [('bear',bear_valid),('base',base_valid),('bull',bull_valid)] if not v]
            response['weighted_iv'] = {
                'iv':        weighted_iv,
                'upside':    weighted_upside,
                'verdict':   weighted_verdict,
                'valid':     True,
                'partial':   True,
                'excluded':  invalid_scenarios,
                'reason':    f"{', '.join(invalid_scenarios)} scenario(s) excluded — EV < Net Debt. Weights redistributed among valid scenarios only.",
                'weights':   adj_weights,
                'formula':   ' + '.join([
                    f"{adj_weights['bear']*100:.0f}% x Bear (₹{bear_iv})" if bear_valid else '',
                    f"{adj_weights['base']*100:.0f}% x Base (₹{base_iv})" if base_valid else '',
                    f"{adj_weights['bull']*100:.0f}% x Bull (₹{bull_iv})" if bull_valid else '',
                ]).strip(' +'),
            }
        else:
            # All 3 valid — standard weighting
            w_bear, w_base, w_bull = 0.25, 0.50, 0.25
            weighted_iv = round(w_bear*bear_iv + w_base*base_iv + w_bull*bull_iv, 2)
            weighted_upside = round(((weighted_iv - fin['current_price']) / fin['current_price']) * 100, 2)
            weighted_verdict = ('UNDERVALUED' if weighted_upside > 15 else
                               'OVERVALUED'  if weighted_upside < -15 else 'FAIRLY VALUED')
            response['weighted_iv'] = {
                'iv':      weighted_iv,
                'upside':  weighted_upside,
                'verdict': weighted_verdict,
                'valid':   True,
                'partial': False,
                'weights': {'bear': w_bear, 'base': w_base, 'bull': w_bull},
                'formula': f"25% x Bear (₹{bear_iv}) + 50% x Base (₹{base_iv}) + 25% x Bull (₹{bull_iv})",
            }

        # ── Step 9: Additional Valuation Methods ─────────────────
        pe_result        = calculate_pe_valuation(fin, sector)
        ev_ebitda_result = calculate_ev_ebitda_valuation(fin, sector)
        pb_result        = calculate_pb_valuation(fin, sector)

        # ── Step 10: Composite Valuation ─────────────────────────
        composite = calculate_composite_valuation(
            response['weighted_iv'], pe_result, ev_ebitda_result, pb_result, fin, sector
        )

        response['pe_valuation']        = pe_result
        response['ev_ebitda_valuation'] = ev_ebitda_result
        response['pb_valuation']        = pb_result
        response['composite']           = composite

        # ── Merge all warnings and assumptions ───────────────────
        existing_warnings    = response['warnings']
        existing_assumptions = response['assumptions']

        for method_result in [pe_result, ev_ebitda_result, pb_result]:
            existing_warnings    += method_result.get('warnings', [])
            existing_assumptions += method_result.get('assumptions', [])

        sw = SECTOR_WEIGHTS.get(sector, SECTOR_WEIGHTS['General / Other'])
        existing_assumptions.append({
            'parameter': 'Composite Weights Source',
            'value':     f"DCF {round(sw['dcf']*100)}% / P/E {round(sw['pe']*100)}% / EV/EBITDA {round(sw['ev_ebitda']*100)}% / P/B {round(sw['pb']*100)}%",
            'source':    f'Damodaran sector classification — {sector} ({DAMODARAN_LAST_UPDATED})',
            'type':      'sector_table'
        })
        existing_assumptions.append({
            'parameter': 'Damodaran Multiples Last Updated',
            'value':     DAMODARAN_LAST_UPDATED,
            'source':    'pages.stern.nyu.edu/~adamodar — India dataset',
            'type':      'data_source'
        })

        response['warnings']    = existing_warnings
        response['assumptions'] = existing_assumptions

        cache_set(cache_key, response)
        return jsonify(response)

    except Exception as e:
        import traceback
        traceback.print_exc()
        return jsonify({'error': str(e)}), 500


if __name__ == '__main__':
    app.run(debug=True, port=5000)
