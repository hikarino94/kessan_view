#!/usr/bin/env python3
"""YoY/QoQ計算の検証スクリプト"""
import sys, os, pathlib, json
sys.path.insert(0, '/home/tkimura/kessan_view')
os.chdir('/home/tkimura/kessan_view')

# 1. DBクリーニング
db_path = pathlib.Path('data/kessan_view.db')
if db_path.exists():
    db_path.unlink()
from db.database import init_db, get_session
init_db()
print('✅ DB初期化')

from models.schemas import Stock, FinancialStatement
from services.sync import _parse_date, _safe_float

# 2. キャッシュ済みJSONからDB投入（APIコール不要）
test_data = {
    '67580': ('/tmp/sony.json', 'ソニーグループ'),
    '79740': ('/tmp/nintendo.json', '任天堂'),
}

session = get_session()
for code, (json_path, name) in test_data.items():
    # 銘柄マスタ（既存の場合はスキップ）
    existing = session.query(Stock).filter_by(code=code).first()
    if not existing:
        session.add(Stock(code=code, name=name, sector_33_name='テスト', market_name='プライム'))
        session.commit()

    # 決算データ
    data = json.load(open(json_path))
    items = data.get('data', [])
    for item in items:
        c = item.get('Code', '')
        if not c:
            continue
        fs = FinancialStatement(
            code=c,
            disclosed_date=_parse_date(item.get('DiscDate')),
            disclosed_time=item.get('DiscTime', ''),
            disclosure_number=item.get('DiscNo', ''),
            type_of_document=item.get('DocType', ''),
            type_of_current_period=item.get('CurPerType', ''),
            current_period_start_date=_parse_date(item.get('CurPerSt')),
            current_period_end_date=_parse_date(item.get('CurPerEn')),
            current_fiscal_year_start_date=_parse_date(item.get('CurFYSt')),
            current_fiscal_year_end_date=_parse_date(item.get('CurFYEn')),
            net_sales=_safe_float(item.get('Sales')),
            operating_profit=_safe_float(item.get('OP')),
            ordinary_profit=_safe_float(item.get('OdP')),
            profit=_safe_float(item.get('NP')),
            earnings_per_share=_safe_float(item.get('EPS')),
            forecast_net_sales=_safe_float(item.get('FSales')),
            forecast_operating_profit=_safe_float(item.get('FOP')),
            forecast_profit=_safe_float(item.get('FNP')),
            raw_json=json.dumps(item, ensure_ascii=False, default=str),
        )
        session.add(fs)
    session.commit()
    print(f'✅ {name}({code}): {len(items)}件投入')

# 3. EarnForecastRevision を手動で追加してフィルタリングテスト
print()
print('--- 手動テストデータ追加: EarnForecastRevision ---')
session = get_session()
# 任天堂のFY期間で予想修正レコードを追加
fs_rev = FinancialStatement(
    code='79740',
    disclosed_date=_parse_date('2025-08-01'),
    disclosed_time='15:00:00',
    disclosure_number='99999999999999',  # 大きい番号
    type_of_document='EarnForecastRevision',
    type_of_current_period='FY',
    current_period_start_date=_parse_date('2025-04-01'),
    current_period_end_date=_parse_date('2026-03-31'),
    current_fiscal_year_start_date=_parse_date('2025-04-01'),
    current_fiscal_year_end_date=_parse_date('2026-03-31'),
    net_sales=None,  # 予想修正には実績なし
    operating_profit=None,
    profit=None,
    forecast_net_sales=_safe_float('2100000000000'),
    forecast_operating_profit=_safe_float('700000000000'),
    forecast_profit=_safe_float('500000000000'),
    raw_json='{"DocType":"EarnForecastRevision","test":"manual"}',
)
session.add(fs_rev)
# 訂正レコードも追加（同じ2Q決算の訂正版）
fs_corr = FinancialStatement(
    code='79740',
    disclosed_date=_parse_date('2025-11-20'),
    disclosed_time='15:00:00',
    disclosure_number='99999999999998',  # より大きい番号
    type_of_document='2QFinancialStatements_Consolidated_Japanese_CorrectedReport',
    type_of_current_period='2Q',
    current_period_start_date=_parse_date('2025-04-01'),
    current_period_end_date=_parse_date('2025-09-30'),
    current_fiscal_year_start_date=_parse_date('2025-04-01'),
    current_fiscal_year_end_date=_parse_date('2026-03-31'),
    net_sales=_safe_float('999999000000'),  # 訂正値
    operating_profit=_safe_float('333333000000'),
    profit=_safe_float('222222000000'),
    raw_json='{"DocType":"2QFinancialStatements_Corrected","test":"manual"}',
)
session.add(fs_corr)
session.commit()
session.close()
print('  EarnForecastRevision (FY, CurPerType=FY) 追加')
print('  CorrectedReport (2Q訂正, DocType=*FinancialStatements*Corrected*) 追加')

# 4. フィルタリングテスト
print()
print('=' * 60)
print('  フィルタリングおよびYoY/QoQ検証')
print('=' * 60)

import logging
logging.basicConfig(level=logging.DEBUG, format='%(message)s')
from services.financial_analysis import FinancialAnalyzer
analyzer = FinancialAnalyzer()

# 任天堂のDBの全レコード数
session = get_session()
all_count = session.query(FinancialStatement).filter_by(code='79740').count()
session.close()
print(f'\n任天堂 DB全レコード: {all_count}件')

# フィルタ後
stmts = analyzer.get_statements_for_code('79740')
print(f'フィルタ後: {len(stmts)}件')
for s in stmts:
    sales_b = f'{s.net_sales/1e9:.0f}B' if s.net_sales else '-'
    op_b = f'{s.operating_profit/1e9:.0f}B' if s.operating_profit else '-'
    print(f'  {s.current_fiscal_year_end_date} {s.type_of_current_period:4s} '
          f'{s.type_of_document[:40]:40s} 売上:{sales_b:>8s} 営利:{op_b:>8s}')

# EarnForecastRevision が除外されているか確認
has_revision = any('Revision' in s.type_of_document for s in stmts)
print(f'\nEarnForecastRevision除外: {"✅ OK" if not has_revision else "❌ NG - まだ含まれている!"}')

# 訂正レコードが最新で採用されているか確認
latest_2q = [s for s in stmts
             if s.type_of_current_period == '2Q'
             and s.current_fiscal_year_end_date
             and s.current_fiscal_year_end_date.year == 2026]
if latest_2q:
    q2 = latest_2q[0]
    is_corrected = q2.net_sales == 999999000000
    print(f'2Q訂正版採用: {"✅ OK (訂正値採用)" if is_corrected else "❌ NG (元の値が残っている)"}')
    print(f'  2Q売上: {q2.net_sales/1e9:.0f}B, DiscNo: {q2.disclosure_number}')

# 5. YoY/QoQ計算
for code, name in [('67580', 'ソニー'), ('79740', '任天堂')]:
    print(f'\n{"="*50}')
    print(f'  {name} ({code}) YoY/QoQ')
    print(f'{"="*50}')

    stmts = analyzer.get_statements_for_code(code)
    if not stmts:
        print('  データなし')
        continue

    # 最新決算
    latest = stmts[-1]
    print(f'\n最新決算: {latest.current_fiscal_year_end_date} {latest.type_of_current_period}')
    sales_b = f'{latest.net_sales/1e9:.0f}B' if latest.net_sales else '-'
    op_b = f'{latest.operating_profit/1e9:.0f}B' if latest.operating_profit else '-'
    np_b = f'{latest.profit/1e9:.0f}B' if latest.profit else '-'
    print(f'  売上: {sales_b}, 営利: {op_b}, 純利: {np_b}')

    yoy = analyzer.compare_year_over_year(code)
    print(f'\n--- YoY ---')
    prev = yoy.get("previous")
    if prev:
        print(f'  比較: {prev.current_fiscal_year_end_date} {prev.type_of_current_period}')
        prev_s = f'{prev.net_sales/1e9:.0f}B' if prev.net_sales else '-'
        prev_o = f'{prev.operating_profit/1e9:.0f}B' if prev.operating_profit else '-'
        print(f'    売上: {prev_s}, 営利: {prev_o}')
    else:
        print('  比較対象: なし')

    for label, key in [('売上高', 'yoy_net_sales'), ('営利', 'yoy_operating_profit'), ('純利', 'yoy_profit')]:
        v = yoy.get(key)
        if v is not None:
            print(f'  {label} YoY: {v:+.1f}%')
        else:
            print(f'  {label} YoY: -')

    qoq = analyzer.compare_quarter_over_quarter(code)
    print(f'\n--- QoQ ---')
    prev_q = qoq.get("previous")
    if prev_q:
        print(f'  比較: {prev_q.current_fiscal_year_end_date} {prev_q.type_of_current_period}')
    else:
        print('  比較対象: なし')
    for label, key in [('売上高', 'qoq_net_sales'), ('営利', 'qoq_operating_profit'), ('純利', 'qoq_profit')]:
        v = qoq.get(key)
        if v is not None:
            print(f'  {label} QoQ: {v:+.1f}%')
        else:
            print(f'  {label} QoQ: -')

    signals = analyzer.detect_signals(code)
    print(f'\n--- シグナル ---')
    if signals:
        for sig in signals:
            print(f'  {sig}')
    else:
        print('  (なし)')

print()
print('✅ 全検証完了')
