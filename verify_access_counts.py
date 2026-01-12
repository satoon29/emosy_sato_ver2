import pandas as pd
from datetime import date, datetime, timezone
import firebase_admin
from firebase_admin import credentials, firestore

# 実験期間の定義
EXPERIMENT_PERIODS = {
    'user21': {'start': date(2025, 12, 4), 'end': date(2025, 12, 24)},
    'user22': {'start': date(2025, 12, 5), 'end': date(2025, 12, 25)},
    'user23': {'start': date(2025, 12, 6), 'end': date(2025, 12, 26)},
    'User24': {'start': date(2025, 12, 6), 'end': date(2025, 12, 26)},
    'user25': {'start': date(2025, 12, 6), 'end': date(2025, 12, 26)},
    'bocco01': {'start': date(2025, 12, 6), 'end': date(2025, 12, 26)},
    'bocco02': {'start': date(2025, 12, 5), 'end': date(2025, 12, 25)},
    'bocco03': {'start': date(2025, 12, 5), 'end': date(2025, 12, 25)},
    'bocco04': {'start': date(2025, 12, 5), 'end': date(2025, 12, 25)},
    'bocco05': {'start': date(2025, 12, 6), 'end': date(2025, 12, 26)},
}


def convert_to_aware_datetime(dt):
    """ナイーブなdatetimeをUTC aware datetimeに変換"""
    if dt.tzinfo is None:
        return dt.replace(tzinfo=timezone.utc)
    return dt


def analyze_discrepancies(db):
    """access_logsとpage_viewsの不一致を詳細に分析"""
    
    print("\n" + "=" * 140)
    print("アクセスログとページビューの詳細分析（不一致の原因を調査）")
    print("=" * 140)
    
    for user_id in sorted(EXPERIMENT_PERIODS.keys()):
        period = EXPERIMENT_PERIODS[user_id]
        
        # 期間内のdatetimeを作成（UTC対応）
        start_dt = convert_to_aware_datetime(datetime.combine(period['start'], datetime.min.time()))
        end_dt = convert_to_aware_datetime(datetime.combine(period['end'], datetime.max.time()))
        
        # access_logsを取得
        access_logs_query = db.collection('access_logs').where('user_id', '==', user_id)
        access_logs_docs = list(access_logs_query.stream())
        
        # ページビューを取得
        page_views_query = db.collection('users').document(user_id).collection('page_views')
        page_views_docs = list(page_views_query.stream())
        
        # 期間内・期間外で分類
        access_in_period = 0
        access_out_period = 0
        pageview_in_period = 0
        pageview_out_period = 0
        
        for doc in access_logs_docs:
            record = doc.to_dict()
            ts = record.get('timestamp')
            if ts:
                if hasattr(ts, 'datetime'):
                    ts_dt = ts.datetime()
                else:
                    ts_dt = pd.Timestamp(ts).to_pydatetime()
                
                if ts_dt.tzinfo is None:
                    ts_dt = ts_dt.replace(tzinfo=timezone.utc)
                
                if start_dt <= ts_dt <= end_dt:
                    access_in_period += 1
                else:
                    access_out_period += 1
        
        for doc in page_views_docs:
            record = doc.to_dict()
            ts = record.get('start_time')
            if ts:
                if hasattr(ts, 'datetime'):
                    ts_dt = ts.datetime()
                else:
                    ts_dt = pd.Timestamp(ts).to_pydatetime()
                
                if ts_dt.tzinfo is None:
                    ts_dt = ts_dt.replace(tzinfo=timezone.utc)
                
                if start_dt <= ts_dt <= end_dt:
                    pageview_in_period += 1
                else:
                    pageview_out_period += 1
        
        total_access = access_in_period + access_out_period
        total_pageview = pageview_in_period + pageview_out_period
        diff = access_in_period - pageview_in_period
        
        print(f"\n【{user_id}】実験期間: {period['start']} ～ {period['end']}")
        print("-" * 140)
        print(f"  access_logs:")
        print(f"    期間内: {access_in_period:3d}回")
        print(f"    期間外: {access_out_period:3d}回")
        print(f"    合計:   {total_access:3d}回")
        print(f"  page_views:")
        print(f"    期間内: {pageview_in_period:3d}回")
        print(f"    期間外: {pageview_out_period:3d}回")
        print(f"    合計:   {total_pageview:3d}回")
        print(f"  差分（access_logs期間内 - page_views期間内）: {diff:+3d}回")
        
        # 不一致の分析
        if diff != 0:
            if access_in_period > pageview_in_period:
                print(f"  → access_logsが多い理由の仮説:")
                print(f"     1. アクセスログにはページビュー記録漏れがある")
                print(f"     2. 同じセッション内の複数アクセスを１ページビューとして記録")
                print(f"     3. ページビュー記録時に start_time が未記録のレコードがある")
            else:
                print(f"  → page_viewsが多い理由の仮説:")
                print(f"     1. ページビューはアクセスログより以前のデータを含む")
    
    print("\n" + "=" * 140)


def main():
    """メイン処理"""
    
    try:
        # Firebase初期化
        if not firebase_admin._apps:
            try:
                from config import FIREBASE_CREDENTIALS_PATH
                cred = credentials.Certificate(FIREBASE_CREDENTIALS_PATH)
                print(f"Firebase認証情報を読み込みました: {FIREBASE_CREDENTIALS_PATH}")
            except ImportError:
                import streamlit as st
                cred_dict = dict(st.secrets["firebase_credentials"])
                cred = credentials.Certificate(cred_dict)
                print("Streamlit Secretsから認証情報を読み込みました")
            
            firebase_admin.initialize_app(cred)
            print("Firebase初期化完了")
        
        db = firestore.client()
        if db is None:
            print("Firebase接続に失敗しました")
            return
        
        print("Firestoreクライアント接続完了")
        
        # 不一致を詳細に分析
        analyze_discrepancies(db)
        
        print("\n分析完了")
        
    except Exception as e:
        print(f"\nエラーが発生しました: {str(e)}")
        import traceback
        traceback.print_exc()


if __name__ == "__main__":
    main()
