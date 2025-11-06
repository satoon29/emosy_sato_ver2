import pandas as pd
from datetime import datetime, timedelta
from collections import Counter
import firebase_admin
from firebase_admin import credentials, firestore
from config import FIREBASE_CREDENTIALS_PATH


def initialize_firebase_standalone():
    """Firebase接続を初期化（Streamlit非依存）"""
    if not firebase_admin._apps:
        try:
            cred = credentials.Certificate(FIREBASE_CREDENTIALS_PATH)
            firebase_admin.initialize_app(cred)
        except Exception as e:
            print(f"Firebaseの初期化に失敗しました: {e}")
            return None
    return firestore.client()


def fetch_access_logs(db, start_date=None, end_date=None):
    """アクセスログをFirestoreから取得"""
    query = db.collection('access_logs')
    
    # 期間指定がある場合はフィルタ
    if start_date:
        query = query.where('timestamp', '>=', start_date)
    if end_date:
        query = query.where('timestamp', '<=', end_date)
    
    docs = query.stream()
    
    records = []
    for doc in docs:
        record = doc.to_dict()
        record['doc_id'] = doc.id
        records.append(record)
    
    if not records:
        return pd.DataFrame()
    
    df = pd.DataFrame(records)
    return df


def analyze_user_access_counts(df):
    """ユーザーごとのアクセス回数を集計"""
    if df.empty:
        print("アクセスログがありません")
        return
    
    # ユーザーIDごとのアクセス回数
    user_counts = df['user_id'].value_counts().sort_values(ascending=False)
    
    print("\n" + "="*60)
    print("各ユーザーのアクセス回数")
    print("="*60)
    print(f"{'ユーザーID':15s} {'アクセス回数':>10s} {'割合':>10s}")
    print("-"*60)
    
    total_access = len(df)
    for user_id, count in user_counts.items():
        percentage = count / total_access * 100
        print(f"{user_id:15s} {count:10d}回 {percentage:9.1f}%")
    
    print("-"*60)
    print(f"{'総アクセス数':15s} {total_access:10d}回")
    print(f"{'ユニークユーザー数':15s} {len(user_counts):10d}人")
    print("="*60)
    
    return user_counts


def analyze_session_counts(df):
    """ユーザーごとのセッション数を集計"""
    if df.empty:
        return
    
    # ユーザーIDとセッションIDでグループ化してセッション数をカウント
    session_counts = df.groupby('user_id')['session_id'].nunique().sort_values(ascending=False)
    
    print("\n" + "="*60)
    print("各ユーザーのセッション数（訪問回数）")
    print("="*60)
    print(f"{'ユーザーID':15s} {'セッション数':>12s}")
    print("-"*60)
    
    for user_id, count in session_counts.items():
        print(f"{user_id:15s} {count:12d}回")
    
    print("="*60)
    
    return session_counts


def analyze_daily_access(df):
    """日別アクセス数を集計"""
    if df.empty:
        return
    
    df['date'] = pd.to_datetime(df['timestamp']).dt.date
    daily_counts = df.groupby('date').size().sort_index()
    
    print("\n" + "="*60)
    print("日別アクセス数")
    print("="*60)
    print(f"{'日付':12s} {'アクセス回数':>12s}")
    print("-"*60)
    
    for date, count in daily_counts.items():
        print(f"{str(date):12s} {count:12d}回")
    
    print("="*60)
    
    return daily_counts


def save_analysis_to_csv(df):
    """分析結果をCSVに保存"""
    if df.empty:
        return
    
    # ユーザーごとの集計
    user_stats = df.groupby('user_id').agg({
        'timestamp': ['count', 'min', 'max'],
        'session_id': 'nunique'
    })
    
    user_stats.columns = ['access_count', 'first_access', 'last_access', 'session_count']
    user_stats = user_stats.sort_values('access_count', ascending=False)
    
    user_stats.to_csv('access_log_analysis.csv', encoding='utf-8-sig')
    print("\n分析結果を access_log_analysis.csv に保存しました")
    
    # 詳細ログも保存
    df_export = df[['user_id', 'timestamp', 'session_id', 'token']].copy()
    df_export = df_export.sort_values('timestamp', ascending=False)
    df_export.to_csv('access_log_details.csv', index=False, encoding='utf-8-sig')
    print("詳細ログを access_log_details.csv に保存しました")


def main():
    """メイン処理"""
    db = initialize_firebase_standalone()
    if db is None:
        print("Firebase接続に失敗しました")
        return
    
    # 全期間のアクセスログを取得
    print("アクセスログを取得中...")
    df = fetch_access_logs(db)
    
    if df.empty:
        print("アクセスログがありません")
        return
    
    print(f"\n取得したログ件数: {len(df)}件")
    print(f"期間: {df['timestamp'].min()} ～ {df['timestamp'].max()}")
    
    # 各種分析を実行
    analyze_user_access_counts(df)
    analyze_session_counts(df)
    analyze_daily_access(df)
    
    # CSVに保存
    save_analysis_to_csv(df)


if __name__ == "__main__":
    main()
