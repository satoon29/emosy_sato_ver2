import pandas as pd
from datetime import datetime, timedelta, date, timezone
from collections import Counter
import firebase_admin
from firebase_admin import credentials, firestore
import matplotlib.pyplot as plt
import matplotlib.font_manager as fm
import os

# 日本語フォントを設定
JAPANESE_FONT_PATH = "assets/NotoSansJP-Regular.ttf"
if os.path.exists(JAPANESE_FONT_PATH):
    fm.fontManager.addfont(JAPANESE_FONT_PATH)
    plt.rcParams['font.family'] = 'Noto Sans JP'
else:
    print(f"⚠️ 日本語フォントが見つかりません: {JAPANESE_FONT_PATH}")

# 実験期間の定義
EXPERIMENT_PERIODS = {
    'user21': {'start': date(2025, 12, 4), 'end': date(2025, 12, 24)},
    'user22': {'start': date(2025, 12, 5), 'end': date(2025, 12, 25)},
    'user23': {'start': date(2025, 12, 6), 'end': date(2025, 12, 26)},
    'User24': {'start': date(2025, 12, 6), 'end': date(2025, 12, 26)},
    'user25': {'start': date(2025, 12, 6), 'end': date(2025, 12, 26)},
}

NOTIFICATIONS_PER_DAY = 20


def convert_to_aware_datetime(dt):
    """ナイーブなdatetimeをUTC aware datetimeに変換"""
    if dt.tzinfo is None:
        return dt.replace(tzinfo=timezone.utc)
    return dt


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


def fetch_page_views(db, user_id=None, start_date=None, end_date=None):
    """ページビューログをFirestoreから取得"""
    records = []
    
    if user_id:
        # 特定のユーザーのページビューを取得
        query = db.collection('users').document(user_id).collection('page_views')
        
        if start_date:
            query = query.where('start_time', '>=', start_date)
        if end_date:
            query = query.where('start_time', '<=', end_date)
        
        docs = query.stream()
        
        for doc in docs:
            record = doc.to_dict()
            record['doc_id'] = doc.id
            record['user_id'] = user_id
            records.append(record)
    else:
        # 全ユーザーのページビューを取得
        users_ref = db.collection('users')
        users = users_ref.stream()
        
        for user_doc in users:
            user_id = user_doc.id
            query = db.collection('users').document(user_id).collection('page_views')
            
            if start_date:
                query = query.where('start_time', '>=', start_date)
            if end_date:
                query = query.where('start_time', '<=', end_date)
            
            docs = query.stream()
            
            for doc in docs:
                record = doc.to_dict()
                record['doc_id'] = doc.id
                record['user_id'] = user_id
                records.append(record)
    
    if not records:
        return pd.DataFrame()
    
    df = pd.DataFrame(records)
    return df


def analyze_user_access_counts_with_period_filter(db, report_file):
    """ユーザーごとのアクセス回数を集計（実験期間内のみ）"""
    
    output = []
    output.append("\n各ユーザーのアクセス回数（実験期間内のみ）")
    output.append("-" * 80)
    output.append(f"{'ユーザーID':15s} {'実験期間':35s} {'全アクセス数':15s} {'期間内アクセス数':20s} {'割合':15s}")
    output.append("-" * 80)
    
    user_counts = {}
    total_access = 0
    
    # アクセスログを全件取得
    access_logs_query = db.collection('access_logs')
    access_logs_docs = list(access_logs_query.stream())
    
    access_logs_by_user = {}
    for doc in access_logs_docs:
        record = doc.to_dict()
        user_id = record.get('user_id')
        if user_id:
            if user_id not in access_logs_by_user:
                access_logs_by_user[user_id] = []
            access_logs_by_user[user_id].append(record.get('timestamp'))
    
    # 各ユーザーについて実験期間内のアクセスを集計
    for user_id, period in EXPERIMENT_PERIODS.items():
        if user_id not in access_logs_by_user:
            output.append(f"{user_id:15s} {str(period['start']):10s} ～ {str(period['end']):10s} {'0':15d}回 {'0':20d}回 {'0.0':14.1f}%")
            user_counts[user_id] = 0
            continue
        
        # 期間内のdatetimeを作成（UTC対応）
        start_dt = convert_to_aware_datetime(datetime.combine(period['start'], datetime.min.time()))
        end_dt = convert_to_aware_datetime(datetime.combine(period['end'], datetime.max.time()))
        
        all_timestamps = access_logs_by_user[user_id]
        total_count = len(all_timestamps)
        
        # 期間内のアクセスをカウント
        in_period_count = 0
        for ts in all_timestamps:
            if hasattr(ts, 'datetime'):
                ts_dt = ts.datetime()
            else:
                ts_dt = pd.Timestamp(ts).to_pydatetime()
            
            if ts_dt.tzinfo is None:
                ts_dt = ts_dt.replace(tzinfo=timezone.utc)
            
            if start_dt <= ts_dt <= end_dt:
                in_period_count += 1
        
        percentage = (in_period_count / total_count * 100) if total_count > 0 else 0
        period_str = f"{period['start']} ～ {period['end']}"
        output.append(f"{user_id:15s} {period_str:35s} {total_count:15d}回 {in_period_count:20d}回 {percentage:14.1f}%")
        
        user_counts[user_id] = in_period_count
        total_access += in_period_count
    
    output.append("-" * 80)
    output.append(f"{'合計':15s} {total_access:15d}回")
    
    # コンソールとファイルの両方に出力
    for line in output:
        print(line)
        report_file.write(line + "\n")
    
    return user_counts


def analyze_session_counts(df, report_file):
    """ユーザーごとのセッション数を集計"""
    if df.empty:
        return
    
    # ユーザーIDとセッションIDでグループ化してセッション数をカウント
    session_counts = df.groupby('user_id')['session_id'].nunique().sort_values(ascending=False)
    
    output = []
    output.append("\n各ユーザーのセッション数（訪問回数）")
    output.append("-" * 40)
    
    for user_id, count in session_counts.items():
        output.append(f"{user_id}: {count}回")
    
    # コンソールとファイルの両方に出力
    for line in output:
        print(line)
        report_file.write(line + "\n")
    
    return session_counts


def analyze_daily_access(df, report_file):
    """日別アクセス数を集計"""
    if df.empty:
        return
    
    df['date'] = pd.to_datetime(df['timestamp']).dt.date
    daily_counts = df.groupby('date').size().sort_index()
    
    output = []
    output.append("\n日別アクセス数")
    output.append("-" * 40)
    
    for date, count in daily_counts.items():
        output.append(f"{date}: {count}回")
    
    # コンソールとファイルの両方に出力
    for line in output:
        print(line)
        report_file.write(line + "\n")
    
    return daily_counts


def analyze_view_mode_counts(df, report_file):
    """表示モード別のアクセス回数を集計"""
    if df.empty or 'view_mode' not in df.columns:
        return
    
    # view_modeがNoneでないものだけをカウント
    view_mode_df = df[df['view_mode'].notna()]
    
    if view_mode_df.empty:
        return
    
    view_mode_counts = view_mode_df['view_mode'].value_counts().sort_index()
    
    output = []
    output.append("\n表示モード別アクセス回数")
    output.append("-" * 40)
    
    for mode, count in view_mode_counts.items():
        percentage = count / len(view_mode_df) * 100
        output.append(f"{mode}: {count}回 ({percentage:.1f}%)")
    
    output.append(f"\n合計: {len(view_mode_df)}回")
    
    # コンソールとファイルの両方に出力
    for line in output:
        print(line)
        report_file.write(line + "\n")
    
    return view_mode_counts


def analyze_user_view_modes(df, report_file):
    """ユーザーごとの表示モード別アクセス回数を集計"""
    if df.empty or 'view_mode' not in df.columns:
        return
    
    # view_modeがNoneでないものだけを対象
    view_mode_df = df[df['view_mode'].notna()]
    
    if view_mode_df.empty:
        return
    
    # ユーザーIDと表示モードでピボットテーブルを作成
    pivot = view_mode_df.pivot_table(
        index='user_id',
        columns='view_mode',
        aggfunc='size',
        fill_value=0
    )
    
    output = []
    output.append("\nユーザーごとの表示モード別アクセス回数")
    output.append("-" * 60)
    
    # ヘッダー行
    modes = pivot.columns.tolist()
    header = f"{'ユーザーID':15s}"
    for mode in modes:
        header += f" {mode:12s}"
    output.append(header)
    output.append("-" * 60)
    
    # 各ユーザーの行
    for user_id, row in pivot.iterrows():
        line = f"{user_id:15s}"
        for mode in modes:
            line += f" {int(row[mode]):12d}回"
        output.append(line)
    
    # コンソールとファイルの両方に出力
    for line in output:
        print(line)
        report_file.write(line + "\n")
    
    return pivot


def estimate_missing_durations(df):
    """終了時刻が記録されていないページビューの滞在時間を推定"""
    if df.empty:
        return df
    
    df = df.copy()
    df = df.sort_values(['user_id', 'session_id', 'start_time'])
    
    # 終了時刻がないレコードに対して推定値を設定
    for idx, row in df.iterrows():
        if pd.isna(row['end_time']) or pd.isna(row['duration_seconds']):
            # 同じセッション内の次のページビューがあれば、その開始時刻を終了時刻とする
            next_view = df[
                (df['user_id'] == row['user_id']) & 
                (df['session_id'] == row['session_id']) &
                (df['start_time'] > row['start_time'])
            ].sort_values('start_time').head(1)
            
            if not next_view.empty:
                estimated_end = next_view.iloc[0]['start_time']
            else:
                # 次のページビューがない場合は、デフォルト値（例：5分）を使用
                estimated_end = row['start_time'] + timedelta(minutes=5)
            
            df.at[idx, 'end_time'] = estimated_end
            df.at[idx, 'duration_seconds'] = (estimated_end - row['start_time']).total_seconds()
            df.at[idx, 'is_estimated'] = True
        else:
            df.at[idx, 'is_estimated'] = False
    
    return df


def analyze_viewing_duration(df, report_file):
    """ユーザーごとの閲覧時間を集計"""
    if df.empty:
        return
    
    # duration_secondsがNoneでないもの（実際に記録されたもの）だけを対象
    duration_df = df[df['duration_seconds'].notna()].copy()
    
    if duration_df.empty:
        output = ["\n閲覧時間のデータがありません"]
        for line in output:
            print(line)
            report_file.write(line + "\n")
        return
    
    # ユーザーごとの総閲覧時間を計算
    user_duration = duration_df.groupby('user_id')['duration_seconds'].agg(['sum', 'mean', 'count'])
    user_duration.columns = ['total_seconds', 'avg_seconds', 'view_count']
    user_duration = user_duration.sort_values('total_seconds', ascending=False)
    
    output = []
    output.append("\nユーザーごとの閲覧時間（実測値のみ）")
    output.append("-" * 60)
    output.append(f"{'ユーザーID':15s} {'総閲覧時間':15s} {'平均閲覧時間':15s} {'閲覧回数':10s}")
    output.append("-" * 60)
    
    for user_id, row in user_duration.iterrows():
        total_time = str(timedelta(seconds=int(row['total_seconds'])))
        avg_time = str(timedelta(seconds=int(row['avg_seconds'])))
        output.append(f"{user_id:15s} {total_time:15s} {avg_time:15s} {int(row['view_count']):10d}回")
    
    # コンソールとファイルの両方に出力
    for line in output:
        print(line)
        report_file.write(line + "\n")
    
    return user_duration


def main():
    """メイン処理"""
    # レポートファイルを開く
    report_file = open('log_report.txt', 'w', encoding='utf-8')
    
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
            message = "Firebase接続に失敗しました"
            print(message)
            report_file.write(message + "\n")
            return
        
        print("Firestoreクライアント接続完了")
        
        # ===== アクセスログの分析（実験期間内のみ） =====
        print("\n【アクセスログの分析（実験期間内のみ）】")
        
        # ユーザーごとのアクセス回数（実験期間内のみ）
        print("ユーザーごとのアクセス回数を集計中...")
        user_access_counts = analyze_user_access_counts_with_period_filter(db, report_file)
        
        # レポート保存完了メッセージ
        final_message = "\n分析完了: log_report.txt に保存しました"
        print(final_message)
        report_file.write(final_message + "\n")
        
    except Exception as e:
        error_message = f"\nエラーが発生しました: {str(e)}"
        print(error_message)
        report_file.write(error_message + "\n")
        import traceback
        traceback.print_exc()
        
    finally:
        report_file.close()


if __name__ == "__main__":
    main()
