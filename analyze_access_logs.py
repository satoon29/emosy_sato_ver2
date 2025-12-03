import pandas as pd
from datetime import datetime, timedelta
from collections import Counter
import firebase_admin
from firebase_admin import credentials, firestore


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


def fetch_page_views(db, start_date=None, end_date=None):
    """ページビューログをFirestoreから取得"""
    query = db.collection('page_views')
    
    # 期間指定がある場合はフィルタ
    if start_date:
        query = query.where('start_time', '>=', start_date)
    if end_date:
        query = query.where('start_time', '<=', end_date)
    
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


def analyze_user_access_counts(df, report_file):
    """ユーザーごとのアクセス回数を集計"""
    if df.empty:
        message = "アクセスログがありません"
        print(message)
        report_file.write(message + "\n")
        return
    
    # ユーザーIDごとのアクセス回数
    user_counts = df['user_id'].value_counts().sort_values(ascending=False)
    
    output = []
    output.append("\n各ユーザーのアクセス回数")
    output.append("-" * 40)
    
    total_access = len(df)
    for user_id, count in user_counts.items():
        percentage = count / total_access * 100
        output.append(f"{user_id}: {count}回 ({percentage:.1f}%)")
    
    output.append(f"\n合計: {total_access}回 (ユニークユーザー: {len(user_counts)}人)")
    
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


def analyze_viewing_duration(df, report_file):
    """ユーザーごとの閲覧時間を集計"""
    if df.empty:
        return
    
    # duration_secondsがNoneでないものだけを対象
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
    output.append("\nユーザーごとの閲覧時間")
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


def analyze_view_mode_duration(df, report_file):
    """表示モード別の閲覧時間を集計"""
    if df.empty or 'view_mode' not in df.columns:
        return
    
    # duration_secondsとview_modeがNoneでないものだけを対象
    duration_df = df[(df['duration_seconds'].notna()) & (df['view_mode'].notna())].copy()
    
    if duration_df.empty:
        return
    
    # 表示モード別の閲覧時間を集計
    mode_duration = duration_df.groupby('view_mode')['duration_seconds'].agg(['sum', 'mean', 'count'])
    mode_duration.columns = ['total_seconds', 'avg_seconds', 'view_count']
    mode_duration = mode_duration.sort_values('total_seconds', ascending=False)
    
    output = []
    output.append("\n表示モード別の閲覧時間")
    output.append("-" * 60)
    output.append(f"{'表示モード':15s} {'総閲覧時間':15s} {'平均閲覧時間':15s} {'閲覧回数':10s}")
    output.append("-" * 60)
    
    for mode, row in mode_duration.iterrows():
        total_time = str(timedelta(seconds=int(row['total_seconds'])))
        avg_time = str(timedelta(seconds=int(row['avg_seconds'])))
        output.append(f"{mode:15s} {total_time:15s} {avg_time:15s} {int(row['view_count']):10d}回")
    
    # コンソールとファイルの両方に出力
    for line in output:
        print(line)
        report_file.write(line + "\n")
    
    return mode_duration


def analyze_user_mode_duration(df, report_file):
    """ユーザーと表示モード別の閲覧時間を集計"""
    if df.empty or 'view_mode' not in df.columns:
        return
    
    # duration_secondsとview_modeがNoneでないものだけを対象
    duration_df = df[(df['duration_seconds'].notna()) & (df['view_mode'].notna())].copy()
    
    if duration_df.empty:
        return
    
    # ユーザーIDと表示モードでピボットテーブルを作成
    pivot = duration_df.pivot_table(
        index='user_id',
        columns='view_mode',
        values='duration_seconds',
        aggfunc='sum',
        fill_value=0
    )
    
    output = []
    output.append("\nユーザーと表示モード別の閲覧時間（秒）")
    output.append("-" * 80)
    
    # ヘッダー行
    modes = pivot.columns.tolist()
    header = f"{'ユーザーID':15s}"
    for mode in modes:
        header += f" {mode:20s}"
    output.append(header)
    output.append("-" * 80)
    
    # 各ユーザーの行
    for user_id, row in pivot.iterrows():
        line = f"{user_id:15s}"
        for mode in modes:
            duration = str(timedelta(seconds=int(row[mode])))
            line += f" {duration:20s}"
        output.append(line)
    
    # コンソールとファイルの両方に出力
    for line in output:
        print(line)
        report_file.write(line + "\n")
    
    return pivot


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
    # レポートファイルを開く
    report_file = open('log_report.txt', 'w', encoding='utf-8')
    
    try:
        # Firebase初期化
        if not firebase_admin._apps:
            # 環境に応じて適切な認証方法を選択
            try:
                # Streamlit Cloud環境の場合
                import streamlit as st
                cred_dict = dict(st.secrets["firebase_credentials"])
                cred = credentials.Certificate(cred_dict)
            except:
                # ローカル環境の場合
                from config import FIREBASE_CREDENTIALS_PATH
                cred = credentials.Certificate(FIREBASE_CREDENTIALS_PATH)
            
            firebase_admin.initialize_app(cred)
        
        db = firestore.client()
        if db is None:
            message = "Firebase接続に失敗しました"
            print(message)
            report_file.write(message + "\n")
            return
        
        # 全期間のアクセスログを取得
        df = fetch_access_logs(db)
        
        if df.empty:
            message = "アクセスログがありません"
            print(message)
            report_file.write(message + "\n")
            return
        
        # ページビューログを取得
        page_views_df = fetch_page_views(db)
        
        # 基本情報
        header = f"アクセスログ分析レポート (総件数: {len(df)}件)"
        period = f"期間: {df['timestamp'].min().strftime('%Y-%m-%d %H:%M')} ～ {df['timestamp'].max().strftime('%Y-%m-%d %H:%M')}"
        
        print(header)
        print(period)
        report_file.write(header + "\n")
        report_file.write(period + "\n")
        
        # 各種分析を実行
        analyze_user_access_counts(df, report_file)
        analyze_session_counts(df, report_file)
        analyze_view_mode_counts(df, report_file)
        analyze_user_view_modes(df, report_file)
        analyze_daily_access(df, report_file)
        
        # 閲覧時間の分析を追加
        if not page_views_df.empty:
            analyze_viewing_duration(page_views_df, report_file)
            analyze_view_mode_duration(page_views_df, report_file)
            analyze_user_mode_duration(page_views_df, report_file)
        
        # CSVに保存
        save_analysis_to_csv(df)
        
        # レポート保存完了メッセージ
        final_message = "\n分析完了: log_report.txt に保存しました"
        print(final_message)
        report_file.write(final_message + "\n")
        
    finally:
        report_file.close()


if __name__ == "__main__":
    main()
