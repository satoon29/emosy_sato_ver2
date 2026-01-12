import pandas as pd
from datetime import datetime, timedelta, date
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
    'user21': {'start': date(2024, 12, 4), 'end': date(2024, 12, 24)},
    'user22': {'start': date(2024, 12, 5), 'end': date(2024, 12, 25)},
    'user23': {'start': date(2024, 12, 6), 'end': date(2024, 12, 26)},
    'User24': {'start': date(2024, 12, 6), 'end': date(2024, 12, 26)},
    'user25': {'start': date(2024, 12, 6), 'end': date(2024, 12, 26)},
}

NOTIFICATIONS_PER_DAY = 20


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


def plot_user_access_counts(df, report_file, target_users=None):
    """ユーザーごとのアクセス回数を棒グラフで可視化"""
    if df.empty:
        return
    
    # ユーザーIDごとのアクセス回数を集計
    user_counts = df['user_id'].value_counts().sort_index()
    
    # 特定のユーザーのみを対象にする場合
    if target_users:
        user_counts = user_counts[user_counts.index.isin(target_users)]
        if user_counts.empty:
            output = [f"\n指定されたユーザー {target_users} のデータがありません"]
            for line in output:
                print(line)
                report_file.write(line + "\n")
            return
    
    # グラフを作成
    fig, ax = plt.subplots(figsize=(12, 6))
    
    # 棒グラフを描画
    bars = ax.bar(range(len(user_counts)), user_counts.values, color='#4A90E2', alpha=0.8)
    
    # X軸のラベルを設定
    ax.set_xticks(range(len(user_counts)))
    ax.set_xticklabels(user_counts.index, rotation=45, ha='right')
    
    # 各バーの上に数値を表示
    for i, (user, count) in enumerate(user_counts.items()):
        ax.text(i, count, str(count), ha='center', va='bottom', fontsize=10)
    
    # グラフの装飾
    ax.set_xlabel('ユーザーID', fontsize=12)
    ax.set_ylabel('アクセス回数', fontsize=12)
    ax.set_title('ユーザーごとのアクセス回数', fontsize=14, fontweight='bold')
    ax.grid(axis='y', linestyle='--', alpha=0.3)
    
    plt.tight_layout()
    
    # グラフを保存
    plt.savefig('user_access_counts.svg', dpi=300, bbox_inches='tight', format='svg')
    print("\nグラフを user_access_counts.svg に保存しました")
    report_file.write("\nグラフを user_access_counts.svg に保存しました\n")
    
    plt.close()
    
    return user_counts


def plot_daily_access_by_user(df, report_file, target_users=None):
    """ユーザーごとの日別アクセス数を折れ線グラフで可視化"""
    if df.empty:
        return
    
    df_copy = df.copy()
    df_copy['date'] = pd.to_datetime(df_copy['timestamp']).dt.date
    
    # 特定のユーザーのみを対象にする場合
    if target_users:
        df_copy = df_copy[df_copy['user_id'].isin(target_users)]
        if df_copy.empty:
            output = [f"\n指定されたユーザー {target_users} のデータがありません"]
            for line in output:
                print(line)
                report_file.write(line + "\n")
            return
    
    # ユーザーIDと日付でピボットテーブルを作成
    pivot = df_copy.pivot_table(
        index='date',
        columns='user_id',
        aggfunc='size',
        fill_value=0
    )
    
    # グラフを作成
    fig, ax = plt.subplots(figsize=(14, 7))
    
    # 各ユーザーの折れ線グラフを描画
    colors = ['#4A90E2', '#E24A4A', '#4AE290', '#E2904A', '#904AE2']
    for i, user_id in enumerate(pivot.columns):
        ax.plot(pivot.index, pivot[user_id], marker='o', 
                label=user_id, linewidth=2, color=colors[i % len(colors)])
    
    # グラフの装飾
    ax.set_xlabel('日付', fontsize=12)
    ax.set_ylabel('アクセス回数', fontsize=12)
    ax.set_title('ユーザーごとの日別アクセス推移', fontsize=14, fontweight='bold')
    ax.legend(loc='upper left', bbox_to_anchor=(1, 1))
    ax.grid(True, linestyle='--', alpha=0.3)
    
    # X軸の日付表示を調整
    plt.xticks(rotation=45, ha='right')
    
    plt.tight_layout()
    
    # グラフを保存
    plt.savefig('daily_access_by_user.svg', dpi=300, bbox_inches='tight', format='svg')
    print("グラフを daily_access_by_user.svg に保存しました")
    report_file.write("グラフを daily_access_by_user.svg に保存しました\n")
    
    plt.close()
    
    return pivot


def fetch_emotion_records(db, user_id):
    """特定ユーザーの感情記録をFirestoreから取得"""
    try:
        query = db.collection('users').document(user_id).collection('emotions')
        docs = query.stream()
        
        records = []
        for doc in docs:
            record = doc.to_dict()
            record['doc_id'] = doc.id
            
            # dayフィールドが存在することを確認
            if 'day' not in record:
                continue
            
            records.append(record)
        
        if not records:
            return pd.DataFrame()
        
        df = pd.DataFrame(records)
        
        # dayフィールドからdatetimeを作成
        # day: "2025/12/06", time: "10:30" の形式を想定
        if 'time' in df.columns:
            df['datetime'] = pd.to_datetime(df['day'] + ' ' + df['time'], format='%Y/%m/%d %H:%M', errors='coerce')
        else:
            # timeフィールドがない場合はdayのみで日付を作成
            df['datetime'] = pd.to_datetime(df['day'], format='%Y/%m/%d', errors='coerce')
        
        df.dropna(subset=['datetime'], inplace=True)
        
        return df
    except Exception as e:
        print(f"感情記録の取得に失敗 ({user_id}): {e}")
        import traceback
        traceback.print_exc()
        return pd.DataFrame()


def calculate_response_rate_by_user(db, report_file):
    """ユーザーごとの感情入力率を計算"""
    output = []
    output.append("\n実験期間における感情入力率")
    output.append("-" * 80)
    output.append(f"{'ユーザーID':10s} {'実験期間':25s} {'日数':5s} {'総通知数':10s} {'入力数':10s} {'入力率':10s}")
    output.append("-" * 80)
    
    user_stats = []
    
    for user_id, period in EXPERIMENT_PERIODS.items():
        # 感情記録を取得
        df = fetch_emotion_records(db, user_id)
        
        if df.empty:
            output.append(f"{user_id:10s} データなし")
            continue
        
        # 実験期間内のデータをフィルタ
        start_datetime = datetime.combine(period['start'], datetime.min.time())
        end_datetime = datetime.combine(period['end'], datetime.max.time())
        df_period = df[(df['datetime'] >= start_datetime) & (df['datetime'] <= end_datetime)]
        
        # 統計を計算
        days = (period['end'] - period['start']).days + 1
        total_notifications = days * NOTIFICATIONS_PER_DAY
        input_count = len(df_period)
        response_rate = (input_count / total_notifications * 100) if total_notifications > 0 else 0
        
        period_str = f"{period['start']} ~ {period['end']}"
        output.append(f"{user_id:10s} {period_str:25s} {days:5d}日 {total_notifications:10d}回 {input_count:10d}回 {response_rate:9.1f}%")
        
        user_stats.append({
            'user_id': user_id,
            'start_date': period['start'],
            'end_date': period['end'],
            'days': days,
            'total_notifications': total_notifications,
            'input_count': input_count,
            'response_rate': response_rate
        })
    
    # コンソールとファイルの両方に出力
    for line in output:
        print(line)
        report_file.write(line + "\n")
    
    return pd.DataFrame(user_stats)


def calculate_daily_response_rate(db, report_file):
    """経過日数ごとの回答率の平均値を計算"""
    all_daily_rates = []
    
    for user_id, period in EXPERIMENT_PERIODS.items():
        # 感情記録を取得
        df = fetch_emotion_records(db, user_id)
        
        if df.empty:
            continue
        
        # 実験期間内のデータをフィルタ
        start_datetime = datetime.combine(period['start'], datetime.min.time())
        end_datetime = datetime.combine(period['end'], datetime.max.time())
        df_period = df[(df['datetime'] >= start_datetime) & (df['datetime'] <= end_datetime)]
        
        # 日付ごとの入力数を集計
        df_period['date'] = df_period['datetime'].dt.date
        daily_counts = df_period.groupby('date').size()
        
        # 実験期間の全日付を生成
        date_range = pd.date_range(start=period['start'], end=period['end'], freq='D')
        
        for i, current_date in enumerate(date_range):
            elapsed_days = i + 1
            date_obj = current_date.date()
            count = daily_counts.get(date_obj, 0)
            rate = (count / NOTIFICATIONS_PER_DAY * 100) if NOTIFICATIONS_PER_DAY > 0 else 0
            
            all_daily_rates.append({
                'user_id': user_id,
                'elapsed_days': elapsed_days,
                'date': date_obj,
                'count': count,
                'rate': rate
            })
    
    df_daily = pd.DataFrame(all_daily_rates)
    
    if df_daily.empty:
        output = ["\n経過日数ごとの回答率データがありません"]
        for line in output:
            print(line)
            report_file.write(line + "\n")
        return df_daily
    
    # 経過日数ごとの平均回答率を計算
    avg_rates = df_daily.groupby('elapsed_days')['rate'].mean().reset_index()
    
    output = []
    output.append("\n経過日数ごとの平均回答率")
    output.append("-" * 40)
    output.append(f"{'経過日数':10s} {'平均回答率':15s}")
    output.append("-" * 40)
    
    for _, row in avg_rates.iterrows():
        output.append(f"{int(row['elapsed_days']):10d}日目 {row['rate']:14.1f}%")
    
    # コンソールとファイルの両方に出力
    for line in output:
        print(line)
        report_file.write(line + "\n")
    
    return df_daily


def plot_response_rate_by_elapsed_days(df_daily, report_file):
    """経過日数ごとの平均回答率を折れ線グラフで可視化"""
    if df_daily.empty:
        return
    
    # 経過日数ごとの平均回答率を計算
    avg_rates = df_daily.groupby('elapsed_days')['rate'].mean().reset_index()
    
    # グラフを作成
    fig, ax = plt.subplots(figsize=(14, 7))
    
    # 折れ線グラフを描画
    ax.plot(avg_rates['elapsed_days'], avg_rates['rate'], 
            marker='o', linewidth=2, color='#4A90E2', markersize=6)
    
    # グラフの装飾
    ax.set_xlabel('実験開始からの経過日数', fontsize=14)
    ax.set_ylabel('平均回答率 (%)', fontsize=14)
    ax.set_title('経過日数ごとの平均回答率の推移', fontsize=16, fontweight='bold')
    ax.grid(True, linestyle='--', alpha=0.3)
    ax.set_ylim(0, 100)
    
    # X軸の目盛りを設定
    ax.set_xticks(range(1, int(avg_rates['elapsed_days'].max()) + 1, 2))
    
    plt.tight_layout()
    
    # グラフを保存
    plt.savefig('response_rate_by_elapsed_days.png', dpi=300, bbox_inches='tight')
    print("\nグラフを response_rate_by_elapsed_days.png に保存しました")
    report_file.write("\nグラフを response_rate_by_elapsed_days.png に保存しました\n")
    
    plt.close()


def plot_response_rate_by_user_and_days(df_daily, report_file):
    """ユーザーごとの経過日数別回答率を折れ線グラフで可視化"""
    if df_daily.empty:
        return
    
    # グラフを作成
    fig, ax = plt.subplots(figsize=(14, 7))
    
    # 各ユーザーの折れ線グラフを描画
    colors = ['#4A90E2', '#E24A4A', '#4AE290', '#E2904A', '#904AE2']
    for i, user_id in enumerate(sorted(df_daily['user_id'].unique())):
        user_data = df_daily[df_daily['user_id'] == user_id]
        user_rates = user_data.groupby('elapsed_days')['rate'].mean().reset_index()
        
        ax.plot(user_rates['elapsed_days'], user_rates['rate'], 
                marker='o', label=user_id, linewidth=2, 
                color=colors[i % len(colors)], markersize=5, alpha=0.7)
    
    # グラフの装飾
    ax.set_xlabel('実験開始からの経過日数', fontsize=14)
    ax.set_ylabel('回答率 (%)', fontsize=14)
    ax.set_title('ユーザーごとの経過日数別回答率', fontsize=16, fontweight='bold')
    ax.legend(loc='upper left', bbox_to_anchor=(1, 1))
    ax.grid(True, linestyle='--', alpha=0.3)
    ax.set_ylim(0, 100)
    
    plt.tight_layout()
    
    # グラフを保存
    plt.savefig('response_rate_by_user_and_days.png', dpi=300, bbox_inches='tight')
    print("グラフを response_rate_by_user_and_days.png に保存しました")
    report_file.write("グラフを response_rate_by_user_and_days.png に保存しました\n")
    
    plt.close()


def analyze_user_access_counts_from_page_views(db, report_file):
    """ユーザーごとのアクセス回数を集計（page_viewsコレクションから取得）"""
    if not db:
        return
    
    output = []
    output.append("\n各ユーザーのアクセス回数（page_viewsコレクションから）")
    output.append("-" * 60)
    output.append(f"{'ユーザーID':15s} {'アクセス回数':15s} {'割合':15s}")
    output.append("-" * 60)
    
    user_access_counts = {}
    total_access = 0
    
    try:
        # usersコレクションを取得
        users_ref = db.collection('users')
        users = users_ref.stream()
        
        for user_doc in users:
            user_id = user_doc.id
            
            # 各ユーザーのpage_viewsコレクションを取得
            page_views_query = db.collection('users').document(user_id).collection('page_views')
            page_views_docs = page_views_query.stream()
            
            # start_timeフィールドが存在するドキュメントをカウント
            access_count = 0
            for page_view_doc in page_views_docs:
                page_view_data = page_view_doc.to_dict()
                
                # start_timeフィールドが存在することを確認
                if 'start_time' in page_view_data and page_view_data['start_time'] is not None:
                    access_count += 1
            
            if access_count > 0:
                user_access_counts[user_id] = access_count
                total_access += access_count
    
    except Exception as e:
        print(f"page_viewsコレクション読み込みエラー: {e}")
        import traceback
        traceback.print_exc()
        return
    
    # ソートして出力
    for user_id in sorted(user_access_counts.keys()):
        count = user_access_counts[user_id]
        percentage = (count / total_access * 100) if total_access > 0 else 0
        output.append(f"{user_id:15s} {count:15d}回 {percentage:14.1f}%")
    
    output.append("-" * 60)
    output.append(f"{'合計':15s} {total_access:15d}回 {100.0:14.1f}%")
    
    # コンソールとファイルの両方に出力
    for line in output:
        print(line)
        report_file.write(line + "\n")
    
    return user_access_counts


def analyze_user_access_by_view_mode_from_page_views(db, report_file):
    """ユーザーごとの表示モード別アクセス回数を集計（page_viewsコレクションから）"""
    if not db:
        return
    
    output = []
    output.append("\n各ユーザーの表示モード別アクセス回数（page_viewsコレクションから）")
    output.append("=" * 100)
    
    user_mode_data = {}
    
    try:
        # usersコレクションを取得
        users_ref = db.collection('users')
        users = users_ref.stream()
        
        for user_doc in users:
            user_id = user_doc.id
            user_mode_data[user_id] = {}
            
            # 各ユーザーのpage_viewsコレクションを取得
            page_views_query = db.collection('users').document(user_id).collection('page_views')
            page_views_docs = page_views_query.stream()
            
            # view_modeごとにカウント
            for page_view_doc in page_views_docs:
                page_view_data = page_view_doc.to_dict()
                
                # start_timeが存在し、view_modeフィールドがある場合
                if 'start_time' in page_view_data and page_view_data['start_time'] is not None:
                    view_mode = page_view_data.get('view_mode', '不明')
                    
                    if view_mode not in user_mode_data[user_id]:
                        user_mode_data[user_id][view_mode] = 0
                    
                    user_mode_data[user_id][view_mode] += 1
    
    except Exception as e:
        print(f"page_viewsコレクション読み込みエラー: {e}")
        import traceback
        traceback.print_exc()
        return
    
    # ヘッダー行を作成
    all_modes = set()
    for user_modes in user_mode_data.values():
        all_modes.update(user_modes.keys())
    
    all_modes = sorted(list(all_modes))
    
    output.append(f"{'ユーザーID':15s}", end='')
    for mode in all_modes:
        output.append(f" {mode:15s}")
    output.append('')
    output.append("-" * (15 + len(all_modes) * 16))
    
    # 各ユーザーのデータを出力
    for user_id in sorted(user_mode_data.keys()):
        line = f"{user_id:15s}"
        for mode in all_modes:
            count = user_mode_data[user_id].get(mode, 0)
            line += f" {count:15d}"
        output.append(line)
    
    # コンソールとファイルの両方に出力
    for line in output:
        print(line)
        report_file.write(line + "\n")
    
    return user_mode_data


def analyze_daily_access_from_page_views(db, report_file):
    """日別アクセス数を集計（page_viewsコレクションから）"""
    if not db:
        return
    
    output = []
    output.append("\n日別アクセス数（page_viewsコレクションから）")
    output.append("-" * 40)
    output.append(f"{'日付':15s} {'アクセス数':15s}")
    output.append("-" * 40)
    
    daily_counts = {}
    
    try:
        # usersコレクションを取得
        users_ref = db.collection('users')
        users = users_ref.stream()
        
        for user_doc in users:
            user_id = user_doc.id
            
            # 各ユーザーのpage_viewsコレクションを取得
            page_views_query = db.collection('users').document(user_id).collection('page_views')
            page_views_docs = page_views_query.stream()
            
            # start_timeから日付を抽出してカウント
            for page_view_doc in page_views_docs:
                page_view_data = page_view_doc.to_dict()
                
                if 'start_time' in page_view_data and page_view_data['start_time'] is not None:
                    start_time = page_view_data['start_time']
                    
                    # Timestamp型をdateに変換
                    if hasattr(start_time, 'date'):
                        date_key = str(start_time.date())
                    else:
                        date_key = pd.to_datetime(start_time).strftime('%Y-%m-%d')
                    
                    if date_key not in daily_counts:
                        daily_counts[date_key] = 0
                    
                    daily_counts[date_key] += 1
    
    except Exception as e:
        print(f"page_viewsコレクション読み込みエラー: {e}")
        import traceback
        traceback.print_exc()
        return
    
    # ソートして出力
    total_access = 0
    for date_key in sorted(daily_counts.keys()):
        count = daily_counts[date_key]
        output.append(f"{date_key:15s} {count:15d}回")
        total_access += count
    
    output.append("-" * 40)
    output.append(f"{'合計':15s} {total_access:15d}回")
    
    # コンソールとファイルの両方に出力
    for line in output:
        print(line)
        report_file.write(line + "\n")
    
    return daily_counts


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
        
        # ===== page_viewsコレクションからのアクセス分析 =====
        print("\n【page_viewsコレクションからのアクセス分析】")
        
        # ユーザーごとのアクセス回数
        print("ユーザーごとのアクセス回数を集計中...")
        user_access_counts = analyze_user_access_counts_from_page_views(db, report_file)
        
        # ユーザーごとの表示モード別アクセス回数
        print("表示モード別アクセス回数を集計中...")
        user_mode_data = analyze_user_access_by_view_mode_from_page_views(db, report_file)
        
        # 日別アクセス数
        print("日別アクセス数を集計中...")
        daily_counts = analyze_daily_access_from_page_views(db, report_file)
        
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
