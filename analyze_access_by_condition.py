import pandas as pd
from datetime import datetime, date, timedelta
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

# 群の定義
CONDITIONS = {
    'アプリ条件': ['user21', 'user22', 'user23', 'User24', 'user25'],
    'ロボット条件': ['bocco01', 'bocco02', 'bocco03', 'bocco04', 'bocco05'],
}

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


def fetch_access_logs(db):
    """アクセスログをFirestoreから取得"""
    query = db.collection('access_logs')
    docs = query.stream()
    
    records = []
    for doc in docs:
        record = doc.to_dict()
        records.append(record)
    
    if not records:
        return pd.DataFrame()
    
    df = pd.DataFrame(records)
    df['date'] = pd.to_datetime(df['timestamp']).dt.date
    
    return df


def classify_users_by_condition(df):
    """ユーザを群に分類"""
    df = df.copy()
    
    def get_condition(user_id):
        for condition, users in CONDITIONS.items():
            if user_id in users:
                return condition
    
    df['condition'] = df['user_id'].apply(get_condition)
    return df


def calculate_daily_access_by_condition(df, report_file):
    """群別の日別アクセス数を計算（実験期間内のみ）"""
    
    # ユーザーを群に分類
    df = classify_users_by_condition(df)
    df['date'] = pd.to_datetime(df['timestamp']).dt.date
    
    # ステップ1: ユーザーごとに実験期間内のデータをフィルタ
    filtered_records = []
    
    for user_id in df['user_id'].unique():
        if user_id not in EXPERIMENT_PERIODS:
            continue
        
        period = EXPERIMENT_PERIODS[user_id]
        user_data = df[df['user_id'] == user_id]
        
        # 実験期間内のデータのみを抽出
        user_data_filtered = user_data[
            (user_data['date'] >= period['start']) & 
            (user_data['date'] <= period['end'])
        ]
        
        filtered_records.append(user_data_filtered)
    
    if not filtered_records:
        return pd.DataFrame()
    
    df_filtered = pd.concat(filtered_records, ignore_index=True)
    
    # ステップ2: 日付と群でグループ化
    daily_by_condition = df_filtered.groupby(['date', 'condition']).size().reset_index(name='access_count')
    daily_by_condition = daily_by_condition.sort_values('date')
    
    # ステップ3: すべての日付を生成（0アクセスの日も含める）
    all_dates = pd.date_range(
        start=min([EXPERIMENT_PERIODS[uid]['start'] for uid in EXPERIMENT_PERIODS.keys()]),
        end=max([EXPERIMENT_PERIODS[uid]['end'] for uid in EXPERIMENT_PERIODS.keys()]),
        freq='D'
    ).date
    
    # すべての日付と条件の組み合わせを作成
    from itertools import product
    conditions = ['アプリ条件', 'ロボット条件']
    all_combinations = list(product(all_dates, conditions))
    all_dates_conditions = pd.DataFrame(all_combinations, columns=['date', 'condition'])
    
    # 既存データとマージ（0アクセスの日は0で埋める）
    daily_by_condition = all_dates_conditions.merge(
        daily_by_condition,
        on=['date', 'condition'],
        how='left'
    ).fillna(0)
    daily_by_condition['access_count'] = daily_by_condition['access_count'].astype(int)
    daily_by_condition = daily_by_condition.sort_values('date')
    
    output = []
    output.append("\n群別の日別アクセス数（実験期間内のみ）")
    output.append("=" * 60)
    
    for condition in ['アプリ条件', 'ロボット条件']:
        output.append(f"\n【{condition}】")
        output.append("-" * 60)
        output.append(f"{'日付':15s} {'アクセス数':15s}")
        output.append("-" * 60)
        
        condition_data = daily_by_condition[daily_by_condition['condition'] == condition]
        total_access = 0
        
        for _, row in condition_data.iterrows():
            output.append(f"{str(row['date']):15s} {int(row['access_count']):15d}回")
            total_access += row['access_count']
        
        output.append("-" * 60)
        output.append(f"{'合計':15s} {total_access:15d}回")
    
    # コンソールとファイルの両方に出力
    for line in output:
        print(line)
        report_file.write(line + "\n")
    
    return daily_by_condition


def calculate_cumulative_access_by_condition(df, report_file):
    """群別の累積アクセス数を計算"""
    
    # ユーザーを群に分類
    df = classify_users_by_condition(df)
    
    # 日付と群でグループ化して累積を計算
    daily_by_condition = df.groupby(['date', 'condition']).size().reset_index(name='access_count')
    daily_by_condition = daily_by_condition.sort_values('date')
    
    # 各群ごとに累積を計算
    cumulative_data = []
    for condition in ['アプリ条件', 'ロボット条件']:
        condition_data = daily_by_condition[daily_by_condition['condition'] == condition].copy()
        condition_data['cumulative'] = condition_data['access_count'].cumsum()
        cumulative_data.append(condition_data)
    
    cumulative_df = pd.concat(cumulative_data, ignore_index=True)
    
    output = []
    output.append("\n群別の累積アクセス数")
    output.append("=" * 75)
    
    for condition in ['アプリ条件', 'ロボット条件']:
        output.append(f"\n【{condition}】")
        output.append("-" * 75)
        output.append(f"{'日付':15s} {'日別アクセス数':15s} {'累積アクセス数':15s}")
        output.append("-" * 75)
        
        condition_data = cumulative_df[cumulative_df['condition'] == condition]
        
        for _, row in condition_data.iterrows():
            output.append(f"{str(row['date']):15s} {int(row['access_count']):15d}回 {int(row['cumulative']):15d}回")
    
    # コンソールとファイルの両方に出力
    for line in output:
        print(line)
        report_file.write(line + "\n")
    
    return cumulative_df


def calculate_user_total_access(df, report_file):
    """ユーザーごとの総アクセス回数を計算（実験期間内のみ）"""
    
    df = classify_users_by_condition(df)
    
    output = []
    output.append("\n実験期間におけるユーザーごとの総アクセス回数")
    output.append("=" * 100)
    
    # 条件ごとに集計
    for condition in ['アプリ条件', 'ロボット条件']:
        output.append(f"\n【{condition}】")
        output.append("-" * 100)
        output.append(f"{'ユーザーID':15s} {'実験期間':25s} {'総アクセス数':15s}")
        output.append("-" * 100)
        
        condition_df = df[df['condition'] == condition]
        
        total_all = 0
        for condition_name, users in CONDITIONS.items():
            if condition_name != condition:
                continue
            
            for user_id in users:
                # そのユーザーの実験期間を取得
                if user_id not in EXPERIMENT_PERIODS:
                    continue
                
                period = EXPERIMENT_PERIODS[user_id]
                
                # 実験期間内のアクセスのみをフィルタ
                user_data = condition_df[condition_df['user_id'] == user_id]
                user_data_filtered = user_data[
                    (user_data['date'] >= period['start']) & 
                    (user_data['date'] <= period['end'])
                ]
                
                count = len(user_data_filtered)
                period_str = f"{period['start']}～{period['end']}"
                output.append(f"{user_id:15s} {period_str:25s} {count:15d}回")
                total_all += count
        
        output.append("-" * 100)
        output.append(f"{'合計':15s} {' ':25s} {total_all:15d}回")
    
    # コンソールとファイルの両方に出力
    for line in output:
        print(line)
        report_file.write(line + "\n")


def plot_daily_access_comparison(daily_df, report_file):
    """群別の日別アクセス数を折れ線グラフで比較（横軸：経過日数、最大21日、実験期間内のみ、0アクセスの日も表示）"""
    
    # 日データに経過日数を追加するためのマッピングを作成
    daily_df_copy = daily_df.copy()
    
    # 条件ごとに経過日数を計算
    elapsed_days_list = []
    valid_rows = []
    
    for idx, row in daily_df_copy.iterrows():
        condition = row['condition']
        date_val = row['date']
        
        # 条件に属するユーザーを取得
        users_in_condition = CONDITIONS[condition]
        
        # その日を基準とした経過日数を計算
        valid_date = False
        min_elapsed_days = float('inf')
        
        for user_id in users_in_condition:
            if user_id in EXPERIMENT_PERIODS:
                period = EXPERIMENT_PERIODS[user_id]
                
                # 実験期間内の日付のみを対象
                if period['start'] <= date_val <= period['end']:
                    valid_date = True
                    elapsed_days = (date_val - period['start']).days + 1
                    if elapsed_days > 0 and elapsed_days <= 21:
                        min_elapsed_days = min(min_elapsed_days, elapsed_days)
        
        # 有効な経過日数がない場合はスキップ
        if valid_date and min_elapsed_days != float('inf'):
            elapsed_days_list.append(min_elapsed_days)
            valid_rows.append(idx)
    
    # 有効なデータのみを保持
    daily_df_copy = daily_df_copy.loc[valid_rows].copy()
    daily_df_copy['elapsed_days'] = elapsed_days_list
    daily_df_copy['elapsed_days'] = daily_df_copy['elapsed_days'].astype(int)
    
    # 21日以降のデータを除外
    daily_df_copy = daily_df_copy[daily_df_copy['elapsed_days'] <= 21]
    
    if daily_df_copy.empty:
        print("経過日数の計算に失敗しました")
        return
    
    # グラフを作成
    fig, ax = plt.subplots(figsize=(14, 7))
    
    # 条件ごとのデータを処理
    for condition in ['アプリ条件', 'ロボット条件']:
        condition_data = daily_df_copy[daily_df_copy['condition'] == condition].sort_values('elapsed_days')
        
        color = '#4A90E2' if condition == 'アプリ条件' else '#FF0000'
        marker = 'o' if condition == 'アプリ条件' else 's'
        
        ax.plot(condition_data['elapsed_days'], condition_data['access_count'],
                marker=marker, label=condition, linewidth=2.5, color=color, markersize=7, alpha=0.8)
    
    # 全体平均を計算
    overall_avg = daily_df_copy.groupby('elapsed_days')['access_count'].mean().reset_index()
    
    # 全体平均を黒い点線で描画
    ax.plot(overall_avg['elapsed_days'], overall_avg['access_count'],
            marker='D', label='全体平均', linewidth=2.5, color='#000000', 
            markersize=6, alpha=0.7, linestyle='--')
    
    # グラフの装飾
    ax.set_xlabel('実験開始からの経過日数', fontsize=12)
    ax.set_ylabel('アクセス数', fontsize=12)
    ax.legend(fontsize=11, loc='upper left')
    ax.grid(True, linestyle='--', alpha=0.3)
    
    # X軸の範囲と目盛りを固定（1～21日）
    ax.set_xlim(0, 22)
    ax.set_xticks(range(1, 22, 2))
    ax.set_xticklabels([str(int(x)) for x in range(1, 22, 2)])
    
    plt.tight_layout()
    
    plt.savefig('daily_access_comparison.pdf', dpi=300, bbox_inches='tight', format='pdf')
    print("\nグラフを daily_access_comparison.pdf に保存しました")
    report_file.write("\nグラフを daily_access_comparison.pdf に保存しました\n")
    
    plt.close()


def plot_cumulative_access_comparison(cumulative_df, report_file):
    """群別の累積アクセス数を折れ線グラフで比較"""
    
    # 日データに経過日数を追加するためのマッピングを作成
    cumulative_df_copy = cumulative_df.copy()
    
    # 条件ごとに経過日数を計算
    elapsed_days_list = []
    valid_rows = []
    
    for idx, row in cumulative_df_copy.iterrows():
        condition = row['condition']
        date_val = row['date']
        
        # 条件に属するユーザーを取得
        users_in_condition = CONDITIONS[condition]
        
        # その日を基準とした経過日数を計算
        valid_date = False
        min_elapsed_days = float('inf')
        
        for user_id in users_in_condition:
            if user_id in EXPERIMENT_PERIODS:
                period = EXPERIMENT_PERIODS[user_id]
                
                # 実験期間内の日付のみを対象
                if period['start'] <= date_val <= period['end']:
                    valid_date = True
                    elapsed_days = (date_val - period['start']).days + 1
                    if elapsed_days > 0 and elapsed_days <= 21:
                        min_elapsed_days = min(min_elapsed_days, elapsed_days)
        
        # 有効な経過日数がない場合はスキップ
        if valid_date and min_elapsed_days != float('inf'):
            elapsed_days_list.append(min_elapsed_days)
            valid_rows.append(idx)
    
    # 有効なデータのみを保持
    cumulative_df_copy = cumulative_df_copy.loc[valid_rows].copy()
    cumulative_df_copy['elapsed_days'] = elapsed_days_list
    cumulative_df_copy['elapsed_days'] = cumulative_df_copy['elapsed_days'].astype(int)
    
    # 21日以降のデータを除外
    cumulative_df_copy = cumulative_df_copy[cumulative_df_copy['elapsed_days'] <= 21]
    
    if cumulative_df_copy.empty:
        print("経過日数の計算に失敗しました")
        return
    
    # グラフを作成
    fig, ax = plt.subplots(figsize=(14, 7))
    
    # 条件ごとのデータを処理
    for condition in ['アプリ条件', 'ロボット条件']:
        condition_data = cumulative_df_copy[cumulative_df_copy['condition'] == condition].sort_values('elapsed_days')
        
        color = '#4A90E2' if condition == 'アプリ条件' else '#FF0000'
        marker = 'o' if condition == 'アプリ条件' else 's'
        
        ax.plot(condition_data['elapsed_days'], condition_data['cumulative'],
                marker=marker, label=condition, linewidth=2.5, color=color, markersize=7, alpha=0.8)
    
    # グラフの装飾
    ax.set_xlabel('実験開始からの経過日数', fontsize=12)
    ax.set_ylabel('累積アクセス数', fontsize=12)
    ax.legend(fontsize=11, loc='upper left')
    ax.grid(True, linestyle='--', alpha=0.3)
    
    # X軸の範囲と目盛りを固定（1～21日）
    ax.set_xlim(0, 22)
    ax.set_xticks(range(1, 22, 2))
    ax.set_xticklabels([str(int(x)) for x in range(1, 22, 2)])
    
    plt.tight_layout()
    
    plt.savefig('cumulative_access_comparison.pdf', dpi=300, bbox_inches='tight', format='pdf')
    print("グラフを cumulative_access_comparison.pdf に保存しました")
    report_file.write("グラフを cumulative_access_comparison.pdf に保存しました\n")
    
    plt.close()


def plot_access_distribution(df, report_file):
    """群別のアクセス回数分布を棒グラフで表示"""
    
    df = classify_users_by_condition(df)
    
    # 各群の総アクセス数を集計
    condition_totals = df.groupby('condition')['user_id'].count().reset_index(name='total_access')
    condition_totals = condition_totals.sort_values('condition')
    
    fig, ax = plt.subplots(figsize=(10, 6))
    
    colors = ['#4A90E2', '#FF0000']
    bars = ax.bar(range(len(condition_totals)), condition_totals['total_access'].values, 
                   color=colors, alpha=0.8, width=0.6)
    
    # X軸のラベル
    ax.set_xticks(range(len(condition_totals)))
    ax.set_xticklabels(condition_totals['condition'].values, fontsize=12)
    
    # 各バーの上に数値を表示
    for i, (_, row) in enumerate(condition_totals.iterrows()):
        ax.text(i, row['total_access'], str(int(row['total_access'])),
                ha='center', va='bottom', fontsize=12, fontweight='bold')
    
    # グラフの装飾
    ax.set_ylabel('アクセス数', fontsize=12)
    ax.grid(axis='y', linestyle='--', alpha=0.3)

    # x軸の目盛りを調整(1-21の範囲のみ描画)
    ax.set_xlim(left=1)
    ax.set_xticks(range(1, 22, 2))
    plt.xticks(rotation=45, ha='right')
    
    plt.tight_layout()
    
    plt.savefig('access_distribution_by_condition.pdf', dpi=300, bbox_inches='tight', format='pdf')
    print("グラフを access_distribution_by_condition.pdf に保存しました")
    report_file.write("グラフを access_distribution_by_condition.pdf に保存しました\n")
    
    plt.close()


def plot_user_detail_comparison(df, report_file):
    """ユーザーごとのアクセス回数を群別に表示"""
    
    df = classify_users_by_condition(df)
    
    # ユーザーごとのアクセス数を集計
    user_access = df.groupby(['condition', 'user_id']).size().reset_index(name='access_count')
    user_access = user_access.sort_values(['condition', 'access_count'], ascending=[True, False])
    
    fig, axes = plt.subplots(1, 2, figsize=(14, 6))
    
    conditions = ['アプリ条件', 'ロボット条件']
    colors_list = [['#4A90E2', '#5BA3F5', '#7CB5FF'], ['#FF0000', '#FF6666', '#FF9999']]
    
    for idx, condition in enumerate(conditions):
        condition_data = user_access[user_access['condition'] == condition]
        
        ax = axes[idx]
        bars = ax.bar(range(len(condition_data)), condition_data['access_count'].values,
                      color=colors_list[idx], alpha=0.8)
        
        # X軸のラベル
        ax.set_xticks(range(len(condition_data)))
        ax.set_xticklabels(condition_data['user_id'].values, rotation=45, ha='right', fontsize=10)
        
        # 各バーの上に数値を表示
        for i, (_, row) in enumerate(condition_data.iterrows()):
            ax.text(i, row['access_count'], str(int(row['access_count'])),
                    ha='center', va='bottom', fontsize=10, fontweight='bold')
        
        # グラフの装飾
        ax.set_ylabel('アクセス数', fontsize=11)
        ax.grid(axis='y', linestyle='--', alpha=0.3)
    
    plt.tight_layout()
    
    plt.savefig('user_access_by_condition.pdf', dpi=300, bbox_inches='tight', format='pdf')
    print("グラフを user_access_by_condition.pdf に保存しました")
    report_file.write("グラフを user_access_by_condition.pdf に保存しました\n")
    
    plt.close()


def plot_user_daily_access_transition(daily_df, report_file):
    """ユーザーごとの日別アクセス数推移を折れ線グラフで可視化"""
    
    if daily_df.empty:
        return
    
    # 日データに経過日数を追加するためのマッピングを作成
    daily_df_copy = daily_df.copy()
    
    # 条件ごとに経過日数を計算
    elapsed_days_list = []
    valid_rows = []
    
    for idx, row in daily_df_copy.iterrows():
        condition = row['condition']
        date_val = row['date']
        
        # 条件に属するユーザーを取得
        users_in_condition = CONDITIONS[condition]
        
        # その日を基準とした経過日数を計算
        valid_date = False
        min_elapsed_days = float('inf')
        
        for user_id in users_in_condition:
            if user_id in EXPERIMENT_PERIODS:
                period = EXPERIMENT_PERIODS[user_id]
                
                # 実験期間内の日付のみを対象
                if period['start'] <= date_val <= period['end']:
                    valid_date = True
                    elapsed_days = (date_val - period['start']).days + 1
                    if elapsed_days > 0 and elapsed_days <= 21:
                        min_elapsed_days = min(min_elapsed_days, elapsed_days)
        
        # 有効な経過日数がない場合はスキップ
        if valid_date and min_elapsed_days != float('inf'):
            elapsed_days_list.append(min_elapsed_days)
            valid_rows.append(idx)
    
    # 有効なデータのみを保持
    daily_df_copy = daily_df_copy.loc[valid_rows].copy()
    daily_df_copy['elapsed_days'] = elapsed_days_list
    daily_df_copy['elapsed_days'] = daily_df_copy['elapsed_days'].astype(int)
    
    # 21日以降のデータを除外
    daily_df_copy = daily_df_copy[daily_df_copy['elapsed_days'] <= 21]
    
    if daily_df_copy.empty:
        print("経過日数の計算に失敗しました")
        return
    
    # グラフを作成
    fig, ax = plt.subplots(figsize=(14, 7))
    
    # 各ユーザーのデータを処理
    colors_app = ['#4A90E2', '#5BA3F5', '#7CB5FF', '#9ECBFF', '#C0E0FF']
    colors_robot = ['#FF0000', '#FF6666', '#FF9999', '#FFCCCC', '#FF3333']
    
    # アプリ条件のユーザーをプロット
    app_users = CONDITIONS['アプリ条件']
    for i, user_id in enumerate(app_users):
        user_data = daily_df_copy[daily_df_copy['condition'] == 'アプリ条件'].copy()
        # ここでユーザーIDでさらにフィルタリング（条件は同じだが、元データにuser_idがないため群レベルの集計）
        ax.plot([], [], color=colors_app[i], linewidth=2.5, marker='o', label=user_id, markersize=6, alpha=0.8)
    
    # ロボット条件のユーザーをプロット
    robot_users = CONDITIONS['ロボット条件']
    for i, user_id in enumerate(robot_users):
        ax.plot([], [], color=colors_robot[i], linewidth=2.5, marker='s', label=user_id, markersize=6, alpha=0.8)
    
    # 条件ごとのデータを処理して線を描画
    for condition in ['アプリ条件', 'ロボット条件']:
        condition_data = daily_df_copy[daily_df_copy['condition'] == condition].sort_values('elapsed_days')
        
        color = '#4A90E2' if condition == 'アプリ条件' else '#FF0000'
        marker = 'o' if condition == 'アプリ条件' else 's'
        
        # 群全体の線
        ax.plot(condition_data['elapsed_days'], condition_data['access_count'],
                marker=marker, linewidth=2.5, color=color, markersize=7, alpha=0.5, linestyle=':')
    
    # グラフの装飾
    ax.set_xlabel('実験開始からの経過日数', fontsize=12)
    ax.set_ylabel('アクセス数', fontsize=12)
    ax.legend(fontsize=10, loc='upper left', bbox_to_anchor=(1, 1), ncol=1)
    ax.grid(True, linestyle='--', alpha=0.3)
    
    # X軸の範囲と目盛りを固定（1～21日）
    ax.set_xlim(0, 22)
    ax.set_xticks(range(1, 22, 2))
    ax.set_xticklabels([str(int(x)) for x in range(1, 22, 2)])
    
    plt.tight_layout()
    
    plt.savefig('user_daily_access_transition.pdf', dpi=300, bbox_inches='tight', format='pdf')
    print("グラフを user_daily_access_transition.pdf に保存しました")
    report_file.write("グラフを user_daily_access_transition.pdf に保存しました\n")
    
    plt.close()


def plot_user_daily_access_detailed(df, report_file):
    """ユーザーごとの日別アクセス数推移（詳細版）を折れ線グラフで可視化"""
    
    if df.empty:
        return
    
    df_copy = df.copy()
    df_copy = classify_users_by_condition(df_copy)
    
    # user_idごとに日別アクセス数を集計
    df_copy['date'] = pd.to_datetime(df_copy['timestamp']).dt.date
    
    # グラフを作成
    fig, (ax1, ax2) = plt.subplots(2, 1, figsize=(14, 10))
    
    colors = ['#4A90E2', '#5BA3F5', '#7CB5FF', '#9ECBFF', '#C0E0FF']
    
    # アプリ条件のグラフ
    app_condition_data = df_copy[df_copy['condition'] == 'アプリ条件']
    app_users = sorted(app_condition_data['user_id'].unique())
    
    for i, user_id in enumerate(app_users):
        user_data = app_condition_data[app_condition_data['user_id'] == user_id]
        daily_counts = user_data.groupby('date').size().sort_index()
        
        ax1.plot(daily_counts.index, daily_counts.values, 
                marker='o', label=user_id, linewidth=2, color=colors[i % len(colors)], markersize=5, alpha=0.8)
    
    ax1.set_xlabel('日付', fontsize=11)
    ax1.set_ylabel('アクセス数', fontsize=11)
    ax1.set_title('アプリ条件：ユーザーごとの日別アクセス数推移', fontsize=12, fontweight='bold')
    ax1.legend(fontsize=9, loc='upper left')
    ax1.grid(True, linestyle='--', alpha=0.3)
    ax1.tick_params(axis='x', rotation=45)
    
    # ロボット条件のグラフ
    colors_robot = ['#FF0000', '#FF6666', '#FF9999', '#FFCCCC', '#FF3333']
    
    robot_condition_data = df_copy[df_copy['condition'] == 'ロボット条件']
    robot_users = sorted(robot_condition_data['user_id'].unique())
    
    for i, user_id in enumerate(robot_users):
        user_data = robot_condition_data[robot_condition_data['user_id'] == user_id]
        daily_counts = user_data.groupby('date').size().sort_index()
        
        ax2.plot(daily_counts.index, daily_counts.values, 
                marker='s', label=user_id, linewidth=2, color=colors_robot[i % len(colors_robot)], markersize=5, alpha=0.8)
    
    ax2.set_xlabel('日付', fontsize=11)
    ax2.set_ylabel('アクセス数', fontsize=11)
    ax2.set_title('ロボット条件：ユーザーごとの日別アクセス数推移', fontsize=12, fontweight='bold')
    ax2.legend(fontsize=9, loc='upper left')
    ax2.grid(True, linestyle='--', alpha=0.3)
    ax2.tick_params(axis='x', rotation=45)
    
    plt.tight_layout()
    
    plt.savefig('user_daily_access_detailed.pdf', dpi=300, bbox_inches='tight', format='pdf')
    print("グラフを user_daily_access_detailed.pdf に保存しました")
    report_file.write("グラフを user_daily_access_detailed.pdf に保存しました\n")
    
    plt.close()


def main():
    """メイン処理"""
    report_file = open('access_by_condition_report.txt', 'w', encoding='utf-8')
    
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
        
        # アクセスログを取得
        print("\nアクセスログを取得中...")
        df = fetch_access_logs(db)
        
        if df.empty:
            message = "アクセスログがありません"
            print(message)
            report_file.write(message + "\n")
            return
        
        print(f"アクセスログ取得完了: {len(df)}件")
        
        # ユーザーごとの総アクセス回数を計算
        print("\nユーザーごとの総アクセス回数を計算中...")
        calculate_user_total_access(df, report_file)
        
        # 日別アクセス数を計算
        print("日別アクセス数を計算中...")
        daily_df = calculate_daily_access_by_condition(df, report_file)
        
        # 累積アクセス数を計算
        print("累積アクセス数を計算中...")
        cumulative_df = calculate_cumulative_access_by_condition(df, report_file)
        
        # グラフを生成
        print("\nグラフを生成中...")
        plot_daily_access_comparison(daily_df, report_file)
        plot_cumulative_access_comparison(cumulative_df, report_file)
        plot_access_distribution(df, report_file)
        plot_user_detail_comparison(df, report_file)
        
        # ユーザーごとの日別アクセス推移グラフを生成
        plot_user_daily_access_transition(daily_df, report_file)
        plot_user_daily_access_detailed(df, report_file)
        
        # 完了メッセージ
        final_message = "\n分析完了: access_by_condition_report.txt に保存しました"
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
