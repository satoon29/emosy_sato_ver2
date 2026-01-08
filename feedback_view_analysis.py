import pandas as pd
from datetime import datetime, date, timedelta
import firebase_admin
from firebase_admin import credentials, firestore
import matplotlib.pyplot as plt
import matplotlib.font_manager as fm
import os
import pytz
from scipy import stats

# 日本語フォントを設定
JAPANESE_FONT_PATH = "assets/NotoSansJP-Regular.ttf"
if os.path.exists(JAPANESE_FONT_PATH):
    fm.fontManager.addfont(JAPANESE_FONT_PATH)
    plt.rcParams['font.family'] = 'Noto Sans JP'
else:
    print(f"⚠️ 日本語フォントが見つかりません: {JAPANESE_FONT_PATH}")

# 各群の実験期間定義
GROUPS = {
    'スマートフォン通知条件': {
        'users': ['user21', 'user22', 'user23', 'User24', 'user25'],
        'periods': {
            'user21': {'start': date(2025, 12, 4), 'end': date(2025, 12, 24)},
            'user22': {'start': date(2025, 12, 5), 'end': date(2025, 12, 25)},
            'user23': {'start': date(2025, 12, 6), 'end': date(2025, 12, 26)},
            'User24': {'start': date(2025, 12, 6), 'end': date(2025, 12, 26)},
            'user25': {'start': date(2025, 12, 6), 'end': date(2025, 12, 26)},
        },
        'color': '#4A90E2'
    },
    'ロボット共感条件': {
        'users': ['bocco01', 'bocco02', 'bocco03', 'bocco04', 'bocco05'],
        'periods': {
            'bocco01': {'start': date(2025, 12, 6), 'end': date(2025, 12, 26)},
            'bocco02': {'start': date(2025, 12, 5), 'end': date(2025, 12, 25)},
            'bocco03': {'start': date(2025, 12, 5), 'end': date(2025, 12, 25)},
            'bocco04': {'start': date(2025, 12, 5), 'end': date(2025, 12, 25)},
            'bocco05': {'start': date(2025, 12, 6), 'end': date(2025, 12, 26)},
        },
        'color': '#FF0000'
    },
}

"""
    'ロボット好感群': {
        'users': ['bocco01', 'bocco02'],
        'periods': {
            'bocco01': {'start': date(2025, 12, 6), 'end': date(2025, 12, 26)},
            'bocco02': {'start': date(2025, 12, 5), 'end': date(2025, 12, 25)},
            
        },
        'color': '#FF6666'
    },
    'ロボット不信感群': {
        'users': ['bocco04', 'bocco05', 'bocco03'],
        'periods': {
            'bocco04': {'start': date(2025, 12, 5), 'end': date(2025, 12, 25)},
            'bocco05': {'start': date(2025, 12, 6), 'end': date(2025, 12, 26)},
            'bocco03': {'start': date(2025, 12, 5), 'end': date(2025, 12, 25)},
        },
        'color': '#FFCCCC'
    }
"""

# ユーザー名のマッピング（グラフ表示用）
USER_NAME_MAPPING = {
    'user21': 'P1-A',
    'user22': 'P2-A',
    'user23': 'P3-A',
    'User24': 'P4-A',
    'user25': 'P5-A',
    'bocco01': 'P1-B',
    'bocco02': 'P2-B',
    'bocco03': 'P3-B',
    'bocco04': 'P4-B',
    'bocco05': 'P5-B',
}

def fetch_page_views(db, user_id):
    """特定ユーザーのページビューログをFirestoreから取得"""
    try:
        query = db.collection('users').document(user_id).collection('page_views')
        docs = query.stream()
        
        records = []
        for doc in docs:
            record = doc.to_dict()
            record['doc_id'] = doc.id
            
            if 'start_time' not in record:
                continue
            
            records.append(record)
        
        if not records:
            return pd.DataFrame()
        
        df = pd.DataFrame(records)
        
        # start_timeをdatetimeに変換
        # start_time: "2025年12月16日 11:26:51 UTC+9" の形式
        df['datetime'] = pd.to_datetime(df['start_time'], format='%Y年%m月%d日 %H:%M:%S UTC%z', errors='coerce')
        df.dropna(subset=['datetime'], inplace=True)
        
        return df
        
    except Exception as e:
        print(f"ページビューの取得に失敗 ({user_id}): {e}")
        return pd.DataFrame()


def calculate_daily_feedback_view_rate(db, group_name, group_data, report_file):
    """経過日数ごとのフィードバック閲覧率を計算（1日1回以上閲覧した割合）"""
    all_daily_views = []
    
    for user_id in group_data['users']:
        period = group_data['periods'][user_id]
        
        # ページビューログを取得
        df = fetch_page_views(db, user_id)
        
        if df.empty:
            continue
        
        # 実験期間内のデータをフィルタ
        # UTCタイムゾーン対応
        start_datetime = datetime.combine(period['start'], datetime.min.time())
        end_datetime = datetime.combine(period['end'], datetime.max.time())
        
        # datetimeをUTC対応にする
        start_datetime_utc = pd.Timestamp(start_datetime, tz='UTC')
        end_datetime_utc = pd.Timestamp(end_datetime, tz='UTC')
        
        df_period = df[(df['datetime'] >= start_datetime_utc) & (df['datetime'] <= end_datetime_utc)]
        
        if df_period.empty:
            continue
        
        # 日付ごとに閲覧が1回以上あったかを確認
        df_period['date'] = df_period['datetime'].dt.date
        daily_views = df_period.groupby('date').size() > 0
        daily_views_dict = daily_views.to_dict()
        
        # 実験期間の全日付を生成
        date_range = pd.date_range(start=period['start'], end=period['end'], freq='D')
        
        for i, current_date in enumerate(date_range):
            elapsed_days = i + 1
            date_obj = current_date.date()
            # その日に1回以上閲覧したか（True=1, False=0）
            viewed = 1 if daily_views_dict.get(date_obj, False) else 0
            
            all_daily_views.append({
                'user_id': user_id,
                'group_name': group_name,
                'elapsed_days': elapsed_days,
                'date': date_obj,
                'viewed': viewed
            })
    
    df_daily = pd.DataFrame(all_daily_views)
    return df_daily


def calculate_daily_access_count(db, group_name, group_data, report_file):
    """経過日数ごとの日々のアクセス回数を計算"""
    all_daily_access = []
    
    for user_id in group_data['users']:
        period = group_data['periods'][user_id]
        
        # ページビューログを取得
        df = fetch_page_views(db, user_id)
        
        if df.empty:
            continue
        
        # 実験期間内のデータをフィルタ
        start_datetime = datetime.combine(period['start'], datetime.min.time())
        end_datetime = datetime.combine(period['end'], datetime.max.time())
        
        start_datetime_utc = pd.Timestamp(start_datetime, tz='UTC')
        end_datetime_utc = pd.Timestamp(end_datetime, tz='UTC')
        
        df_period = df[(df['datetime'] >= start_datetime_utc) & (df['datetime'] <= end_datetime_utc)]
        
        if df_period.empty:
            continue
        
        # 日付ごとのアクセス回数をカウント（ドキュメント数 = アクセス回数）
        df_period['date'] = df_period['datetime'].dt.date
        daily_access_counts = df_period.groupby('date').size()  # この行が重要！
        
        # 実験期間の全日付を生成
        date_range = pd.date_range(start=period['start'], end=period['end'], freq='D')
        
        for i, current_date in enumerate(date_range):
            elapsed_days = i + 1
            date_obj = current_date.date()
            # その日のアクセス回数（ドキュメント数）
            access_count = daily_access_counts.get(date_obj, 0)
            
            all_daily_access.append({
                'user_id': user_id,
                'group_name': group_name,
                'elapsed_days': elapsed_days,
                'date': date_obj,
                'access_count': access_count  # アクセス「有無」ではなく「回数」
            })
    
    df_daily = pd.DataFrame(all_daily_access)
    return df_daily


def plot_feedback_view_rate_by_group(all_group_data, report_file):
    """各群のフィードバック閲覧率を折れ線グラフで可視化"""
    
    # グラフを作成
    fig, ax = plt.subplots(figsize=(14, 7))
    
    output = []
    output.append("\n各群のフィードバック閲覧率")
    output.append("=" * 80)
    
    all_daily_rates_combined = []
    max_days = 0
    
    for group_name, group_df in all_group_data.items():
        if group_df.empty:
            continue
        
        # 経過日数ごとの閲覧率を計算（1日1回以上閲覧した人の割合）
        daily_stats = group_df.groupby('elapsed_days').agg({
            'viewed': ['sum', 'count']
        })
        daily_stats.columns = ['viewed_count', 'user_count']
        daily_stats['view_rate'] = (daily_stats['viewed_count'] / daily_stats['user_count'] * 100)
        daily_stats = daily_stats.reset_index()
        daily_stats['elapsed_days'] = daily_stats['elapsed_days'].astype(int)
        
        max_days = max(max_days, int(daily_stats['elapsed_days'].max()))
        
        # グラフに描画
        ax.plot(daily_stats['elapsed_days'], daily_stats['view_rate'], 
                marker='o', label=group_name, linewidth=2.5, 
                color=GROUPS[group_name]['color'], markersize=6, alpha=0.8)
        
        # 全体平均計算用にデータを集約
        all_daily_rates_combined.extend(group_df.to_dict('records'))
        
        # レポートに出力
        output.append(f"\n【{group_name}】")
        output.append("-" * 80)
        output.append(f"{'経過日数':10s} {'閲覧者数':10s} {'対象者数':10s} {'閲覧率':10s}")
        output.append("-" * 80)
        
        for _, row in daily_stats.iterrows():
            output.append(f"{int(row['elapsed_days']):10d} {int(row['viewed_count']):10d} {int(row['user_count']):10d} {row['view_rate']:9.1f}%")
    
    # 全体平均を計算して描画
    if all_daily_rates_combined:
        combined_df = pd.DataFrame(all_daily_rates_combined)
        overall_avg = combined_df.groupby('elapsed_days')['viewed'].agg(['sum', 'count'])
        overall_avg.columns = ['viewed_count', 'user_count']
        overall_avg['view_rate'] = (overall_avg['viewed_count'] / overall_avg['user_count'] * 100)
        overall_avg = overall_avg.reset_index()
        overall_avg['elapsed_days'] = overall_avg['elapsed_days'].astype(int)
        
        ax.plot(overall_avg['elapsed_days'], overall_avg['view_rate'], 
                marker='D', label='全体平均', linewidth=3, 
                color='#000000', markersize=6, alpha=0.9, linestyle='--')
    
    # グラフの装飾
    ax.set_xlabel('実験開始からの経過日数', fontsize=14)
    ax.set_ylabel('フィードバック閲覧率 (%)', fontsize=14)
    ax.legend(loc='lower left', fontsize=12, frameon=True, fancybox=False, shadow=False, edgecolor='black')
    ax.grid(True, linestyle='--', alpha=0.3)
    ax.set_ylim(0, 105)
    
    # X軸の目盛りを整数で設定
    if max_days > 0:
        ax.set_xticks(range(1, max_days + 1, 2))
        ax.set_xticklabels([str(x) for x in range(1, max_days + 1, 2)])
    
    plt.tight_layout()
    
    # グラフを保存
    plt.savefig('feedback_view_rate_by_group.png', dpi=300, bbox_inches='tight')
    print("\nグラフを feedback_view_rate_by_group.png に保存しました")
    report_file.write("\nグラフを feedback_view_rate_by_group.png に保存しました\n")
    
    # レポートに出力
    for line in output:
        print(line)
        report_file.write(line + "\n")
    
    plt.close()


def plot_individual_daily_access_count(all_group_access_data, report_file):
    """個人ごとの日々のアクセス回数を折れ線グラフで可視化"""
    
    for group_name, group_df in all_group_access_data.items():
        if group_df.empty:
            continue
        
        # グラフを作成
        fig, ax = plt.subplots(figsize=(14, 7))
        
        # ユーザーごとの色を定義
        colors = ['#4A90E2', '#E24A4A', '#4AE290', '#E2904A', '#904AE2', '#FF6666', '#FFB366', '#99CC99']
        
        # グループのユーザーリストから順序を保持
        group_users = group_df['user_id'].unique()
        
        max_days = 0
        
        # ユーザーごとに折れ線を描画（グループ内の元の順序を保持）
        for idx, user_id in enumerate(group_users):
            user_data = group_df[group_df['user_id'] == user_id]
            daily_access = user_data.groupby('elapsed_days')['access_count'].sum().reset_index()
            daily_access['elapsed_days'] = daily_access['elapsed_days'].astype(int)
            
            max_days = max(max_days, int(daily_access['elapsed_days'].max()))
            
            # ユーザー名をマッピング
            display_name = USER_NAME_MAPPING.get(user_id, user_id)
            
            ax.plot(daily_access['elapsed_days'], daily_access['access_count'], 
                    marker='o', label=display_name, linewidth=2, 
                    color=colors[idx % len(colors)], markersize=5, alpha=0.8)
        
        # グループ全体の平均アクセス回数を描画
        group_avg_access = group_df.groupby('elapsed_days')['access_count'].mean().reset_index()
        group_avg_access['elapsed_days'] = group_avg_access['elapsed_days'].astype(int)
        
        ax.plot(group_avg_access['elapsed_days'], group_avg_access['access_count'], 
                marker='D', label=f'{group_name}平均', linewidth=3, 
                color=GROUPS[group_name]['color'], markersize=7, alpha=0.9)
        
        # グラフの装飾
        ax.set_xlabel('実験開始からの経過日数', fontsize=14)
        ax.set_ylabel('アクセス回数（回）', fontsize=14)
        ax.legend(loc='upper right', fontsize=11, frameon=True, fancybox=False, shadow=False, edgecolor='black')
        ax.grid(True, linestyle='--', alpha=0.3)
        ax.set_ylim(0, 13)
        
        # X軸の目盛りを整数で設定
        if max_days > 0:
            ax.set_xticks(range(1, max_days + 1, 2))
            ax.set_xticklabels([str(x) for x in range(1, max_days + 1, 2)])
        
        plt.tight_layout()
        
        # グラフを保存
        filename = f'individual_daily_access_count_{group_name.replace(" ", "_")}.png'
        plt.savefig(filename, dpi=300, bbox_inches='tight')
        print(f"グラフを {filename} に保存しました")
        report_file.write(f"グラフを {filename} に保存しました\n")
        
        plt.close()


def plot_group_average_access_count(all_group_access_data, report_file):
    """条件ごとの平均アクセス回数を棒グラフで可視化（標準誤差のエラーバー付き）"""
    
    if not all_group_access_data:
        return
    
    # グラフを作成（横幅を半分に）
    fig, ax = plt.subplots(figsize=(6, 7))
    
    output = []
    output.append("\n条件ごとの平均アクセス回数")
    output.append("=" * 80)
    
    group_names = []
    avg_access_counts = []
    se_access_counts = []
    colors_list = []
    
    for group_name, group_df in all_group_access_data.items():
        if group_df.empty:
            continue
        
        # グループ全体の平均アクセス回数と標準誤差を計算
        data = group_df['access_count'].values
        avg_access = data.mean()
        se = stats.sem(data)  # 標準誤差を計算
        
        group_names.append(group_name)
        avg_access_counts.append(avg_access)
        se_access_counts.append(se)
        colors_list.append(GROUPS[group_name]['color'])
        
        output.append(f"\n{group_name}")
        output.append(f"平均アクセス回数: {avg_access:.2f}回")
        output.append(f"標準誤差: {se:.2f}回")
    
    # 棒グラフを描画（標準誤差のエラーバー付き）
    bars = ax.bar(range(len(group_names)), avg_access_counts, 
                  width=0.6,  # 棒の幅
                  yerr=se_access_counts,  # 標準誤差をエラーバーとして表示
                  color=colors_list, alpha=0.8,
                  capsize=10, error_kw={'elinewidth': 2, 'capthick': 2})  # エラーバーのスタイル設定
    
    # X軸のラベルを設定
    ax.set_xticks(range(len(group_names)))
    ax.set_xticklabels(group_names, fontsize=12)
    
    # グラフの装飾
    ax.set_ylabel('平均アクセス回数（回）', fontsize=14, fontweight='bold')
    ax.grid(axis='y', linestyle='--', alpha=0.3)
    ax.set_ylim(0, 2)
    
    plt.tight_layout()
    
    # グラフを保存
    plt.savefig('group_average_access_count.png', dpi=300, bbox_inches='tight')
    print("\nグラフを group_average_access_count.png に保存しました")
    report_file.write("\nグラフを group_average_access_count.png に保存しました\n")
    
    # レポートに出力
    for line in output:
        print(line)
        report_file.write(line + "\n")
    
    plt.close()


def output_individual_access_details(all_group_access_data, report_file):
    """被験者ごとのアクセス回数の詳細をレポートに出力"""
    
    output = []
    output.append("\n被験者ごとのアクセス回数の詳細")
    output.append("=" * 100)
    
    for group_name, group_df in all_group_access_data.items():
        if group_df.empty:
            continue
        
        output.append(f"\n【{group_name}】")
        output.append("=" * 100)
        
        # グループのユーザーリストから順序を保持
        group_users = group_df['user_id'].unique()
        
        for user_id in group_users:
            user_data = group_df[group_df['user_id'] == user_id]
            display_name = USER_NAME_MAPPING.get(user_id, user_id)
            
            # ユーザーごとの統計情報を計算
            total_access = user_data['access_count'].sum()
            avg_access = user_data['access_count'].mean()
            max_access = user_data['access_count'].max()
            min_access = user_data['access_count'].min()
            
            output.append(f"\n{display_name} ({user_id})")
            output.append("-" * 100)
            output.append(f"{'経過日数':10s} {'アクセス回数':15s}")
            output.append("-" * 100)
            
            # 日ごとのアクセス回数を出力
            user_daily_data = user_data.sort_values('elapsed_days')
            for _, row in user_daily_data.iterrows():
                output.append(f"{int(row['elapsed_days']):10d}日目 {int(row['access_count']):15d}回")
            
            # サマリー統計を出力
            output.append("-" * 100)
            output.append(f"合計アクセス回数: {int(total_access):d}回")
            output.append(f"平均アクセス回数: {avg_access:.2f}回/日")
            output.append(f"最大アクセス回数: {int(max_access):d}回")
            output.append(f"最小アクセス回数: {int(min_access):d}回")
            output.append("")
    
    # コンソールとファイルの両方に出力
    for line in output:
        print(line)
        report_file.write(line + "\n")


def main():
    """メイン処理"""
    report_file = open('feedback_view_analysis_report.txt', 'w', encoding='utf-8')
    
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
        
        # 各群のフィードバック閲覧率を計算
        print("\n各群のフィードバック閲覧率を計算中...")
        all_group_data = {}
        
        for group_name, group_data in GROUPS.items():
            print(f"  {group_name}を処理中...")
            df_daily = calculate_daily_feedback_view_rate(db, group_name, group_data, report_file)
            if not df_daily.empty:
                all_group_data[group_name] = df_daily
        
        # 各群の日々のアクセス回数を計算
        print("\n各群の日々のアクセス回数を計算中...")
        all_group_access_data = {}
        
        for group_name, group_data in GROUPS.items():
            print(f"  {group_name}のアクセス回数を処理中...")
            df_access = calculate_daily_access_count(db, group_name, group_data, report_file)
            if not df_access.empty:
                all_group_access_data[group_name] = df_access
        
        # グラフを作成
        if all_group_data:
            # 群別の比較グラフ
            plot_feedback_view_rate_by_group(all_group_data, report_file)
        
        if all_group_access_data:
            # 個人別のアクセス回数グラフ
            print("\n個人別の日々のアクセス回数グラフを作成中...")
            report_file.write("\n個人別の日々のアクセス回数グラフを作成中...\n")
            plot_individual_daily_access_count(all_group_access_data, report_file)
            
            # 条件ごとの平均アクセス回数グラフ
            print("\n条件ごとの平均アクセス回数グラフを作成中...")
            report_file.write("\n条件ごとの平均アクセス回数グラフを作成中...\n")
            plot_group_average_access_count(all_group_access_data, report_file)
            
            # 被験者ごとのアクセス回数詳細をレポートに出力
            print("\n被験者ごとのアクセス回数の詳細をレポートに出力中...")
            output_individual_access_details(all_group_access_data, report_file)
        
        # 分析完了メッセージ
        final_message = "\n分析完了: feedback_view_analysis_report.txt に保存しました"
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
