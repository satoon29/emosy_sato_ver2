import pandas as pd
from datetime import datetime, timedelta, date
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
    'bocco01': {'start': date(2025, 12, 6), 'end': date(2025, 12, 26)},
    'bocco02': {'start': date(2025, 12, 5), 'end': date(2025, 12, 25)},
    'bocco03': {'start': date(2025, 12, 5), 'end': date(2025, 12, 25)},
    'bocco04': {'start': date(2025, 12, 5), 'end': date(2025, 12, 25)},
    'bocco05': {'start': date(2025, 12, 6), 'end': date(2025, 12, 26)},
}

NOTIFICATIONS_PER_DAY = 20

# ユーザー名のマッピング（グラフ表示用）
USER_NAME_MAPPING = {
    'user21': 'P1-A',
    'user22': 'P2-A',
    'user23': 'P3-A',
    'user24': 'P4-A',
    'user25': 'P5-A',
    'bocco01': 'P1-B',
    'bocco02': 'P2-B',
    'bocco03': 'P3-B',
    'bocco04': 'P4-B',
    'bocco05': 'P5-B',
}


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
        # day: "2024/12/06", time: "10:30" の形式
        if 'time' in df.columns:
            df['datetime'] = pd.to_datetime(df['day'] + ' ' + df['time'], format='%Y/%m/%d %H:%M', errors='coerce')
        else:
            # timeフィールドがない場合はdayのみで日付を作成
            df['datetime'] = pd.to_datetime(df['day'], format='%Y/%m/%d', errors='coerce')
        
        # datetimeの変換に失敗した行を削除
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
            user_stats.append({
                'user_id': user_id,
                'start_date': period['start'],
                'end_date': period['end'],
                'days': (period['end'] - period['start']).days + 1,
                'total_notifications': ((period['end'] - period['start']).days + 1) * NOTIFICATIONS_PER_DAY,
                'input_count': 0,
                'response_rate': 0.0
            })
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
    ax.grid(True, linestyle='--', alpha=0.3)
    ax.set_ylim(0, 100)
    
    # X軸の目盛りを整数で設定
    max_days = int(avg_rates['elapsed_days'].max())
    ax.set_xticks(range(1, max_days + 1, 2))
    ax.set_xticklabels([str(int(x)) for x in range(1, max_days + 1, 2)])
    
    plt.tight_layout()
    
    # グラフを保存
    plt.savefig('response_rate_by_elapsed_days.pdf', dpi=300, bbox_inches='tight', format='pdf')
    print("\nグラフを response_rate_by_elapsed_days.pdf に保存しました")
    report_file.write("\nグラフを response_rate_by_elapsed_days.pdf に保存しました\n")
    
    plt.close()


def plot_response_rate_by_user_and_days(df_daily, report_file):
    """ユーザーごとの経過日数別回答率を折れ線グラフで可視化"""
    if df_daily.empty:
        return
    
    # グラフを作成
    fig, ax = plt.subplots(figsize=(14, 7))
    
    # 各ユーザーの折れ線グラフを描画
    colors_user = ['#4A90E2', '#E24A4A', '#4AE290', '#E2904A', '#904AE2']
    colors_bocco = ['#FF0000', '#FF6666', '#FF9999', '#FFCCCC', '#FF3333']
    
    user_ids = sorted(df_daily['user_id'].unique())
    
    # user群を描画（青系）
    for i, user_id in enumerate([uid for uid in user_ids if uid.startswith('user')]):
        user_data = df_daily[df_daily['user_id'] == user_id]
        user_rates = user_data.groupby('elapsed_days')['rate'].mean().reset_index()
        
        # ユーザー名をマッピング
        display_name = USER_NAME_MAPPING.get(user_id, user_id)
        
        ax.plot(user_rates['elapsed_days'], user_rates['rate'], 
                marker='o', label=display_name, linewidth=2, 
                color=colors_user[i % len(colors_user)], markersize=5, alpha=0.7)
    
    # bocco群を描画（赤系）
    for i, user_id in enumerate([uid for uid in user_ids if uid.startswith('bocco')]):
        user_data = df_daily[df_daily['user_id'] == user_id]
        user_rates = user_data.groupby('elapsed_days')['rate'].mean().reset_index()
        
        # ユーザー名をマッピング
        display_name = USER_NAME_MAPPING.get(user_id, user_id)
        
        ax.plot(user_rates['elapsed_days'], user_rates['rate'], 
                marker='s', label=display_name, linewidth=2, 
                color=colors_bocco[i % len(colors_bocco)], markersize=5, alpha=0.7)
    
    # 全体の平均を描画（黒色、太い線）
    avg_rates = df_daily.groupby('elapsed_days')['rate'].mean().reset_index()
    ax.plot(avg_rates['elapsed_days'], avg_rates['rate'], 
            marker='D', label='全体平均', linewidth=3, 
            color='#000000', markersize=6, alpha=0.9)
    
    # グラフの装飾
    ax.set_xlabel('実験開始からの経過日数', fontsize=14)
    ax.set_ylabel('回答率 (%)', fontsize=14)
    ax.set_title('ユーザーごとの経過日数別回答率（user群とbocco群）', fontsize=16, fontweight='bold')
    ax.legend(loc='upper left', bbox_to_anchor=(1, 1), fontsize=10)
    ax.grid(True, linestyle='--', alpha=0.3)
    ax.set_ylim(0, 100)
    
    plt.tight_layout()
    
    # グラフを保存
    plt.savefig('response_rate_by_user_and_days.pdf', dpi=300, bbox_inches='tight', format='pdf')
    print("グラフを response_rate_by_user_and_days.pdf に保存しました")
    report_file.write("グラフを response_rate_by_user_and_days.pdf に保存しました\n")
    
    plt.close()


def plot_response_rate_by_group(df_daily, report_file):
    """群ごとの平均回答率を折れ線グラフで可視化"""
    if df_daily.empty:
        return
    
    # グラフを作成
    fig, ax = plt.subplots(figsize=(14, 7))
    
    # 群１：スマートフォン通知条件（user21~25）の平均
    user_data = df_daily[df_daily['user_id'].str.startswith(('user', 'User'))]
    user_avg = user_data.groupby('elapsed_days')['rate'].mean().reset_index()
    
    ax.plot(user_avg['elapsed_days'], user_avg['rate'], 
            marker='o', label='スマートフォン通知条件', linewidth=3, 
            color='#4A90E2', markersize=7, alpha=0.8)
    
    # 群２：ロボット共感条件（bocco01~05）の平均
    bocco_data = df_daily[df_daily['user_id'].str.startswith('bocco')]
    bocco_avg = bocco_data.groupby('elapsed_days')['rate'].mean().reset_index()
    
    ax.plot(bocco_avg['elapsed_days'], bocco_avg['rate'], 
            marker='s', label='ロボット共感条件', linewidth=3, 
            color='#FF0000', markersize=7, alpha=0.8)
    
    # 全体平均
    all_avg = df_daily.groupby('elapsed_days')['rate'].mean().reset_index()
    ax.plot(all_avg['elapsed_days'], all_avg['rate'], 
            marker='D', label='全体平均', linewidth=3, 
            color='#000000', markersize=7, alpha=0.9, linestyle='--')
    
    # グラフの装飾
    ax.set_xlabel('実験開始からの経過日数', fontsize=14)
    ax.set_ylabel('平均回答率 (%)', fontsize=14)
    ax.legend(loc='best', fontsize=12)
    ax.grid(True, linestyle='--', alpha=0.3)
    ax.set_ylim(0, 100)
    
    # X軸の目盛りを設定
    max_days = int(all_avg['elapsed_days'].max())
    ax.set_xticks(range(1, max_days + 1, 2))
    
    plt.tight_layout()
    
    # グラフを保存
    plt.savefig('response_rate_by_group.pdf', dpi=300, bbox_inches='tight', format='pdf')
    print("グラフを response_rate_by_group.pdf に保存しました")
    report_file.write("グラフを response_rate_by_group.pdf に保存しました\n")
    
    plt.close()
    
    # グループ統計情報をレポートに追加
    output = []
    output.append("\n群ごとの平均回答率統計")
    output.append("-" * 60)
    output.append(f"{'経過日数':10s} {'群１（スマートフォン）':20s} {'群２（ロボット）':20s} {'全体平均':15s}")
    output.append("-" * 60)
    
    for day in range(1, max_days + 1):
        user_rate = user_avg[user_avg['elapsed_days'] == day]['rate'].values
        bocco_rate = bocco_avg[bocco_avg['elapsed_days'] == day]['rate'].values
        all_rate = all_avg[all_avg['elapsed_days'] == day]['rate'].values
        
        user_str = f"{user_rate[0]:.1f}%" if len(user_rate) > 0 else "N/A"
        bocco_str = f"{bocco_rate[0]:.1f}%" if len(bocco_rate) > 0 else "N/A"
        all_str = f"{all_rate[0]:.1f}%" if len(all_rate) > 0 else "N/A"
        
        output.append(f"{day:10d}日目 {user_str:20s} {bocco_str:20s} {all_str:15s}")
    
    # コンソールとファイルの両方に出力
    for line in output:
        print(line)
        report_file.write(line + "\n")


def main():
    """メイン処理"""
    # レポートファイルを開く
    report_file = open('emotion_response_report.txt', 'w', encoding='utf-8')
    
    try:
        # Firebase初期化
        if not firebase_admin._apps:
            try:
                # ローカル環境の場合: config.pyからパスを取得
                from config import FIREBASE_CREDENTIALS_PATH
                cred = credentials.Certificate(FIREBASE_CREDENTIALS_PATH)
                print(f"Firebase認証情報を読み込みました: {FIREBASE_CREDENTIALS_PATH}")
            except ImportError:
                # Streamlit Cloud環境の場合
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
        
        # 感情入力率の分析
        print("\n感情入力率を計算中...")
        user_response_stats = calculate_response_rate_by_user(db, report_file)
        
        print("経過日数ごとの回答率を計算中...")
        df_daily = calculate_daily_response_rate(db, report_file)
        
        if not df_daily.empty:
            plot_response_rate_by_elapsed_days(df_daily, report_file)
            plot_response_rate_by_user_and_days(df_daily, report_file)
            plot_response_rate_by_group(df_daily, report_file)
        
        # レポート保存完了メッセージ
        final_message = "\n分析完了: emotion_response_report.txt に保存しました"
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
