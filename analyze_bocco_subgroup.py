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

# bocco01~03の実験期間
BOCCO_SUBGROUP = {
    'bocco01': {'start': date(2025, 12, 6), 'end': date(2025, 12, 26)},
    'bocco02': {'start': date(2025, 12, 5), 'end': date(2025, 12, 25)},
    'bocco03': {'start': date(2025, 12, 5), 'end': date(2025, 12, 25)},
}

NOTIFICATIONS_PER_DAY = 20


def fetch_emotion_records(db, user_id):
    """特定ユーザーの感情記録をFirestoreから取得"""
    try:
        query = db.collection('users').document(user_id).collection('emotions')
        docs = query.stream()
        
        records = []
        for doc in docs:
            record = doc.to_dict()
            record['doc_id'] = doc.id
            
            if 'day' not in record:
                continue
            
            records.append(record)
        
        if not records:
            return pd.DataFrame()
        
        df = pd.DataFrame(records)
        
        if 'time' in df.columns:
            df['datetime'] = pd.to_datetime(df['day'] + ' ' + df['time'], format='%Y/%m/%d %H:%M', errors='coerce')
        else:
            df['datetime'] = pd.to_datetime(df['day'], format='%Y/%m/%d', errors='coerce')
        
        df.dropna(subset=['datetime'], inplace=True)
        
        return df
        
    except Exception as e:
        print(f"感情記録の取得に失敗 ({user_id}): {e}")
        return pd.DataFrame()


def calculate_daily_response_rate_subgroup(db, report_file):
    """bocco01~03の経過日数ごとの回答率を計算"""
    all_daily_rates = []
    
    for user_id, period in BOCCO_SUBGROUP.items():
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
        output = ["\nbocco01~03の回答率データがありません"]
        for line in output:
            print(line)
            report_file.write(line + "\n")
        return df_daily
    
    # 経過日数ごとの平均回答率を計算
    avg_rates = df_daily.groupby('elapsed_days')['rate'].mean().reset_index()
    
    output = []
    output.append("\nbocco01~03の経過日数ごとの回答率")
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


def plot_bocco_subgroup_response_rate(df_daily, report_file):
    """bocco01~03の経過日数別回答率を折れ線グラフで可視化"""
    if df_daily.empty:
        return
    
    # グラフを作成
    fig, ax = plt.subplots(figsize=(14, 7))
    
    # 各ユーザーの折れ線グラフを描画
    colors = ['#FF0000', '#FF6666', '#FF9999']
    
    for i, user_id in enumerate(sorted(df_daily['user_id'].unique())):
        user_data = df_daily[df_daily['user_id'] == user_id]
        user_rates = user_data.groupby('elapsed_days')['rate'].mean().reset_index()
        
        ax.plot(user_rates['elapsed_days'], user_rates['rate'], 
                marker='o', label=user_id, linewidth=2.5, 
                color=colors[i % len(colors)], markersize=6, alpha=0.8)
    
    # bocco01~03の平均を描画（濃い赤）
    avg_rates = df_daily.groupby('elapsed_days')['rate'].mean().reset_index()
    ax.plot(avg_rates['elapsed_days'], avg_rates['rate'], 
            marker='D', label='bocco01~03平均', linewidth=3, 
            color='#CC0000', markersize=7, alpha=0.9)
    
    # グラフの装飾
    ax.set_xlabel('実験開始からの経過日数', fontsize=14)
    ax.set_ylabel('回答率 (%)', fontsize=14)
    ax.set_title('bocco01~03の経過日数別回答率の比較', fontsize=16, fontweight='bold')
    ax.legend(loc='best', fontsize=11)
    ax.grid(True, linestyle='--', alpha=0.3)
    ax.set_ylim(0, 100)
    
    plt.tight_layout()
    
    # グラフを保存
    plt.savefig('response_rate_bocco01_03.png', dpi=300, bbox_inches='tight')
    print("\nグラフを response_rate_bocco01_03.png に保存しました")
    report_file.write("\nグラフを response_rate_bocco01_03.png に保存しました\n")
    
    plt.close()


def plot_bocco_subgroup_table(df_daily, report_file):
    """bocco01~03の詳細テーブルを描画"""
    if df_daily.empty:
        return
    
    output = []
    output.append("\nbocco01~03の詳細回答率テーブル")
    output.append("=" * 80)
    
    # ピボットテーブルを作成
    pivot = df_daily.pivot_table(
        index='elapsed_days',
        columns='user_id',
        values='rate',
        aggfunc='mean'
    )
    
    # 平均を追加
    pivot['平均'] = pivot.mean(axis=1)
    
    # テーブルヘッダーを出力
    header = f"{'経過日数':10s}"
    for col in pivot.columns:
        header += f" {col:12s}"
    output.append(header)
    output.append("-" * 80)
    
    # テーブル本体を出力
    for day, row in pivot.iterrows():
        line = f"{int(day):10d}日目"
        for col in pivot.columns:
            value = row[col]
            if pd.notna(value):
                line += f" {value:11.1f}%"
            else:
                line += f" {'N/A':>11s}"
        output.append(line)
    
    # コンソールとファイルの両方に出力
    for line in output:
        print(line)
        report_file.write(line + "\n")


def main():
    """メイン処理"""
    report_file = open('bocco_subgroup_analysis.txt', 'w', encoding='utf-8')
    
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
        
        # bocco01~03の分析
        print("\nbocco01~03の回答率を計算中...")
        df_daily = calculate_daily_response_rate_subgroup(db, report_file)
        
        if not df_daily.empty:
            plot_bocco_subgroup_response_rate(df_daily, report_file)
            plot_bocco_subgroup_table(df_daily, report_file)
        
        # 分析完了メッセージ
        final_message = "\n分析完了: bocco_subgroup_analysis.txt に保存しました"
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
