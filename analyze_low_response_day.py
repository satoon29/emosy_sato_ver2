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
    'bocco01': {'start': date(2025, 12, 6), 'end': date(2025, 12, 26)},
    'bocco02': {'start': date(2025, 12, 5), 'end': date(2025, 12, 25)},
    'bocco03': {'start': date(2025, 12, 5), 'end': date(2025, 12, 25)},
    'bocco04': {'start': date(2025, 12, 5), 'end': date(2025, 12, 25)},
    'bocco05': {'start': date(2025, 12, 6), 'end': date(2025, 12, 26)},
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


def analyze_low_response_day(db, target_elapsed_days=9, report_file=None):
    """特定の経過日数における各ユーザーの回答率を分析"""
    
    output = []
    output.append(f"\n経過{target_elapsed_days}日目における各被験者の回答率")
    output.append("=" * 80)
    output.append(f"{'ユーザーID':15s} {'実験開始日':15s} {'対象日':15s} {'入力数':10s} {'回答率':10s}")
    output.append("-" * 80)
    
    user_details = []
    
    for user_id, period in EXPERIMENT_PERIODS.items():
        # 感情記録を取得
        df = fetch_emotion_records(db, user_id)
        
        if df.empty:
            output.append(f"{user_id:15s} {str(period['start']):15s} データなし")
            continue
        
        # 実験期間内のデータをフィルタ
        start_datetime = datetime.combine(period['start'], datetime.min.time())
        end_datetime = datetime.combine(period['end'], datetime.max.time())
        df_period = df[(df['datetime'] >= start_datetime) & (df['datetime'] <= end_datetime)]
        
        # 対象日を計算（経過日数 = 1が開始日）
        target_date = period['start'] + timedelta(days=target_elapsed_days - 1)
        
        # その日のデータをフィルタ
        target_date_start = datetime.combine(target_date, datetime.min.time())
        target_date_end = datetime.combine(target_date, datetime.max.time())
        df_target_day = df_period[(df_period['datetime'] >= target_date_start) & 
                                   (df_period['datetime'] <= target_date_end)]
        
        input_count = len(df_target_day)
        response_rate = (input_count / NOTIFICATIONS_PER_DAY * 100) if NOTIFICATIONS_PER_DAY > 0 else 0
        
        output.append(f"{user_id:15s} {str(period['start']):15s} {str(target_date):15s} {input_count:10d}回 {response_rate:9.1f}%")
        
        user_details.append({
            'user_id': user_id,
            'start_date': period['start'],
            'target_date': target_date,
            'input_count': input_count,
            'response_rate': response_rate
        })
    
    # コンソールに出力
    for line in output:
        print(line)
    
    # ファイルに出力
    if report_file:
        for line in output:
            report_file.write(line + "\n")
    
    return pd.DataFrame(user_details)


def plot_low_response_day(user_details_df, target_elapsed_days=9, report_file=None):
    """特定の経過日数における各ユーザーの回答率を棒グラフで可視化"""
    
    if user_details_df.empty:
        return
    
    # グラフを作成
    fig, ax = plt.subplots(figsize=(12, 6))
    
    # 棒グラフを描画
    users = user_details_df['user_id'].values
    rates = user_details_df['response_rate'].values
    
    colors = ['#FF6666' if rate < 50 else '#FF9999' for rate in rates]
    bars = ax.bar(range(len(users)), rates, color=colors, alpha=0.8)
    
    # X軸のラベルを設定
    ax.set_xticks(range(len(users)))
    ax.set_xticklabels(users, rotation=45, ha='right')
    
    # 各バーの上に数値を表示
    for i, (user, rate) in enumerate(zip(users, rates)):
        ax.text(i, rate, f'{rate:.1f}%', ha='center', va='bottom', fontsize=10)
    
    # グラフの装飾
    ax.set_xlabel('ユーザーID', fontsize=12)
    ax.set_ylabel('回答率 (%)', fontsize=12)
    ax.set_title(f'ロボット共感群 経過{target_elapsed_days}日目の回答率', fontsize=14, fontweight='bold')
    ax.axhline(y=50, color='red', linestyle='--', linewidth=2, label='50%基準線')
    ax.set_ylim(0, 100)
    ax.grid(axis='y', linestyle='--', alpha=0.3)
    ax.legend()
    
    plt.tight_layout()
    
    # グラフを保存
    filename = f'response_rate_day{target_elapsed_days}_bocco.pdf'
    plt.savefig(filename, dpi=300, bbox_inches='tight', format='pdf')
    print(f"\nグラフを {filename} に保存しました")
    
    if report_file:
        report_file.write(f"\nグラフを {filename} に保存しました\n")
    
    plt.close()


def main():
    """メイン処理"""
    report_file = open('low_response_analysis.txt', 'w', encoding='utf-8')
    
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
        
        # 経過9日目の分析
        print("\n経過9日目の回答率を分析中...")
        user_details_df = analyze_low_response_day(db, target_elapsed_days=9, report_file=report_file)
        
        # グラフを作成
        if not user_details_df.empty:
            plot_low_response_day(user_details_df, target_elapsed_days=9, report_file=report_file)
        
        # 分析完了メッセージ
        final_message = "\n分析完了: low_response_analysis.txt に保存しました"
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
