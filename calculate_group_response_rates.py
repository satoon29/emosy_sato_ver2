import pandas as pd
from datetime import datetime, date
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

# 各群の実験期間定義
GROUPS = {
    'スマートフォン通知群': {
        'users': ['user21', 'user22', 'user23', 'User24', 'user25'],
        'periods': {
            'user21': {'start': date(2025, 12, 4), 'end': date(2025, 12, 24)},
            'user22': {'start': date(2025, 12, 5), 'end': date(2025, 12, 25)},
            'user23': {'start': date(2025, 12, 6), 'end': date(2025, 12, 26)},
            'User24': {'start': date(2025, 12, 6), 'end': date(2025, 12, 26)},
            'user25': {'start': date(2025, 12, 6), 'end': date(2025, 12, 26)},
        }
    },
    'ロボット共感群': {
        'users': ['bocco01', 'bocco02', 'bocco03', 'bocco04', 'bocco05'],
        'periods': {
            'bocco01': {'start': date(2025, 12, 6), 'end': date(2025, 12, 26)},
            'bocco02': {'start': date(2025, 12, 5), 'end': date(2025, 12, 25)},
            'bocco03': {'start': date(2025, 12, 5), 'end': date(2025, 12, 25)},
            'bocco04': {'start': date(2025, 12, 5), 'end': date(2025, 12, 25)},
            'bocco05': {'start': date(2025, 12, 6), 'end': date(2025, 12, 26)},
        }
    },
    'ロボット好感群': {
        'users': ['bocco01', 'bocco02'],
        'periods': {
            'bocco01': {'start': date(2025, 12, 6), 'end': date(2025, 12, 26)},
            'bocco02': {'start': date(2025, 12, 5), 'end': date(2025, 12, 25)},
        }
    },
    'ロボット不信感群': {
        'users': ['bocco04', 'bocco05', 'bocco03'],
        'periods': {
            'bocco04': {'start': date(2025, 12, 5), 'end': date(2025, 12, 25)},
            'bocco05': {'start': date(2025, 12, 6), 'end': date(2025, 12, 26)},
            'bocco03': {'start': date(2025, 12, 5), 'end': date(2025, 12, 25)},
        }
    }
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


def calculate_group_response_rates(db, report_file):
    """各群の全体回答率を計算"""
    
    output = []
    output.append("\n各群の実験期間全体回答率")
    output.append("=" * 100)
    
    group_stats = []
    
    for group_name, group_data in GROUPS.items():
        output.append(f"\n【{group_name}】")
        output.append("-" * 100)
        output.append(f"{'ユーザーID':15s} {'実験期間':25s} {'日数':5s} {'総通知数':10s} {'入力数':10s} {'回答率':10s}")
        output.append("-" * 100)
        
        group_total_notifications = 0
        group_total_inputs = 0
        user_count = 0
        
        for user_id in group_data['users']:
            period = group_data['periods'][user_id]
            
            # 感情記録を取得
            df = fetch_emotion_records(db, user_id)
            
            if df.empty:
                output.append(f"{user_id:15s} {str(period['start']):15s}～{str(period['end']):9s} データなし")
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
            
            period_str = f"{period['start']}～{period['end']}"
            output.append(f"{user_id:15s} {period_str:25s} {days:5d}日 {total_notifications:10d}回 {input_count:10d}回 {response_rate:9.1f}%")
            
            group_total_notifications += total_notifications
            group_total_inputs += input_count
            user_count += 1
        
        # グループ全体の統計
        if user_count > 0:
            group_response_rate = (group_total_inputs / group_total_notifications * 100) if group_total_notifications > 0 else 0
            output.append("-" * 100)
            output.append(f"{'グループ平均':15s} {group_total_notifications:10d}回 {group_total_inputs:10d}回 {group_response_rate:9.1f}%")
            
            group_stats.append({
                'group_name': group_name,
                'user_count': user_count,
                'total_notifications': group_total_notifications,
                'total_inputs': group_total_inputs,
                'response_rate': group_response_rate
            })
    
    # コンソールとファイルの両方に出力
    for line in output:
        print(line)
        report_file.write(line + "\n")
    
    return pd.DataFrame(group_stats)


def plot_group_response_rates(group_stats_df, report_file):
    """各群の回答率を棒グラフで可視化"""
    
    if group_stats_df.empty:
        return
    
    # グラフを作成
    fig, ax = plt.subplots(figsize=(12, 6))
    
    # 棒グラフを描画
    groups = group_stats_df['group_name'].values
    rates = group_stats_df['response_rate'].values
    
    colors = ['#4A90E2', '#FF0000', '#FF6666', '#FFCCCC']
    bars = ax.bar(range(len(groups)), rates, color=colors, alpha=0.8)
    
    # X軸のラベルを設定
    ax.set_xticks(range(len(groups)))
    ax.set_xticklabels(groups, rotation=45, ha='right', fontsize=11)
    
    # 各バーの上に数値を表示
    for i, (group, rate) in enumerate(zip(groups, rates)):
        ax.text(i, rate, f'{rate:.1f}%', ha='center', va='bottom', fontsize=11, fontweight='bold')
    
    # グラフの装飾
    ax.set_ylabel('全体回答率 (%)', fontsize=12)
    ax.set_title('各群の実験期間全体回答率の比較', fontsize=14, fontweight='bold')
    ax.set_ylim(0, 100)
    ax.grid(axis='y', linestyle='--', alpha=0.3)
    
    # 平均値の線を引く
    avg_rate = rates.mean()
    ax.axhline(y=avg_rate, color='black', linestyle='--', linewidth=2, label=f'平均: {avg_rate:.1f}%')
    ax.legend()
    
    plt.tight_layout()
    
    # グラフを保存
    plt.savefig('group_response_rates.pdf', dpi=300, bbox_inches='tight', format='pdf')
    print("\nグラフを group_response_rates.pdf に保存しました")
    report_file.write("\nグラフを group_response_rates.pdf に保存しました\n")
    
    plt.close()


def main():
    """メイン処理"""
    report_file = open('group_response_rates_report.txt', 'w', encoding='utf-8')
    
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
        
        # 各群の回答率を計算
        print("\n各群の全体回答率を計算中...")
        group_stats_df = calculate_group_response_rates(db, report_file)
        
        # グラフを作成
        if not group_stats_df.empty:
            plot_group_response_rates(group_stats_df, report_file)
        
        # 分析完了メッセージ
        final_message = "\n分析完了: group_response_rates_report.txt に保存しました"
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
