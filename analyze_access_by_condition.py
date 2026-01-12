import pandas as pd
from datetime import datetime, date, timedelta, timezone
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
    'スマートフォン通知条件': ['user21', 'user22', 'user23', 'User24', 'user25'],
    'ロボット共感条件': ['bocco01', 'bocco02', 'bocco03', 'bocco04', 'bocco05'],
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


def convert_to_aware_datetime(dt):
    """ナイーブなdatetimeをUTC aware datetimeに変換"""
    if dt.tzinfo is None:
        return dt.replace(tzinfo=timezone.utc)
    return dt


def fetch_page_views_by_user(db, user_id):
    """特定ユーザーのpage_viewsを取得"""
    try:
        query = db.collection('users').document(user_id).collection('page_views')
        docs = query.stream()
        
        records = []
        for doc in docs:
            record = doc.to_dict()
            if 'start_time' in record and record['start_time'] is not None:
                record['user_id'] = user_id
                records.append(record)
        
        return records
    except Exception as e:
        print(f"page_views取得失敗 ({user_id}): {e}")
        return []


def classify_condition(user_id):
    """ユーザーIDから群を取得"""
    for condition, users in CONDITIONS.items():
        if user_id in users:
            return condition
    return None


def calculate_user_total_access_from_page_views(db, report_file):
    """ユーザーごとの総アクセス回数を計算（page_viewsから取得、実験期間内のみ）"""
    
    output = []
    output.append("\n実験期間におけるユーザーごとの総アクセス回数（page_viewsから）")
    output.append("=" * 100)
    
    # 条件ごとに集計
    for condition in ['スマートフォン通知条件', 'ロボット共感条件']:
        output.append(f"\n【{condition}】")
        output.append("-" * 100)
        output.append(f"{'ユーザーID':15s} {'実験期間':25s} {'総ページビュー数':15s}")
        output.append("-" * 100)
        
        condition_users = CONDITIONS[condition]
        total_all = 0
        
        for user_id in condition_users:
            if user_id not in EXPERIMENT_PERIODS:
                continue
            
            period = EXPERIMENT_PERIODS[user_id]
            
            # page_viewsを取得
            page_views = fetch_page_views_by_user(db, user_id)
            
            # 実験期間内のpage_viewsをカウント
            start_dt = convert_to_aware_datetime(datetime.combine(period['start'], datetime.min.time()))
            end_dt = convert_to_aware_datetime(datetime.combine(period['end'], datetime.max.time()))
            
            in_period_count = 0
            for pv in page_views:
                ts = pv.get('start_time')
                if ts:
                    if hasattr(ts, 'datetime'):
                        ts_dt = ts.datetime()
                    else:
                        ts_dt = pd.Timestamp(ts).to_pydatetime()
                    
                    if ts_dt.tzinfo is None:
                        ts_dt = ts_dt.replace(tzinfo=timezone.utc)
                    
                    if start_dt <= ts_dt <= end_dt:
                        in_period_count += 1
            
            period_str = f"{period['start']}～{period['end']}"
            output.append(f"{user_id:15s} {period_str:25s} {in_period_count:15d}回")
            total_all += in_period_count
        
        output.append("-" * 100)
        output.append(f"{'合計':15s} {' ':25s} {total_all:15d}回")
    
    # コンソールとファイルの両方に出力
    for line in output:
        print(line)
        report_file.write(line + "\n")


def calculate_daily_access_by_condition_from_page_views(db, report_file):
    """群別の日別アクセス数を計算（page_viewsから取得）"""
    
    output = []
    output.append("\n群別の日別アクセス数（page_viewsから、実験期間内のみ）")
    output.append("=" * 60)
    
    # 全ユーザーのpage_viewsを集計
    daily_data = {}  # {'date': {'condition': count}}
    
    for user_id, period in EXPERIMENT_PERIODS.items():
        condition = classify_condition(user_id)
        if not condition:
            continue
        
        # page_viewsを取得
        page_views = fetch_page_views_by_user(db, user_id)
        
        # 実験期間内のpage_viewsを日別に集計
        start_dt = convert_to_aware_datetime(datetime.combine(period['start'], datetime.min.time()))
        end_dt = convert_to_aware_datetime(datetime.combine(period['end'], datetime.max.time()))
        
        for pv in page_views:
            ts = pv.get('start_time')
            if ts:
                if hasattr(ts, 'datetime'):
                    ts_dt = ts.datetime()
                else:
                    ts_dt = pd.Timestamp(ts).to_pydatetime()
                
                if ts_dt.tzinfo is None:
                    ts_dt = ts_dt.replace(tzinfo=timezone.utc)
                
                if start_dt <= ts_dt <= end_dt:
                    date_key = ts_dt.date()
                    if date_key not in daily_data:
                        daily_data[date_key] = {}
                    if condition not in daily_data[date_key]:
                        daily_data[date_key][condition] = 0
                    daily_data[date_key][condition] += 1
    
    # 結果を出力
    for condition in ['スマートフォン通知条件', 'ロボット共感条件']:
        output.append(f"\n【{condition}】")
        output.append("-" * 60)
        output.append(f"{'日付':15s} {'アクセス数':15s}")
        output.append("-" * 60)
        
        total_access = 0
        for date_key in sorted(daily_data.keys()):
            count = daily_data[date_key].get(condition, 0)
            output.append(f"{str(date_key):15s} {count:15d}回")
            total_access += count
        
        output.append("-" * 60)
        output.append(f"{'合計':15s} {total_access:15d}回")
    
    # コンソールとファイルの両方に出力
    for line in output:
        print(line)
        report_file.write(line + "\n")
    
    return daily_data


def plot_daily_access_transition(daily_data, report_file):
    """群別の日別アクセス数推移を折れ線グラフで表示（横軸：経過日数、最大21日）"""
    
    if not daily_data:
        print("グラフを描画するデータがありません")
        return
    
    # グラフを作成
    fig, ax = plt.subplots(figsize=(14, 7))
    
    # 日付でソート
    dates = sorted(daily_data.keys())
    
    # 経過日数を計算するための基準日を取得
    min_date = min(dates)
    
    # 経過日数と各条件のカウントを集計（21日以内のみ）
    elapsed_days_list = []
    app_counts = []
    robot_counts = []
    
    for date_val in dates:
        # 経過日数を計算（基準日を1日目とする）
        elapsed_days = (date_val - min_date).days + 1
        
        # 21日以内のデータのみを対象
        if elapsed_days <= 21:
            elapsed_days_list.append(elapsed_days)
            
            # アクセス数を取得
            app_counts.append(daily_data.get(date_val, {}).get('スマートフォン通知条件', 0))
            robot_counts.append(daily_data.get(date_val, {}).get('ロボット共感条件', 0))
    
    if not elapsed_days_list:
        print("21日以内のデータがありません")
        return
    
    # 折れ線グラフを描画
    ax.plot(elapsed_days_list, app_counts, marker='o', label='スマートフォン通知条件', 
            linewidth=2.5, color='#4A90E2', markersize=6, alpha=0.8)
    ax.plot(elapsed_days_list, robot_counts, marker='s', label='ロボット共感条件', 
            linewidth=2.5, color='#FF0000', markersize=6, alpha=0.8)
    
    # 全体平均を計算
    overall_avg = [(app + robot) / 2 for app, robot in zip(app_counts, robot_counts)]
    
    # 全体平均を黒い点線で描画
    ax.plot(elapsed_days_list, overall_avg, marker='D', label='全体平均', 
            linewidth=2.5, color='#000000', markersize=6, alpha=0.7, linestyle='--')
    
    # グラフの装飾
    ax.set_xlabel('実験開始からの経過日数', fontsize=12)
    ax.set_ylabel('アクセス数', fontsize=12)
    ax.legend(fontsize=11, loc='upper left')
    ax.grid(True, linestyle='--', alpha=0.3)
    
    # 縦軸を整数に設定
    ax.yaxis.set_major_locator(plt.MaxNLocator(integer=True))
    
    # X軸の範囲と目盛りを固定（1～21日）
    ax.set_xlim(0, 22)
    ax.set_xticks(range(1, 22, 2))
    ax.set_xticklabels([str(int(x)) for x in range(1, 22, 2)])
    
    plt.tight_layout()
    
    # グラフを保存
    plt.savefig('daily_access_transition.pdf', dpi=300, bbox_inches='tight', format='pdf')
    print("\nグラフを daily_access_transition.pdf に保存しました")
    report_file.write("\nグラフを daily_access_transition.pdf に保存しました\n")
    
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
        
        # page_viewsから集計
        print("\nユーザーごとの総アクセス回数を計算中...")
        calculate_user_total_access_from_page_views(db, report_file)
        
        print("日別アクセス数を計算中...")
        daily_data = calculate_daily_access_by_condition_from_page_views(db, report_file)
        
        # 折れ線グラフを生成
        print("折れ線グラフを生成中...")
        plot_daily_access_transition(daily_data, report_file)
        
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
