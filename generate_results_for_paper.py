import pandas as pd
from datetime import date, datetime
import firebase_admin
from firebase_admin import credentials, firestore
import os

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
        'users': ['bocco01', 'bocco02', 'bocco03'],
        'periods': {
            'bocco01': {'start': date(2025, 12, 6), 'end': date(2025, 12, 26)},
            'bocco02': {'start': date(2025, 12, 5), 'end': date(2025, 12, 25)},
            'bocco03': {'start': date(2025, 12, 5), 'end': date(2025, 12, 25)},
        }
    },
    'ロボット不信感群': {
        'users': ['bocco04', 'bocco05'],
        'periods': {
            'bocco04': {'start': date(2025, 12, 5), 'end': date(2025, 12, 25)},
            'bocco05': {'start': date(2025, 12, 6), 'end': date(2025, 12, 26)},
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


def fetch_page_views(db, user_id):
    """特定ユーザーのページビューログをFirestoreから取得"""
    try:
        query = db.collection('users').document(user_id).collection('page_views')
        docs = query.stream()
        
        records = []
        for doc in docs:
            record = doc.to_dict()
            if 'start_time' not in record:
                continue
            records.append(record)
        
        if not records:
            return pd.DataFrame()
        
        df = pd.DataFrame(records)
        df['datetime'] = pd.to_datetime(df['start_time'], errors='coerce')
        df.dropna(subset=['datetime'], inplace=True)
        
        return df
        
    except Exception as e:
        print(f"ページビューの取得に失敗 ({user_id}): {e}")
        return pd.DataFrame()


def calculate_group_statistics(db):
    """各群の統計情報を計算"""
    
    results = {}
    
    for group_name, group_data in GROUPS.items():
        group_emotion_count = 0
        group_total_notifications = 0
        group_view_count = 0
        group_view_days = 0
        
        user_details = []
        total_group_days = 0
        
        for user_id in group_data['users']:
            period = group_data['periods'][user_id]
            
            # 感情記録を取得
            df_emotion = fetch_emotion_records(db, user_id)
            
            # ページビューログを取得
            df_pageview = fetch_page_views(db, user_id)
            
            # 実験期間内のデータをフィルタ
            start_datetime = datetime.combine(period['start'], datetime.min.time())
            end_datetime = datetime.combine(period['end'], datetime.max.time())
            
            # UTCタイムゾーン対応
            start_datetime_utc = pd.Timestamp(start_datetime, tz='UTC')
            end_datetime_utc = pd.Timestamp(end_datetime, tz='UTC')
            
            days = (period['end'] - period['start']).days + 1
            total_notifications = days * NOTIFICATIONS_PER_DAY
            total_group_days += days
            
            # 感情入力数
            if not df_emotion.empty:
                df_emotion_period = df_emotion[(df_emotion['datetime'] >= start_datetime) & 
                                               (df_emotion['datetime'] <= end_datetime)]
                emotion_count = len(df_emotion_period)
            else:
                emotion_count = 0
            
            # ページビュー数と閲覧日数
            if not df_pageview.empty:
                df_pageview_period = df_pageview[(df_pageview['datetime'] >= start_datetime_utc) & 
                                                 (df_pageview['datetime'] <= end_datetime_utc)]
                pageview_count = len(df_pageview_period)
                view_dates = df_pageview_period['datetime'].dt.date.unique()
                view_days = len(view_dates)
            else:
                pageview_count = 0
                view_days = 0
            
            emotion_rate = (emotion_count / total_notifications * 100) if total_notifications > 0 else 0
            view_rate = (view_days / days * 100) if days > 0 else 0
            
            group_emotion_count += emotion_count
            group_total_notifications += total_notifications
            group_view_count += pageview_count
            group_view_days += view_days
            
            user_details.append({
                'user_id': user_id,
                'emotion_count': emotion_count,
                'total_notifications': total_notifications,
                'emotion_rate': emotion_rate,
                'pageview_count': pageview_count,
                'view_days': view_days,
                'days': days,
                'view_rate': view_rate
            })
        
        # グループ全体の統計
        group_emotion_rate = (group_emotion_count / group_total_notifications * 100) if group_total_notifications > 0 else 0
        group_view_rate = (group_view_days / total_group_days * 100) if total_group_days > 0 else 0
        
        results[group_name] = {
            'user_count': len(group_data['users']),
            'emotion_count': group_emotion_count,
            'total_notifications': group_total_notifications,
            'emotion_rate': group_emotion_rate,
            'pageview_count': group_view_count,
            'view_days': group_view_days,
            'total_days': total_group_days,
            'view_rate': group_view_rate,
            'user_details': user_details
        }
    
    return results


def write_results(results):
    """結果をresult.txtに記述"""
    
    with open('result.txt', 'w', encoding='utf-8') as f:
        f.write("=" * 100 + "\n")
        f.write("実験結果報告書\n")
        f.write("=" * 100 + "\n\n")
        
        f.write("1. 実験概要\n")
        f.write("-" * 100 + "\n")
        f.write("本実験は、スマートフォン通知とロボット共感機能が感情入力行動に及ぼす影響を調査した。\n")
        f.write("実験期間：2025年12月4日～2025年12月26日\n")
        f.write("通知頻度：1日20回\n")
        f.write("被験者数：スマートフォン通知群5名、ロボット共感群5名\n\n")
        
        f.write("2. 結果\n")
        f.write("=" * 100 + "\n\n")
        
        # 表形式で群別の結果を表示
        f.write("2.1 感情入力率（感情記録数 / 総通知数）\n")
        f.write("-" * 100 + "\n")
        f.write(f"{'群名':25s} {'被験者数':10s} {'総入力数':10s} {'総通知数':10s} {'入力率':10s}\n")
        f.write("-" * 100 + "\n")
        
        for group_name, result in results.items():
            f.write(f"{group_name:25s} {result['user_count']:10d} {result['emotion_count']:10d} {result['total_notifications']:10d} {result['emotion_rate']:9.1f}%\n")
        
        f.write("\n2.2 フィードバック閲覧率（1日1回以上閲覧した日数 / 実験期間日数）\n")
        f.write("-" * 100 + "\n")
        f.write(f"{'群名':25s} {'被験者数':10s} {'閲覧日数':10s} {'総日数':10s} {'閲覧率':10s}\n")
        f.write("-" * 100 + "\n")
        
        for group_name, result in results.items():
            f.write(f"{group_name:25s} {result['user_count']:10d} {result['view_days']:10d} {result['total_days']:10d} {result['view_rate']:9.1f}%\n")
        
        # 群別の詳細結果
        f.write("\n\n3. 被験者別詳細結果\n")
        f.write("=" * 100 + "\n\n")
        
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
    
        for group_name, result in results.items():
            f.write(f"\n【{group_name}】\n")
            f.write("-" * 100 + "\n")
            f.write(f"{'ユーザーID':15s} {'感情入力':10s} {'入力率':10s} {'ページビュー':10s} {'閲覧日数':10s} {'閲覧率':10s}\n")
            f.write("-" * 100 + "\n")
            
            for user_detail in result['user_details']:
                # ユーザー名をマッピング
                display_name = USER_NAME_MAPPING.get(user_detail['user_id'], user_detail['user_id'])
                f.write(f"{display_name:15s} {user_detail['emotion_count']:10d} {user_detail['emotion_rate']:9.1f}% {user_detail['pageview_count']:10d} {user_detail['view_days']:10d} {user_detail['view_rate']:9.1f}%\n")
        
        f.write("\n\n4. 考察\n")
        f.write("=" * 100 + "\n")
        f.write("本実験の結果から、以下のことが明らかになった：\n\n")
        
        # スマートフォン通知群 vs ロボット共感群の比較
        sp_result = results['スマートフォン通知群']
        robot_result = results['ロボット共感群']
        
        f.write("4.1 感情入力率の比較\n")
        f.write("-" * 100 + "\n")
        emotion_diff = sp_result['emotion_rate'] - robot_result['emotion_rate']
        f.write(f"スマートフォン通知群：{sp_result['emotion_rate']:.1f}%\n")
        f.write(f"ロボット共感群：{robot_result['emotion_rate']:.1f}%\n")
        f.write(f"差分：{emotion_diff:+.1f}% ({'スマートフォン通知群が高い' if emotion_diff > 0 else 'ロボット共感群が高い'})\n\n")
        
        f.write("4.2 フィードバック閲覧率の比較\n")
        f.write("-" * 100 + "\n")
        view_diff = sp_result['view_rate'] - robot_result['view_rate']
        f.write(f"スマートフォン通知群：{sp_result['view_rate']:.1f}%\n")
        f.write(f"ロボット共感群：{robot_result['view_rate']:.1f}%\n")
        f.write(f"差分：{view_diff:+.1f}% ({'スマートフォン通知群が高い' if view_diff > 0 else 'ロボット共感群が高い'})\n\n")
        
        # ロボット好感群と不信感群の比較
        if 'ロボット好感群' in results and 'ロボット不信感群' in results:
            positive_result = results['ロボット好感群']
            negative_result = results['ロボット不信感群']
            
            f.write("4.3 ロボット印象による違い\n")
            f.write("-" * 100 + "\n")
            f.write(f"ロボット好感群（bocco01-03）\n")
            f.write(f"  感情入力率：{positive_result['emotion_rate']:.1f}%\n")
            f.write(f"  閲覧率：{positive_result['view_rate']:.1f}%\n\n")
            f.write(f"ロボット不信感群（bocco04-05）\n")
            f.write(f"  感情入力率：{negative_result['emotion_rate']:.1f}%\n")
            f.write(f"  閲覧率：{negative_result['view_rate']:.1f}%\n\n")
        
        f.write("\n5. 結論\n")
        f.write("=" * 100 + "\n")
        f.write("本実験により、通知方法とロボット共感機能が感情入力行動に異なる影響を与えることが示唆された。\n")
        f.write("今後、より詳細な分析と継続研究が必要である。\n")


def main():
    """メイン処理"""
    
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
            print("Firebase接続に失敗しました")
            return
        
        print("Firestoreクライアント接続完了")
        
        # 統計情報を計算
        print("\n統計情報を計算中...")
        results = calculate_group_statistics(db)
        
        # 結果をファイルに記述
        print("結果をresult.txtに記述中...")
        write_results(results)
        
        print("\n完了: result.txt に結果を保存しました")
        
    except Exception as e:
        print(f"\nエラーが発生しました: {str(e)}")
        import traceback
        traceback.print_exc()


if __name__ == "__main__":
    main()
