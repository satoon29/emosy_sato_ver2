"""
ピーク値法による日次感情推定

このスクリプトは、1日の感情記録（Valence値）から、
正規化後の絶対値が最大のものを採用して感情を推定します。
"""

import pandas as pd
import numpy as np
from datetime import date
import firebase_admin
from firebase_admin import credentials, firestore


# ========================================
# 定数定義
# ========================================

# Valenceの定義範囲
VALENCE_MIN = 1.0  # 最小値
VALENCE_MAX = 9.0  # 最大値


# ========================================
# Firebase接続
# ========================================

def initialize_firebase():
    """Firebase接続を初期化"""
    if not firebase_admin._apps:
        try:
            # firebase_credentials.jsonのパスを指定
            cred = credentials.Certificate('firebase_credentials.json')
            firebase_admin.initialize_app(cred)
        except Exception as e:
            print(f"Firebaseの初期化に失敗しました: {e}")
            return None
    return firestore.client()


def fetch_emotion_data(db, user_id):
    """Firestoreから感情データを取得"""
    if db is None:
        return pd.DataFrame()

    query = db.collection("users").document(user_id).collection("emotions")
    docs = query.stream()
    
    records = []
    for doc in docs:
        record = doc.to_dict()
        records.append(record)

    if not records:
        return pd.DataFrame()
    
    df = pd.DataFrame(records)
    df['datetime'] = pd.to_datetime(
        df['day'] + ' ' + df['time'], 
        format='%Y/%m/%d %H:%M', 
        errors='coerce'
    )
    df.dropna(subset=['datetime', 'valence'], inplace=True)
    df['valence'] = pd.to_numeric(df['valence'], errors='coerce')
    df.dropna(subset=['valence'], inplace=True)
    
    return df


# ========================================
# 正規化関数
# ========================================

def normalize_valence(valence):
    """
    Valence値を[-1, 1]の範囲に線形正規化
    
    v_norm = 2 * ((v - v_min) / (v_max - v_min)) - 1
    
    Parameters:
    -----------
    valence : float
        元のValence値（1.0～9.0の範囲）
    
    Returns:
    --------
    float
        正規化されたValence値（-1.0～1.0の範囲）
    """
    v_norm = 2.0 * ((valence - VALENCE_MIN) / (VALENCE_MAX - VALENCE_MIN)) - 1.0
    return v_norm


# ========================================
# 感情分類
# ========================================

def classify_by_valence(valence):
    """Valence値から感情カテゴリ（3分類）を判定"""
    if valence <= 4.5:
        return 'Negative'
    elif valence <= 6.0:
        return 'Neutral'
    else:
        return 'Positive'


# ========================================
# ピーク値法アルゴリズム
# ========================================

def estimate_emotion_by_peak_value(day_df):
    """
    ピーク値法: 正規化したValence値の絶対値が最大のものを採用
    
    Parameters:
    -----------
    day_df : pd.DataFrame
        その日の感情記録データ（'valence'列を含む）
    
    Returns:
    --------
    str
        推定された感情カテゴリ ('Positive', 'Neutral', 'Negative')
    """
    if day_df.empty:
        return 'Neutral'
    
    # Valence値を[-1, 1]の範囲に正規化
    day_df = day_df.copy()
    day_df['valence_normalized'] = day_df['valence'].apply(normalize_valence)
    
    # 正規化後の絶対値が最大のインデックスを取得
    max_abs_idx = day_df['valence_normalized'].abs().idxmax()
    peak_valence_normalized = day_df.loc[max_abs_idx, 'valence_normalized']
    
    # ピーク値がニュートラル範囲内かチェック
    if -0.02 <= peak_valence_normalized <= 0.175:
        return 'Neutral'
    
    # ニュートラル範囲外の場合は正規化後の値の符号で判定
    if peak_valence_normalized > 0:
        return 'Positive'
    else:
        return 'Negative'


# ========================================
# 分析実行
# ========================================

def analyze_user_emotions(user_id):
    """
    指定されたユーザーの全感情データを日ごとに分析
    
    Parameters:
    -----------
    user_id : str
        ユーザーID
    
    Returns:
    --------
    pd.DataFrame
        日ごとの推定結果
    """
    db = initialize_firebase()
    if db is None:
        print("Firebase接続に失敗しました")
        return None
    
    df = fetch_emotion_data(db, user_id)
    
    if df.empty:
        print(f"ユーザー {user_id} のデータが見つかりません")
        return None
    
    # 日付列を追加
    df['date'] = df['datetime'].dt.date
    
    # 日付ごとにグループ化して分析
    results = []
    
    for target_date, day_df in df.groupby('date'):
        emotion = estimate_emotion_by_peak_value(day_df)
        
        results.append({
            'user_id': user_id,
            'date': target_date,
            'record_count': len(day_df),
            'estimated_emotion': emotion,
            'mean_valence': day_df['valence'].mean(),
            'std_valence': day_df['valence'].std() if len(day_df) > 1 else 0
        })
    
    return pd.DataFrame(results)


def estimate_single_day(user_id, target_date):
    """
    指定されたユーザーと日付の感情を推定
    
    Parameters:
    -----------
    user_id : str
        ユーザーID
    target_date : date
        推定対象の日付
    
    Returns:
    --------
    str
        推定された感情カテゴリ
    """
    db = initialize_firebase()
    if db is None:
        print("Firebase接続に失敗しました")
        return None
    
    df = fetch_emotion_data(db, user_id)
    
    if df.empty:
        print(f"ユーザー {user_id} のデータが見つかりません")
        return None
    
    # 指定された日付のデータを抽出
    df['date'] = df['datetime'].dt.date
    day_df = df[df['date'] == target_date]
    
    if day_df.empty:
        print(f"{target_date} のデータが見つかりません")
        return None
    
    # 正規化値を追加（デバッグ用）
    day_df_display = day_df.copy()
    day_df_display['valence_normalized'] = day_df_display['valence'].apply(normalize_valence)
    
    emotion = estimate_emotion_by_peak_value(day_df)
    
    # ピークとなったValence値を特定
    max_abs_idx = day_df_display['valence_normalized'].abs().idxmax()
    peak_valence_original = day_df_display.loc[max_abs_idx, 'valence']
    peak_valence_normalized = day_df_display.loc[max_abs_idx, 'valence_normalized']
    
    # ニュートラル範囲判定
    is_neutral_range = -0.02 <= peak_valence_normalized <= 0.175
    
    print(f"\n【ピーク値法による感情推定】")
    print(f"ユーザー: {user_id}")
    print(f"日付: {target_date}")
    print(f"記録数: {len(day_df)}件")
    print(f"推定感情: {emotion}")
    print(f"Valence平均: {day_df['valence'].mean():.2f}")
    print(f"ピーク値: {peak_valence_original:.2f} (正規化後: {peak_valence_normalized:.3f})")
    print(f"ニュートラル範囲内: {'はい' if is_neutral_range else 'いいえ'}")
    
    return emotion


# ========================================
# メイン処理
# ========================================

def main():
    """使用例"""
    # 例1: 単一日の推定
    print("=== 単一日の推定 ===")
    estimate_single_day(
        user_id='test00',
        target_date=date(2024, 10, 27)
    )
    
    print("\n" + "="*60 + "\n")
    
    # 例2: 全期間の分析
    print("=== 全期間の分析 ===")
    results = analyze_user_emotions('test00')
    
    if results is not None:
        print(f"\n分析結果: {len(results)}日分")
        print("\n最新5日分:")
        print(results.tail(5).to_string(index=False))
        
        # CSVに保存
        results.to_csv('peak_value_results.csv', index=False, encoding='utf-8-sig')
        print(f"\n結果を peak_value_results.csv に保存しました")
        
        # 感情の内訳を表示
        emotion_counts = results['estimated_emotion'].value_counts()
        print("\n感情の内訳:")
        for emotion, count in emotion_counts.items():
            percentage = count / len(results) * 100
            print(f"  {emotion}: {count}日 ({percentage:.1f}%)")


if __name__ == "__main__":
    main()
