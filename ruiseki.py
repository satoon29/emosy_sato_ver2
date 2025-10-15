import streamlit as st
import pandas as pd
import numpy as np
import matplotlib.pyplot as plt
import matplotlib.dates as mdates
import matplotlib.font_manager as fm
from datetime import datetime, time
import firebase_admin
from firebase_admin import credentials, firestore

# --- 1. 設定項目 ---
# 既存のファイルから必要な設定を流用
from config import USER_ID, JAPANESE_FONT_PATH

# --- 2. データ関連の関数 ---

@st.cache_resource
def initialize_firebase():
    """Firebaseへの接続を初期化し、クライアントを返す"""
    if not firebase_admin._apps:
        try:
            cred_dict = dict(st.secrets["firebase_credentials"])
            cred = credentials.Certificate(cred_dict)
            firebase_admin.initialize_app(cred)
        except Exception as e:
            st.error(f"Firebaseの初期化に失敗しました: {e}")
            return None
    return firestore.client()

@st.cache_data(ttl=600)
def fetch_all_emotion_data(_db_client, user_id: str):
    """全期間の感情データをFirestoreから取得する"""
    if _db_client is None:
        return pd.DataFrame()

    query = _db_client.collection("users").document(user_id).collection("emotions")
    docs = query.stream()
    
    records = [doc.to_dict() for doc in docs]
    if not records:
        return pd.DataFrame()
    
    df = pd.DataFrame(records)
    df['datetime'] = pd.to_datetime(df['day'] + ' ' + df['time'], format='%Y/%m/%d %H:%M', errors='coerce')
    df.dropna(subset=['datetime', 'valence'], inplace=True)
    df['valence'] = pd.to_numeric(df['valence'])
    return df

def assign_cluster(valence):
    """Valence値に基づいてクラスタを割り当てる"""
    if valence <= 3.5:
        return '強いネガティブ'
    elif valence <= 4.5:
        return '弱いネガティブ'
    elif valence <= 5.2:
        return 'ネガティブ寄り中立'
    elif valence <= 6.0:
        return 'ポジティブ寄り中立'
    elif valence <= 7.6:
        return '弱いポジティブ'
    else:
        return '強いポジティブ'

def process_for_cumulative_chart(df):
    """累積グラフ用にデータを処理する"""
    if df.empty:
        return pd.DataFrame()

    df['cluster'] = df['valence'].apply(assign_cluster)
    df['time_of_day'] = df['datetime'].dt.time
    df = df.sort_values('time_of_day')

    clusters = [
        '強いポジティブ', '弱いポジティブ', 'ポジティブ寄り中立',
        'ネガティブ寄り中立', '弱いネガティブ', '強いネガティブ'
    ]
    
    # 各時点での累積割合を計算
    cumulative_counts = pd.DataFrame(0, index=df.index, columns=clusters)
    for i, (idx, row) in enumerate(df.iterrows()):
        if i > 0:
            cumulative_counts.iloc[i] = cumulative_counts.iloc[i-1]
        cumulative_counts.loc[idx, row['cluster']] += 1
        
    cumulative_percentage = cumulative_counts.div(cumulative_counts.sum(axis=1), axis=0) * 100
    
    # グラフ描画用に時刻をdatetimeオブジェクトに変換
    today = datetime.now().date()
    cumulative_percentage['plot_time'] = [datetime.combine(today, t) for t in df['time_of_day']]
    
    return cumulative_percentage.set_index('plot_time')


# --- 3. UI表示用の関数 ---

def render_cumulative_chart(df):
    """感情クラスタの累積割合グラフを描画する"""
    st.subheader("感情クラスタの1日累積割合")
    
    if df.empty:
        st.warning("表示するデータがありません。")
        return

    fig, ax = plt.subplots(figsize=(12, 7))

    clusters = [
        '強いポジティブ', '弱いポジティブ', 'ポジティブ寄り中立',
        'ネガティブ寄り中立', '弱いネガティブ', '強いネガティブ'
    ]
    colors = ['#ff9999', '#ffc000', '#ffff00', '#ccffcc', '#99ccff', '#c4a3d5']
    
    # データをプロット
    x = df.index
    # Y軸は各クラスタの割合を累積させたもの
    y = [df[c] for c in clusters]
    
    ax.stackplot(x, y, labels=clusters, colors=colors, alpha=0.8)

    # グラフの書式設定
    ax.set_xlim(datetime.combine(x.min().date(), time.min), datetime.combine(x.min().date(), time.max))
    ax.set_ylim(0, 100)
    ax.set_ylabel('累積割合 (%)', fontsize=16)
    ax.set_xlabel('時刻', fontsize=16)
    ax.xaxis.set_major_formatter(mdates.DateFormatter('%H:%M'))
    ax.xaxis.set_major_locator(mdates.HourLocator(interval=2))
    plt.setp(ax.get_xticklabels(), fontsize=12, rotation=30, ha='right')
    
    ax.legend(loc='upper left', bbox_to_anchor=(1, 1))
    plt.grid(True, which='both', linestyle='--', linewidth=0.5)
    plt.tight_layout(rect=[0, 0, 0.85, 1]) # 凡例が収まるように調整
    
    st.pyplot(fig)

# --- 4. メイン処理 ---

def main():
    """アプリケーションのメイン実行関数"""
    st.title("感情の累積割合分析")

    # 日本語フォントの設定
    if fm.fontManager.findfont('Noto Sans JP', fallback_to_default=False):
        plt.rcParams['font.family'] = 'Noto Sans JP'
    elif os.path.exists(JAPANESE_FONT_PATH):
        fm.fontManager.addfont(JAPANESE_FONT_PATH)
        plt.rcParams['font.family'] = 'Noto Sans JP'
    else:
        st.caption(f"⚠️ 日本語フォントが見つかりません: {JAPANESE_FONT_PATH}")

    db = initialize_firebase()
    if db is None:
        st.stop()

    # データを取得・処理
    all_data = fetch_all_emotion_data(db, USER_ID)
    cumulative_df = process_for_cumulative_chart(all_data)

    # グラフを描画
    render_cumulative_chart(cumulative_df)

if __name__ == "__main__":
    main()
