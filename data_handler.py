import streamlit as st
import pandas as pd
from datetime import timedelta
import firebase_admin
from firebase_admin import credentials, firestore
from google.cloud.firestore_v1.base_query import FieldFilter

# 設定値をconfig.pyからインポート
#from config import FIREBASE_CREDENTIALS_PATH

@st.cache_resource
def initialize_firebase():
    """Firebaseへの接続を初期化し、クライアントを返す"""
    if not firebase_admin._apps:
        try:
            cred_dict = dict(st.secrets["firebase_credentials"])
            cred = credentials.Certificate(cred_dict)
            #cred = credentials.Certificate(FIREBASE_CREDENTIALS_PATH)
            firebase_admin.initialize_app(cred)
        except Exception as e:
            st.error(f"Firebaseの初期化に失敗しました: {e}")
            #st.error(f"'{FIREBASE_CREDENTIALS_PATH}' のパスが正しいか確認してください。")
            st.write(st.secrets.to_dict())
            return None
    return firestore.client()

@st.cache_data(ttl=600)
# ▼▼▼【変更点】user_idを引数で受け取るように修正 ▼▼▼
def fetch_emotion_data(_db_client, end_date, days: int, user_id: str):
    """指定された終了日から過去N日分のデータをFirestoreから取得する"""
    if _db_client is None:
        return pd.DataFrame()

    # ▼▼▼【変更点】指定された日数分のリストを生成 ▼▼▼
    dates_to_fetch = [(end_date - timedelta(days=i)).strftime("%Y/%m/%d") for i in range(days)]
    
    query = _db_client.collection("users").document(user_id).collection("emotions").where(filter=FieldFilter("day", "in", dates_to_fetch))
    docs = query.stream()
    
    records = [doc.to_dict() for doc in docs]
    if not records:
        return pd.DataFrame()
    
    df = pd.DataFrame(records)
    df['datetime'] = pd.to_datetime(df['day'] + ' ' + df['time'], format='%Y/%m/%d %H:%M', errors='coerce')
    df.dropna(subset=['datetime'], inplace=True)
    df.set_index('datetime', inplace=True)
    df = df.between_time('09:00', '22:00')
    df.sort_index(inplace=True)
    return df

@st.cache_data(ttl=600)
def fetch_all_emotion_data(_db_client, user_id: str):
    """全期間の感情データをFirestoreから取得する"""
    if _db_client is None:
        return pd.DataFrame()

    query = _db_client.collection("users").document(user_id).collection("emotions")
    docs = query.stream()
    
    records = []
    for doc in docs:
        record = doc.to_dict()
        record['doc_id'] = doc.id
        records.append(record)

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

def process_for_pie_chart(df):
    """円グラフ用にクラスタの構成比率を計算する"""
    if df.empty:
        return pd.Series(dtype=float)

    # 'cluster'列がなければ作成
    if 'cluster' not in df.columns:
        df['cluster'] = df['valence'].apply(assign_cluster)

    # 各クラスタの出現回数を計算
    cluster_counts = df['cluster'].value_counts()
    
    # 全体に対する割合（%）を計算
    cluster_percentage = (cluster_counts / cluster_counts.sum()) * 100
    
    # 全てのクラスタがデータに含まれるように整形
    clusters = [
        '強いネガティブ', '弱いネガティブ', 'ネガティブ寄り中立',
        'ポジティブ寄り中立', '弱いポジティブ', '強いポジティブ'
    ]
    cluster_percentage = cluster_percentage.reindex(clusters, fill_value=0)
    
    return cluster_percentage

def process_for_cumulative_chart(df):
    """【修正】時間帯ごとのクラスタ構成比を計算する"""
    if df.empty:
        return pd.DataFrame()

    df['cluster'] = df['valence'].apply(assign_cluster)
    df['hour'] = df['datetime'].dt.hour

    clusters = [
        '強いネガティブ', '弱いネガティブ', 'ネガティブ寄り中立',
        'ポジティブ寄り中立', '弱いポジティブ', '強いポジティブ'
    ]
    
    # 時間帯ごとに各クラスタの出現回数を集計
    hourly_counts = pd.crosstab(df['hour'], df['cluster'])
    
    # 全てのクラスタ列が存在するように整形
    hourly_counts = hourly_counts.reindex(columns=clusters, fill_value=0)
    
    # 各時間帯（各行）の合計が100%になるように構成比を計算
    hourly_percentage = hourly_counts.div(hourly_counts.sum(axis=1), axis=0).fillna(0) * 100
    
    # 9時から19時までのインデックスを作成し、データのない時間帯も0で埋める
    all_hours_index = pd.Index(range(9, 20), name='hour')
    hourly_percentage = hourly_percentage.reindex(all_hours_index, fill_value=0)
    
    return hourly_percentage

def process_for_heatmap(df):
    """ヒートマップ用にポジティブとネガティブのデータをそれぞれ処理する"""
    if df.empty or 'lat' not in df.columns or 'lng' not in df.columns:
        return [], []
    
    heatmap_df = df[['lat', 'lng', 'valence']].dropna().copy()
    
    heatmap_df['lat'] = pd.to_numeric(heatmap_df['lat'], errors='coerce')
    heatmap_df['lng'] = pd.to_numeric(heatmap_df['lng'], errors='coerce')
    heatmap_df.dropna(subset=['lat', 'lng'], inplace=True)
    heatmap_df = heatmap_df[(heatmap_df['lat'] != 0) | (heatmap_df['lng'] != 0)]

    if heatmap_df.empty:
        return [], []

    # ポジティブとネガティブにデータを分割
    positive_df = heatmap_df[heatmap_df['valence'] > 6.5]
    negative_df = heatmap_df[heatmap_df['valence'] <= 4.0]

    # [緯度, 経度] のリストを作成
    positive_data = positive_df[['lat', 'lng']].values.tolist()
    negative_data = negative_df[['lat', 'lng']].values.tolist()
    
    return positive_data, negative_data

