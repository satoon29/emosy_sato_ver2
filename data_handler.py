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

