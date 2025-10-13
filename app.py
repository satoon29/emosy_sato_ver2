import streamlit as st
import matplotlib.pyplot as plt
import matplotlib.font_manager as fm
from datetime import date
import os

# 各ファイルから必要なものをインポート
from config import JAPANESE_FONT_PATH
from data_handler import initialize_firebase, fetch_emotion_data 
from ui_components import (
    load_css,
    render_header,
    render_valence_timeseries,
    render_emoji_map,
    render_input_history,
)

def main():
    """アプリケーションのメイン実行関数"""
    load_css("style.css")

    if 'current_date' not in st.session_state:
        st.session_state.current_date = date.today()

    if os.path.exists(JAPANESE_FONT_PATH):
        fm.fontManager.addfont(JAPANESE_FONT_PATH)
        plt.rcParams['font.family'] = 'Noto Sans JP'
    else:
        st.caption(f"⚠️ 日本語フォントが見つかりません: {JAPANESE_FONT_PATH}")
    
    db = initialize_firebase()
    if db is None:
        st.stop()

    # ▼▼▼【変更点】st.tabsの代わりにst.radioを使い、処理を1回にする ▼▼▼
    
    # ラジオボタンで表示期間を選択（タブのように見せる）
    period_options = {"1日間": 1, "3日間": 3, "1週間": 7}
    selected_period = st.radio(
        "表示期間を選択",
        options=period_options.keys(),
        horizontal=True,
        label_visibility="collapsed" # "表示期間を選択"のラベルを非表示にする
    )

    # 共通の処理を担う関数を定義
    def display_dashboard(days: int):
        # 期間を指定してデータを取得
        df = fetch_emotion_data(db, st.session_state.current_date, days=days)
        
        # ヘッダーを表示
        render_header(df, st.session_state.current_date, days=days)

        if df.empty:
            st.markdown(f"<p style='font-size: 24px'>この期間の記録はありません。</p>", unsafe_allow_html=True)
            return

        # 各UIコンポーネントを描画
        render_valence_timeseries(df, st.session_state.current_date, days=days)
        st.divider()
        render_emoji_map(df, days=days)
        st.divider()
        render_input_history(df)

    # 選択された期間に応じてダッシュボードを一度だけ表示
    display_dashboard(days=period_options[selected_period])


if __name__ == "__main__":
    main()