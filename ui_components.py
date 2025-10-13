import streamlit as st
import pandas as pd
import numpy as np
import matplotlib.pyplot as plt
import matplotlib.dates as mdates
from matplotlib.offsetbox import OffsetImage, AnnotationBbox
import os
import folium
from streamlit_folium import st_folium
#import locale
from datetime import timedelta, datetime, time


# 設定値をconfig.pyからインポート
from config import EMOJI_IMAGE_FOLDER

# --- 3. UI表示用の関数 ---
def load_css(file_name):
    """外部CSSファイルを読み込んで適用する"""
    with open(file_name) as f:
        st.markdown(f'<style>{f.read()}</style>', unsafe_allow_html=True)

def format_date_jp(dt):
    weekdays_jp = ['月', '火', '水', '木', '金', '土', '日']
    weekday_str = weekdays_jp[dt.weekday()]
    return dt.strftime(f'%Y年%m月%d日 ({weekday_str})')

def render_header(df, current_date, days: int, user_id: str):
    """ヘッダー部分（タイトル、日付ナビゲーション、記録数）を表示する"""
    col1, col2, col3, col4 = st.columns([1.5, 1.5, 1, 6])
    with col1:
        # ▼▼▼【修正点】keyを追加 ▼▼▼
        if st.button("◀️ 前日", use_container_width=True, key=f"prev_day_{days}"):
            st.session_state.current_date -= timedelta(days=1)
            st.rerun()
    with col2:
        # ▼▼▼【修正点】keyを追加 ▼▼▼
        if st.button("翌日 ▶️", use_container_width=True, key=f"next_day_{days}"):
            st.session_state.current_date += timedelta(days=1)
            st.rerun()

    #locale.setlocale(locale.LC_TIME, 'ja_JP.UTF-8')

    if days == 1:
        title_date_str = format_date_jp(current_date)
    else:
        start_date = current_date - timedelta(days=days - 1)
        start_date_str = format_date_jp(start_date)
        end_date_str = format_date_jp(current_date)
        title_date_str = f"{start_date_str} 〜 {end_date_str}"
    
    st.markdown(f"<h1 class='main-title'>{title_date_str}</h1>", unsafe_allow_html=True)
    st.markdown(f"<p class='subtitle'>この期間に{len(df)}個の絵文字を記録しました！</p>", unsafe_allow_html=True)
    st.divider()


# ▼▼▼【変更点】引数daysを追加し、X軸の範囲を動的に ▼▼▼
def render_valence_timeseries(df, end_date, days: int):
    """感情価の時系列グラフを描画する"""
    st.subheader("感情の時間推移")
    fig, ax = plt.subplots(figsize=(12, 6))

    # --- X軸の範囲を動的に設定 ---
    if days == 1:
        target_date = end_date
        start_time = datetime.combine(target_date, time(9, 0))
        end_time = datetime.combine(target_date, time(19, 30))
        
        # データが空でない場合のみ、最終時刻をチェック
        if not df.empty:
            last_timestamp = df.index[-1]
            # 19時以降のデータがある場合、X軸の終端を延長
            if last_timestamp.time() > time(19, 0):
                end_time = last_timestamp + timedelta(minutes=30)
    else:
        start_date = end_date - timedelta(days=days - 1)
        start_time = datetime.combine(start_date, time.min)
        end_time = datetime.combine(end_date + timedelta(days=1), time.min)
        
    ax.set_xlim(start_time, end_time)

    # グラデーション背景
    gradient = np.linspace(0, 1, 256).reshape(-1, 1)
    ax.imshow(gradient, aspect='auto', cmap='coolwarm_r', alpha=0.3, 
              extent=(mdates.date2num(start_time), mdates.date2num(end_time), 2, 9))

    # 折れ線グラフ
    ax.plot(df.index, df['valence'], marker='o', linestyle='-', color='#F58E7D', label='感情価', zorder=10)

    # 絵文字プロット
    if os.path.isdir(EMOJI_IMAGE_FOLDER):
        for timestamp, row in df.iterrows():
            image_path = os.path.join(EMOJI_IMAGE_FOLDER, f"{row.get('name', '')}.png")
            if os.path.exists(image_path):
                img = plt.imread(image_path)
                imagebox = OffsetImage(img, zoom=0.05)
                ab = AnnotationBbox(imagebox, (timestamp, row['valence']), frameon=False, pad=0.1, zorder=11)
                ax.add_artist(ab)
    else:
        st.warning(f"絵文字画像フォルダ '{EMOJI_IMAGE_FOLDER}' が見つかりません。")

    ax.set_ylim(2, 9)
    ax.set_yticks([])
    ax.set_xlabel('時間', fontsize=20)
    ax.set_ylabel('感情価 (ネガティブ ↔ ポジティブ)', fontsize=20)
    
    # --- 表示期間に応じてX軸の目盛りとフォーマットを変更 ---
    if days == 1:
        ax.xaxis.set_major_formatter(mdates.DateFormatter('%H:%M'))
        ax.xaxis.set_major_locator(mdates.HourLocator(interval=1))
    elif days == 3:
        ax.xaxis.set_major_formatter(mdates.DateFormatter('%m/%d %H:%M'))
        ax.xaxis.set_major_locator(mdates.HourLocator(interval=6))
    else: # days == 7 or other
        ax.xaxis.set_major_formatter(mdates.DateFormatter('%m/%d'))
        ax.xaxis.set_major_locator(mdates.DayLocator(interval=1))
    
    plt.setp(ax.get_xticklabels(), fontsize=14, rotation=30, ha='right')
    plt.grid(True, which='both', linestyle='--', linewidth=0.5)
    plt.tight_layout()
    st.pyplot(fig)


def render_emoji_map(df, days: int):
    """絵文字入力のlat,lngを地図上に表示する"""
    st.subheader("絵文字入力の位置情報")
    if 'lat' in df.columns and 'lng' in df.columns and not df[['lat', 'lng']].isnull().all().all():
        # 絵文字プロットに必要な列を抽出
        map_df = df.dropna(subset=['lat', 'lng'])[['lat', 'lng', 'name']].copy()
        
        # latとlngを数値型に変換
        map_df['lat'] = pd.to_numeric(map_df['lat'], errors='coerce')
        map_df['lng'] = pd.to_numeric(map_df['lng'], errors='coerce')
        map_df.dropna(subset=['lat', 'lng'], inplace=True)

        # 緯度経度が(0, 0)のデータを除外
        map_df = map_df[(map_df['lat'] != 0) | (map_df['lng'] != 0)]

        if map_df.empty:
            st.markdown("<p style='font-size: 18px; color: #888;'>有効な位置情報がありません。</p>", unsafe_allow_html=True)
            return

        # 記録された位置情報の平均値を計算
        mean_lat = map_df['lat'].mean()
        mean_lng = map_df['lng'].mean()

        # foliumを使用して地図を作成 (初期位置を平均値に設定、タイルをグレースケールに変更)
        m = folium.Map(location=[mean_lat, mean_lng], zoom_start=15, tiles='CartoDB positron')

        # データポイントを絵文字アイコンとして追加
        for _, row in map_df.iterrows():
            emoji_name = row.get('name')
            if not emoji_name:
                continue
            
            icon_path = os.path.join(EMOJI_IMAGE_FOLDER, f"{emoji_name}.png")

            if os.path.exists(icon_path):
                icon = folium.features.CustomIcon(
                    icon_path,
                    icon_size=(30, 30) # アイコンサイズを調整
                )
                folium.Marker(
                    location=[row['lat'], row['lng']],
                    icon=icon
                ).add_to(m)

        # Streamlitに地図を表示（ユニークなキーを追加）
        st_folium(m, width=725, height=500, key=f"folium_map_{days}")
        
    else:
        st.markdown("<p style='font-size: 18px; color: #888;'>位置情報は記録されていません。</p>", unsafe_allow_html=True)


def render_input_history(df):
    """入力履歴をヘッダー付きのスクロール可能なリストで表示する"""
    st.subheader("入力履歴")
    history_df = df[['emoji']].copy()
    history_df['記録'] = history_df.index.strftime('%m/%d %H:%M')
    history_df.rename(columns={'emoji': '絵文字'}, inplace=True)
    header_html = "<div class='history-header'><span>絵文字</span><span>記録日時</span></div>"
    list_items_html = ""
    if history_df.empty:
        list_items_html = "<div class='no-history'>履歴はまだありません。</div>"
    else:
        for _, row in history_df.iterrows():
            list_items_html += f"<div class='history-item'><span>{row['絵文字']}</span><span class='history-time'>{row['記録']}</span></div>"
    full_html = f"<div class='history-wrapper'>{header_html}<div class='history-container'>{list_items_html}</div></div>"
    st.markdown(full_html, unsafe_allow_html=True)