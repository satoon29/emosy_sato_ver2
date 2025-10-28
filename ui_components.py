import streamlit as st
import pandas as pd
import numpy as np
import matplotlib.pyplot as plt
import matplotlib.dates as mdates
from matplotlib.offsetbox import OffsetImage, AnnotationBbox
import os
import folium
from folium.plugins import HeatMap
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

    # --- ▼▼▼【変更点】ラグランジュ補間による曲線描画 ▼▼▼ ---
    # 元のデータ点をマーカーとしてプロット
    ax.plot(df.index, df['valence'], marker='o', linestyle='-', color='#F58E7D', zorder=10)
    

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
    ax.set_ylabel('ネガティブ ↔ ポジティブ', fontsize=20)
    
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


def render_emotion_map(df):
    """感情の地図（ブラー付きの円と絵文字アイコン）を表示する"""
    st.subheader("感情の地図")

    if 'lat' not in df.columns or 'lng' not in df.columns or df[['lat', 'lng']].isnull().all().all():
        st.info("この期間の位置情報付きの記録はありません。")
        return

    # foliumを使用して地図を作成
    m = folium.Map(
        location=[34.80914072819409, 135.5609309911741], 
        zoom_start=12, 
        tiles='CartoDB positron',
        attr='CartoDB Positron'
    )

    # --- ▼▼▼【修正点】クラスタの強さに応じて不透明度を定義 ▼▼▼ ---
    # ベースとなる色（RGB値）
    positive_rgb = "255, 140, 148" # コーラル
    negative_rgb = "139, 157, 195" # 落ち着いた青

    # クラスタごとの不透明度を指定された値に変更
    opacity_map = {
        '強いネガティブ': 0.7,
        '弱いネガティブ': 0.5,
        'ネガティブ寄り中立': 0.3,
        'ポジティブ寄り中立': 0.3,
        '弱いポジティブ': 0.5,
        '強いポジティブ': 0.7
    }
    
    # 必要なデータを準備
    map_df = df.dropna(subset=['lat', 'lng', 'cluster', 'name']).copy()
    map_df['lat'] = pd.to_numeric(map_df['lat'], errors='coerce')
    map_df['lng'] = pd.to_numeric(map_df['lng'], errors='coerce')
    map_df.dropna(subset=['lat', 'lng'], inplace=True)
    map_df = map_df[(map_df['lat'] != 0) | (map_df['lng'] != 0)]

    if map_df.empty:
        st.info("この期間の位置情報付きの記録はありません。")
        return

    for _, row in map_df.iterrows():
        cluster = row['cluster']
        opacity = opacity_map.get(cluster, 0.1) # 不明なクラスタは薄く表示

        # クラスタに応じてベース色を選択
        if 'ポジティブ' in cluster:
            base_rgb = positive_rgb
        elif 'ネガティブ' in cluster:
            base_rgb = negative_rgb
        else:
            base_rgb = "204, 204, 204" # 中立はグレー
        
        # --- ▼▼▼【修正点】HeatMapの正しい使い方に修正 ▼▼▼ ---
        # 1. 各点にブラー付きの円（HeatMap）を描画
        HeatMap(
            # データに重み(opacity)を追加
            [[row['lat'], row['lng'], opacity]],
            # グラデーションは透明からベース色へ
            gradient={1: f'rgb({base_rgb})'},
            min_opacity=0.2,
            # max_valは1.0に固定
            max_val=1.0,
            radius=40,
            blur=30
        ).add_to(m)

        # 2. 絵文字アイコンを上に重ねて描画
        icon_path = os.path.join(EMOJI_IMAGE_FOLDER, f"{row['name']}.png")
        if os.path.exists(icon_path):
            icon = folium.features.CustomIcon(icon_path, icon_size=(25, 25))
            folium.Marker(location=[row['lat'], row['lng']], icon=icon).add_to(m)

    # Streamlitに地図を表示（returned_objectsを空リストにして再描画を抑制）
    st_folium(m, width=725, height=500, key="emotion_map_2", returned_objects=[])

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


def render_cluster_pie_chart(pie_data):
    """感情クラスタの割合を示す円グラフを描画する"""
    st.subheader("感情クラスタの割合")

    if pie_data.empty or pie_data.sum() == 0:
        st.info("この期間の感情記録はありません。")
        return

    # 割合が0%のクラスタは表示しない
    pie_data_to_show = pie_data[pie_data > 0]

    fig, ax = plt.subplots(figsize=(8, 8))

    # クラスタと色の定義（目に優しいパステル調に変更）
    colors_dict = {
        '強いネガティブ': '#8b9dc3',   # 落ち着いた青
        '弱いネガティブ': '#a9c5e8',   # やさしい青
        'ネガティブ寄り中立': '#d3e0ea',   # ごく薄い青
        'ポジティブ寄り中立': '#fff2a6',   # やさしい黄色
        '弱いポジティブ': '#ffc8a2',   # ピーチ
        '強いポジティブ': '#ff8c94'    # コーラル
    }

    # 表示するデータのラベル、サイズ、色を準備
    labels = pie_data_to_show.index
    sizes = pie_data_to_show.values
    pie_colors = [colors_dict.get(label, '#cccccc') for label in labels]

    # ドーナツグラフを描画
    wedges, texts, autotexts = ax.pie(
        sizes,
        colors=pie_colors,
        autopct='%1.1f%%',
        startangle=90,
        pctdistance=0.85,
        wedgeprops=dict(width=0.4, edgecolor='w') # ドーナツの幅
    )
    
    # テキストのスタイルを設定
    plt.setp(texts, fontsize=12)
    plt.setp(autotexts, fontsize=10, color="black")
    ax.axis('equal')  # 円を真円に保つ
    
    # 凡例をグラフの右側に配置
    ax.legend(wedges, labels,
              title="感情クラスタ",
              loc="center left",
              bbox_to_anchor=(1, 0, 0.5, 1))

    plt.tight_layout()
    st.pyplot(fig)


def render_cumulative_chart(df):
    """【修正】感情クラスタの時間帯別構成比グラフ（積層面グラフ）を描画する"""
    st.subheader("感情クラスタの時間帯別 構成比（全期間）")
    
    if df.empty:
        st.warning("表示するデータがありません。")
        return

    fig, ax = plt.subplots(figsize=(14, 7))

    clusters = [
        '強いネガティブ', '弱いネガティブ', 'ネガティブ寄り中立',
        'ポジティブ寄り中立', '弱いポジティブ', '強いポジティブ'
    ]
    # 目に優しいパステル調の配色に変更
    colors = [
        '#8b9dc3', '#a9c5e8', '#d3e0ea', 
        '#fff2a6', '#ffc8a2', '#ff8c94'
    ]
    
    # データをプロット
    x = df.index
    # Y軸は各クラスタの割合
    # stackplotに入力するために、各列をリストとして渡す
    y = [df[c] for c in clusters]
    
    ax.stackplot(x, y, labels=clusters, colors=colors, alpha=0.8)

    # グラフの書式設定
    ax.set_ylim(0, 100)
    ax.set_ylabel('構成比 (%)', fontsize=16)
    ax.set_xlabel('時間帯', fontsize=16)
    
    # X軸の目盛りとラベルを設定
    ax.set_xticks(df.index)
    ax.set_xticklabels([f'{h}:00' for h in df.index], rotation=45, ha='right', fontsize=12)
    ax.set_xlim(df.index.min(), df.index.max())
    
    ax.legend(loc='upper left', bbox_to_anchor=(1, 1))
    plt.grid(axis='y', linestyle='--', linewidth=0.5)
    plt.tight_layout(rect=[0, 0, 0.85, 1]) # 凡例が収まるように調整
    
    st.pyplot(fig)