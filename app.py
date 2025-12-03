import streamlit as st
import matplotlib.pyplot as plt
import matplotlib.font_manager as fm
from datetime import date, datetime, timedelta
import os

# 各ファイルから必要なものをインポート
from config import JAPANESE_FONT_PATH
from data_handler import (
    initialize_firebase, 
    fetch_emotion_data,
    fetch_all_emotion_data,
    process_for_cumulative_chart,
    process_for_pie_chart
)
from ui_components import (
    load_css,
    render_header,
    render_valence_timeseries,
    render_emotion_map,
    render_input_history,
    render_cumulative_chart,
    render_cluster_pie_chart,
)

def log_access(db, user_id, token):
    """アクセスログをFirestoreに記録"""
    try:
        access_log = {
            'user_id': user_id,
            'token': token,
            'timestamp': datetime.now(),
            'session_id': st.session_state.get('session_id', None),
            'view_mode': st.session_state.get('current_view_mode', None)
        }
        
        db.collection('access_logs').add(access_log)
    except Exception as e:
        # ログ記録の失敗はアプリの動作を妨げないようにする
        print(f"アクセスログの記録に失敗: {e}")


def log_page_view(db, user_id, view_mode):
    """ページビュー開始時刻を記録"""
    try:
        if 'page_view_start' not in st.session_state:
            st.session_state.page_view_start = datetime.now()
            st.session_state.current_view_mode = view_mode
            
            page_view_log = {
                'session_id': st.session_state.get('session_id', None),
                'view_mode': view_mode,
                'start_time': st.session_state.page_view_start,
                'end_time': None,
                'duration_seconds': None
            }
            
            # ユーザーのサブコレクションとして保存
            doc_ref = db.collection('users').document(user_id).collection('page_views').add(page_view_log)
            st.session_state.page_view_doc_id = doc_ref[1].id
    except Exception as e:
        print(f"ページビュー記録に失敗: {e}")


def update_page_view_duration(db, user_id, view_mode):
    """ページビューの終了時刻と滞在時間を更新"""
    try:
        # 表示モードが変更された場合、前のページビューを終了
        if 'page_view_start' in st.session_state and st.session_state.get('current_view_mode') != view_mode:
            end_time = datetime.now()
            duration = (end_time - st.session_state.page_view_start).total_seconds()
            
            if 'page_view_doc_id' in st.session_state:
                db.collection('users').document(user_id).collection('page_views').document(st.session_state.page_view_doc_id).update({
                    'end_time': end_time,
                    'duration_seconds': duration
                })
            
            # 新しいページビューを開始
            del st.session_state.page_view_start
            del st.session_state.page_view_doc_id
            log_page_view(db, user_id, view_mode)
    except Exception as e:
        print(f"ページビュー更新に失敗: {e}")


def main():
    """アプリケーションのメイン実行関数"""
    load_css("style.css")

    if 'current_date' not in st.session_state:
        st.session_state.current_date = date.today()
    
    # セッションIDを生成（初回アクセス時のみ）
    if 'session_id' not in st.session_state:
        import uuid
        st.session_state.session_id = str(uuid.uuid4())

    if os.path.exists(JAPANESE_FONT_PATH):
        fm.fontManager.addfont(JAPANESE_FONT_PATH)
        plt.rcParams['font.family'] = 'Noto Sans JP'
    else:
        st.caption(f"⚠️ 日本語フォントが見つかりません: {JAPANESE_FONT_PATH}")
    
    db = initialize_firebase()
    if db is None:
        st.stop()

    # --- トークンによるユーザー認証 ---
    token = st.query_params.get("t")
    
    # トークンが存在しない、または無効な場合はエラーを表示して停止
    if not token or token not in st.secrets.get("tokens", {}):
        st.error("アクセス権がありません。正しいURLを指定してください。")
        st.stop()
        
    # トークンからユーザーIDを取得
    user_id = st.secrets["tokens"][token]
    
    # アクセスログを記録
    log_access(db, user_id, token)

    # ラジオボタンで表示モードを選択
    view_options = ["1日間", "3日間", "累積分析"]
    selected_view = st.radio(
        "表示モードを選択",
        options=view_options,
        horizontal=True,
        label_visibility="collapsed"
    )

    # ページビューを記録・更新
    log_page_view(db, user_id, selected_view)
    update_page_view_duration(db, user_id, selected_view)

    if selected_view == "1日間":
        display_dashboard(db, user_id, days=1)
    
    elif selected_view == "3日間":
        display_dashboard(db, user_id, days=3)

    elif selected_view == "累積分析":
        # 全期間のデータを取得して処理
        all_data = fetch_all_emotion_data(db, user_id)
        cumulative_df = process_for_cumulative_chart(all_data)
        pie_data = process_for_pie_chart(all_data)
        
        # 過去2週間のデータをフィルタリング（地図表示用）
        two_weeks_ago = datetime.now() - timedelta(days=21)
        recent_data = all_data[all_data['datetime'] >= two_weeks_ago].copy()
        
        # ヘッダーと累積グラフを描画
        # 期間別表示と異なり、日付ナビゲーションは不要なため、一部のコンポーネントのみ表示
        st.markdown(f"<h1 class='main-title'>ユーザ: {user_id} | 全期間のデータを集計</h1>", unsafe_allow_html=True)
        st.divider()
        render_cumulative_chart(cumulative_df)
        
        # 円グラフと新しい地図を表示
        st.divider()
        render_cluster_pie_chart(pie_data)
        st.divider()
        render_emotion_map(recent_data)


def display_dashboard(db, user_id, days: int):
    """期間別ダッシュボードを表示する共通関数"""
    # 期間とユーザーIDを指定してデータを取得
    df = fetch_emotion_data(db, st.session_state.current_date, days=days, user_id=user_id)
    
    # ヘッダーを表示
    render_header(df, st.session_state.current_date, days=days, user_id=user_id)

    if df.empty:
        st.markdown(f"<p style='font-size: 24px'>この期間の記録はありません。</p>", unsafe_allow_html=True)
        # データがなくても円グラフのコンテナは表示
        st.subheader("感情クラスタの割合")
        st.info("この期間の感情記録はありません。")
        return

    # 円グラフ用のデータを処理
    pie_data = process_for_pie_chart(df)

    # 各UIコンポーネントを描画
    render_valence_timeseries(df, st.session_state.current_date, days=days)
    st.divider()
    render_cluster_pie_chart(pie_data)
    st.divider()
    render_emotion_map(df)
    st.divider()
    render_input_history(df)


if __name__ == "__main__":
    HIDE_ST_STYLE = """
                <style>
                div[data-testid="stToolbar"] {
                visibility: hidden;
                height: 0%;
                position: fixed;
                }
                div[data-testid="stDecoration"] {
                visibility: hidden;
                height: 0%;
                position: fixed;
                }
                #MainMenu {
                visibility: hidden;
                height: 0%;
                }
                header {
                visibility: hidden;
                height: 0%;
                }
                footer {
                visibility: hidden;
                height: 0%;
                }
				        .appview-container .main .block-container{
                            padding-top: 1rem;
                            padding-right: 3rem;
                            padding-left: 3rem;
                            padding-bottom: 1rem;
                        }  
                        .reportview-container {
                            padding-top: 0rem;
                            padding-right: 3rem;
                            padding-left: 3rem;
                            padding-bottom: 0rem;
                        }
                        header[data-testid="stHeader"] {
                            z-index: -1;
                        }
                        div[data-testid="stToolbar"] {
                        z-index: 100;
                        }
                        div[data-testid="stDecoration"] {
                        z-index: 100;
                        }
                </style>
"""

# HIDE_ST_STYLEを適用
st.markdown(HIDE_ST_STYLE, unsafe_allow_html=True)
main()