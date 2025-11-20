import pandas as pd
import numpy as np
from datetime import datetime
import firebase_admin
from firebase_admin import credentials, firestore
from config import FIREBASE_CREDENTIALS_PATH


def initialize_firebase_standalone():
    """Firebase接続を初期化（Streamlit非依存）"""
    if not firebase_admin._apps:
        try:
            cred = credentials.Certificate(FIREBASE_CREDENTIALS_PATH)
            firebase_admin.initialize_app(cred)
        except Exception as e:
            print(f"Firebaseの初期化に失敗しました: {e}")
            return None
    return firestore.client()


def fetch_all_emotion_data_standalone(db_client, user_id):
    """全期間の感情データをFirestoreから取得（Streamlit非依存）"""
    if db_client is None:
        return pd.DataFrame()

    query = db_client.collection("users").document(user_id).collection("emotions")
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
        return '1-強いネガティブ'
    elif valence <= 4.5:
        return '2-弱いネガティブ'
    elif valence <= 5.2:
        return '3-ネガティブ寄り中立'
    elif valence <= 6.0:
        return '4-ポジティブ寄り中立'
    elif valence <= 7.6:
        return '5-弱いポジティブ'
    else:
        return '6-強いポジティブ'


def classify_by_cluster(cluster):
    """クラスタ名から感情カテゴリを判定する"""
    if 'ネガティブ' in cluster:
        return 'Negative'
    elif 'ポジティブ' in cluster:
        return 'Positive'
    else:
        return 'Neutral'


def classify_by_valence(valence):
    """Valence値から感情カテゴリ（3分類）を判定する"""
    if valence <= 4.5:
        return 'Negative'
    elif valence <= 6.0:
        return 'Neutral'
    else:
        return 'Positive'


def algorithm_most_frequent(day_df):
    """最頻値法: 最も頻出した感情クラスタ群を採用"""
    if day_df.empty:
        return 'Neutral'
    
    # 各カテゴリの出現回数をカウント
    day_df['emotion_category'] = day_df['cluster'].apply(classify_by_cluster)
    
    negative_count = (day_df['emotion_category'] == 'Negative').sum()
    neutral_count = (day_df['emotion_category'] == 'Neutral').sum()
    positive_count = (day_df['emotion_category'] == 'Positive').sum()
    
    # 最も多いカテゴリを採用（同点時はPositive優先）
    max_count = max(negative_count, neutral_count, positive_count)
    
    if positive_count == max_count:
        return 'Positive'
    elif negative_count == max_count:
        return 'Negative'
    else:
        return 'Neutral'


def algorithm_peak_value(day_df):
    """ピーク値法: 正規化したValence値の絶対値が最大のものを採用"""
    if day_df.empty:
        return 'Neutral'
    
    # Valence値を正規化 (中央値5.6を0とする)
    normalized_valence = day_df['valence'] - 5.6
    
    # 絶対値が最大のインデックスを取得
    max_abs_idx = normalized_valence.abs().idxmax()
    peak_valence = day_df.loc[max_abs_idx, 'valence']
    
    return classify_by_valence(peak_valence)


def algorithm_latest_value(day_df):
    """最新値法: その日の最後に記録された感情を採用"""
    if day_df.empty:
        return 'Neutral'
    
    # datetimeでソートして最新のものを取得
    latest_valence = day_df.sort_values('datetime').iloc[-1]['valence']
    
    return classify_by_valence(latest_valence)


def algorithm_average_value(day_df):
    """平均値法: Valence値の平均を計算し分類"""
    if day_df.empty:
        return 'Neutral'
    
    avg_valence = day_df['valence'].mean()
    
    return classify_by_valence(avg_valence)


def algorithm_weighted_average(day_df):
    """重み付き平均法: 時刻が遅いほど重みを大きくした加重平均"""
    if day_df.empty:
        return 'Neutral'
    
    # datetimeでソート
    day_df_sorted = day_df.sort_values('datetime').copy()
    
    # 時間を0-1の範囲に正規化して重みとする
    time_diffs = (day_df_sorted['datetime'] - day_df_sorted['datetime'].min()).dt.total_seconds()
    
    if time_diffs.max() == 0:
        # すべて同じ時刻の場合は通常の平均
        weighted_avg = day_df_sorted['valence'].mean()
    else:
        weights = time_diffs / time_diffs.max()
        # 重みを1以上にする(最小0.5、最大1.5)
        weights = weights * 1.0 + 0.5
        weighted_avg = np.average(day_df_sorted['valence'], weights=weights)
    
    return classify_by_valence(weighted_avg)


def algorithm_cluster_based(day_df):
    """クラスタベース法: 6つのクラスタの出現頻度から判定"""
    if day_df.empty:
        return 'Neutral'
    
    # クラスタごとの出現回数をカウント
    cluster_counts = day_df['cluster'].value_counts()
    
    # 各カテゴリの合計を計算
    negative_count = 0
    neutral_count = 0
    positive_count = 0
    
    for cluster_name, count in cluster_counts.items():
        if 'ネガティブ' in cluster_name:
            negative_count += count
        elif 'ポジティブ' in cluster_name:
            positive_count += count
        else:
            neutral_count += count
    
    # 最も多いカテゴリを採用
    max_count = max(negative_count, neutral_count, positive_count)
    
    if positive_count == max_count:
        return 'Positive'
    elif negative_count == max_count:
        return 'Negative'
    else:
        return 'Neutral'


def algorithm_weighted_cluster(day_df):
    """重み付きクラスタ法: クラスタの強さに応じて重み付け"""
    if day_df.empty:
        return 'Neutral'
    
    # クラスタごとの重み定義
    cluster_weights = {
        '1-強いネガティブ': -2.0,
        '2-弱いネガティブ': -1.0,
        '3-ネガティブ寄り中立': -0.3,
        '4-ポジティブ寄り中立': 0.3,
        '5-弱いポジティブ': 1.0,
        '6-強いポジティブ': 2.0
    }
    
    # 重み付きスコアを計算
    total_score = 0
    for _, row in day_df.iterrows():
        cluster = row['cluster']
        weight = cluster_weights.get(cluster, 0)
        total_score += weight
    
    # 平均スコアで判定
    avg_score = total_score / len(day_df)
    
    if avg_score > 0.3:
        return 'Positive'
    elif avg_score < -0.3:
        return 'Negative'
    else:
        return 'Neutral'


def analyze_emotions_by_day(user_id):
    """ユーザーの全感情データを日ごとに分析"""
    # Firebaseからデータ取得（Streamlit非依存版を使用）
    db = initialize_firebase_standalone()
    if db is None:
        print("Firebase接続に失敗しました")
        return None
    
    df = fetch_all_emotion_data_standalone(db, user_id)
    
    if df.empty:
        print(f"ユーザー {user_id} のデータが見つかりません")
        return None
    
    # クラスタ列を追加
    if 'cluster' not in df.columns:
        df['cluster'] = df['valence'].apply(assign_cluster)
    
    # 日付ごとにグループ化
    df['date'] = df['datetime'].dt.date
    grouped = df.groupby('date')
    
    results = []
    
    for date, day_df in grouped:
        result = {
            'user_id': user_id,
            'date': date,
            'record_count': len(day_df),
            'most_frequent': algorithm_most_frequent(day_df),
            'peak_value': algorithm_peak_value(day_df),
            'latest_value': algorithm_latest_value(day_df),
            'average_value': algorithm_average_value(day_df),
            'weighted_average': algorithm_weighted_average(day_df),
            'cluster_based': algorithm_cluster_based(day_df),
            'weighted_cluster': algorithm_weighted_cluster(day_df)
        }
        results.append(result)
    
    return pd.DataFrame(results)


def main():
    """メイン処理: 複数ユーザーの感情を分析してCSV出力"""
    # 分析対象のユーザーIDリスト
    user_ids = [
                "test01",
                "Test02",
                "User00",
                "User01",
                "User03",
                "User04",
                "User05",
                "02",
                "06",
                "07",
                "08",
                "09",
                "10",
                "11"
            ]

    all_results = []
    
    for user_id in user_ids:
        print(f"ユーザー {user_id} を分析中...")
        user_results = analyze_emotions_by_day(user_id)
        
        if user_results is not None:
            all_results.append(user_results)
    
    if all_results:
        # 全ユーザーの結果を結合
        final_df = pd.concat(all_results, ignore_index=True)
        
        # CSV出力
        output_path = 'emotion_analysis_results.csv'
        final_df.to_csv(output_path, index=False, encoding='utf-8-sig')
        print(f"\n分析結果を {output_path} に保存しました")
        print(f"総レコード数: {len(final_df)}")
        print("\n各アルゴリズムの感情分布:")
        print(final_df[['most_frequent', 'peak_value', 'latest_value', 
                       'average_value', 'weighted_average', 'cluster_based', 'weighted_cluster']].apply(pd.Series.value_counts))
        
        # アンケート結果との一致率を計算
        calculate_accuracy(final_df)
    else:
        print("分析結果がありません")


def calculate_accuracy(results_df):
    """アンケート結果との一致率を計算"""
    # レポートファイルを開く
    report_file = open('report.txt', 'w', encoding='utf-8')
    
    def print_and_write(message):
        """コンソールとファイルの両方に出力"""
        print(message)
        report_file.write(message + '\n')
    
    try:
        # CSVファイルを読み込み
        try:
            ex_date_df = pd.read_csv('ex_date.csv')
            print_and_write(f"ex_date.csv を読み込みました: {len(ex_date_df)}行")
        except FileNotFoundError:
            print_and_write("\nエラー: ex_date.csv が見つかりません")
            print_and_write("ex_date.csv を作成してください。フォーマット例:")
            print_and_write("User名,1日目,2日目,3日目")
            print_and_write("test01,2024/10/01,2024/10/02,2024/10/03")
            report_file.close()
            return
        except Exception as e:
            print_and_write(f"\nex_date.csv の読み込み中にエラー: {e}")
            report_file.close()
            return
        
        try:
            questionnaire_df = pd.read_csv('questionnaire.csv')
            print_and_write(f"questionnaire.csv を読み込みました: {len(questionnaire_df)}行")
        except FileNotFoundError:
            print_and_write("\nエラー: questionnaire.csv が見つかりません")
            report_file.close()
            return
        except Exception as e:
            print_and_write(f"\nquestionnaire.csv の読み込み中にエラー: {e}")
            report_file.close()
            return
        
        # ユーザー名の正規化（大文字小文字を統一）
        questionnaire_df['User名'] = questionnaire_df['User名'].str.lower()
        ex_date_df['User名'] = ex_date_df['User名'].str.lower()
        
        # アンケート結果を変換
        comparison_results = []
        
        for _, q_row in questionnaire_df.iterrows():
            user_name = q_row['User名']
            
            # ex_date.csvから該当ユーザーの実験日を取得
            user_ex_dates = ex_date_df[ex_date_df['User名'].str.lower() == user_name]
            
            if user_ex_dates.empty:
                continue
            
            for day_num in [1, 2, 3]:
                # 実験日の取得
                date_col = f'{day_num}日目'
                if date_col not in user_ex_dates.columns:
                    continue
                
                exp_date_str = user_ex_dates.iloc[0][date_col]
                if pd.isna(exp_date_str):
                    continue
                
                # 日付を解析
                try:
                    exp_date = pd.to_datetime(exp_date_str).date()
                except:
                    continue
                
                # アンケートの最高スコアのカテゴリを判定
                positive_score = q_row[f'{day_num}日目positive']
                neutral_score = q_row[f'{day_num}日目neutral']
                negative_score = q_row[f'{day_num}日目negative']
                
                max_score = max(positive_score, neutral_score, negative_score)
                
                # 最高スコアを持つカテゴリを全て取得（同点対応）
                correct_categories = []
                if positive_score == max_score:
                    correct_categories.append('Positive')
                if neutral_score == max_score:
                    correct_categories.append('Neutral')
                if negative_score == max_score:
                    correct_categories.append('Negative')
                
                # 【追加】最低スコアを持つカテゴリを特定
                min_score = min(positive_score, neutral_score, negative_score)
                worst_categories = []
                if positive_score == min_score:
                    worst_categories.append('Positive')
                if neutral_score == min_score:
                    worst_categories.append('Neutral')
                if negative_score == min_score:
                    worst_categories.append('Negative')
                
                # 表示用に最初のカテゴリを採用（従来通り）
                questionnaire_emotion = correct_categories[0] if correct_categories else 'Neutral'
                
                # 推定結果から該当日のデータを取得
                user_result = results_df[
                    (results_df['user_id'].str.lower() == user_name) & 
                    (results_df['date'] == exp_date)
                ]
                
                if user_result.empty:
                    continue
                
                result_row = user_result.iloc[0]
                
                # 各アルゴリズムとの一致を確認（複数正解に対応）
                comparison_results.append({
                    'user_id': user_name,
                    'date': exp_date,
                    'day_num': day_num,
                    'questionnaire': questionnaire_emotion,
                    'correct_categories': ','.join(correct_categories),  # 正解カテゴリをカンマ区切りで保存
                    'worst_categories': ','.join(worst_categories),  # 【追加】最悪のカテゴリ
                    'positive_score': positive_score,
                    'neutral_score': neutral_score,
                    'negative_score': negative_score,
                    'most_frequent': result_row['most_frequent'],
                    'peak_value': result_row['peak_value'],
                    'latest_value': result_row['latest_value'],
                    'average_value': result_row['average_value'],
                    'weighted_average': result_row['weighted_average'],
                    'cluster_based': result_row['cluster_based'],
                    'weighted_cluster': result_row['weighted_cluster'],
                    'most_frequent_match': result_row['most_frequent'] in correct_categories,
                    'peak_value_match': result_row['peak_value'] in correct_categories,
                    'latest_value_match': result_row['latest_value'] in correct_categories,
                    'average_value_match': result_row['average_value'] in correct_categories,
                    'weighted_average_match': result_row['weighted_average'] in correct_categories,
                    'cluster_based_match': result_row['cluster_based'] in correct_categories,
                    'weighted_cluster_match': result_row['weighted_cluster'] in correct_categories,
                    # 【追加】最悪の選択を避けたかどうか
                    'most_frequent_avoid_worst': result_row['most_frequent'] not in worst_categories,
                    'peak_value_avoid_worst': result_row['peak_value'] not in worst_categories,
                    'latest_value_avoid_worst': result_row['latest_value'] not in worst_categories,
                    'average_value_avoid_worst': result_row['average_value'] not in worst_categories,
                    'weighted_average_avoid_worst': result_row['weighted_average'] not in worst_categories,
                    'cluster_based_avoid_worst': result_row['cluster_based'] not in worst_categories,
                    'weighted_cluster_avoid_worst': result_row['weighted_cluster'] not in worst_categories
                })
        
        if comparison_results:
            comparison_df = pd.DataFrame(comparison_results)
            
            # 一致率を計算
            algorithms = ['most_frequent', 'peak_value', 'latest_value', 'average_value', 'weighted_average', 'cluster_based', 'weighted_cluster']
            
            print_and_write("\n=== 全体の一致率とスコア比較 ===")
            for algo in algorithms:
                match_col = f'{algo}_match'
                avoid_worst_col = f'{algo}_avoid_worst'
                
                accuracy = comparison_df[match_col].mean() * 100
                avoid_worst_rate = comparison_df[avoid_worst_col].mean() * 100
                match_count = comparison_df[match_col].sum()
                avoid_worst_count = comparison_df[avoid_worst_col].sum()
                total_count = len(comparison_df)
                
                # 真のスコアと予測スコアを計算
                true_total_score = 0
                pred_total_score = 0
                
                for _, row in comparison_df.iterrows():
                    # 真のラベルに対応するスコア
                    true_label = row['questionnaire'].lower()
                    true_total_score += row[f'{true_label}_score']
                    
                    # 予測ラベルに対応するスコア
                    pred_label = row[algo].lower()
                    pred_total_score += row[f'{pred_label}_score']
                
                score_ratio = (pred_total_score / true_total_score * 100) if true_total_score > 0 else 0
                
                print_and_write(f"{algo}:")
                print_and_write(f"  1位一致率: {accuracy:.1f}% ({match_count}/{total_count})")
                print_and_write(f"  最悪回避率: {avoid_worst_rate:.1f}% ({avoid_worst_count}/{total_count})")
                print_and_write(f"  スコア - 真: {true_total_score}, 予測: {pred_total_score}, 比率: {score_ratio:.1f}%")
            
            # === 被験者ごとの一致率 ===
            print_and_write(f"\n=== 被験者ごとの一致率（1位一致 / 最悪回避） ===")
            for algo in algorithms:
                print_and_write(f"\n【{algo}】")
                match_col = f'{algo}_match'
                avoid_worst_col = f'{algo}_avoid_worst'
                
                user_stats = comparison_df.groupby('user_id').agg({
                    match_col: ['sum', 'count', 'mean'],
                    avoid_worst_col: ['sum', 'mean']
                })
                
                user_stats.columns = ['match_count', 'total', 'match_rate', 'avoid_count', 'avoid_rate']
                user_stats['match_pct'] = user_stats['match_rate'] * 100
                user_stats['avoid_pct'] = user_stats['avoid_rate'] * 100
                user_stats = user_stats.sort_values('match_rate', ascending=False)
                
                print_and_write("User ID    1位一致   最悪回避")
                print_and_write("-" * 50)
                for user_id, row in user_stats.iterrows():
                    print_and_write(f"{user_id:10s} {int(row['match_count']):2d}/{int(row['total']):2d} ({row['match_pct']:5.1f}%)  "
                                  f"{int(row['avoid_count']):2d}/{int(row['total']):2d} ({row['avoid_pct']:5.1f}%)")
            
            # === 被験者ごとのスコア比較（各アルゴリズム） ===
            print_and_write(f"\n=== 被験者ごとのスコア比較 ===")
            
            for algo in algorithms:
                print_and_write(f"\n【{algo}】")
                
                # 各ユーザーの真のスコアと予測スコアを計算
                user_score_comparison = []
                
                for user_id in comparison_df['user_id'].unique():
                    user_data = comparison_df[comparison_df['user_id'] == user_id]
                    
                    true_score = 0
                    pred_score = 0
                    
                    for _, row in user_data.iterrows():
                        # 真のラベルに対応するスコア
                        true_label = row['questionnaire'].lower()
                        true_score += row[f'{true_label}_score']
                        
                        # 予測ラベルに対応するスコア
                        pred_label = row[algo].lower()
                        pred_score += row[f'{pred_label}_score']
                    
                    score_ratio = (pred_score / true_score * 100) if true_score > 0 else 0
                    
                    user_score_comparison.append({
                        'user_id': user_id,
                        'true_score': true_score,
                        'pred_score': pred_score,
                        'ratio': score_ratio
                    })
                
                # 比率の高い順にソート
                user_score_df = pd.DataFrame(user_score_comparison).sort_values('ratio', ascending=False)
                
                print_and_write("User ID    真スコア  予測スコア  比率")
                print_and_write("-" * 45)
                for _, row in user_score_df.iterrows():
                    print_and_write(f"{row['user_id']:10s} {int(row['true_score']):4d}      {int(row['pred_score']):4d}      {row['ratio']:5.1f}%")
            
            # 詳細結果をCSVに保存
            comparison_df.to_csv('questionnaire_comparison.csv', index=False, encoding='utf-8-sig')
            print_and_write(f"\n詳細な比較結果を questionnaire_comparison.csv に保存しました")
            print_and_write(f"分析レポートを report.txt に保存しました")
        else:
            print_and_write("\n比較可能なデータが見つかりませんでした")
            
    except Exception as e:
        print_and_write(f"\n一致率の計算中にエラーが発生しました: {e}")
        import traceback
        traceback.print_exc()
    finally:
        report_file.close()


if __name__ == "__main__":
    main()
