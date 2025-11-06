import pandas as pd
import numpy as np
from sklearn.ensemble import RandomForestClassifier
from sklearn.model_selection import LeaveOneOut
from sklearn.preprocessing import LabelEncoder
import warnings
warnings.filterwarnings('ignore')

from empathy import (
    initialize_firebase_standalone,
    fetch_all_emotion_data_standalone,
    assign_cluster
)


def extract_features(day_df):
    """日ごとの感情データから特徴量を抽出"""
    if day_df.empty:
        return None
    
    # 時系列でソート
    day_df_sorted = day_df.sort_values('datetime').copy()
    
    features = {
        # 基本統計量
        'mean_valence': day_df['valence'].mean(),
        'median_valence': day_df['valence'].median(),
        'std_valence': day_df['valence'].std() if len(day_df) > 1 else 0,
        'min_valence': day_df['valence'].min(),
        'max_valence': day_df['valence'].max(),
        'range_valence': day_df['valence'].max() - day_df['valence'].min(),
        
        # 記録数
        'record_count': len(day_df),
        
        # 時間的特徴
        'first_valence': day_df_sorted.iloc[0]['valence'],
        'last_valence': day_df_sorted.iloc[-1]['valence'],
        'valence_change': day_df_sorted.iloc[-1]['valence'] - day_df_sorted.iloc[0]['valence'],
        
        # ピーク値（正規化後の絶対値最大）
        'peak_abs_valence': abs(day_df['valence'] - 5.6).max(),
        
        # 時間重み付き平均
        'weighted_avg': calculate_weighted_average(day_df_sorted),
        
        # クラスタ分布
        'neg_ratio': sum(day_df['valence'] <= 4.5) / len(day_df),
        'neu_ratio': sum((day_df['valence'] > 4.5) & (day_df['valence'] <= 6.0)) / len(day_df),
        'pos_ratio': sum(day_df['valence'] > 6.0) / len(day_df),
    }
    
    return features


def calculate_weighted_average(day_df_sorted):
    """時間重み付き平均を計算"""
    time_diffs = (day_df_sorted['datetime'] - day_df_sorted['datetime'].min()).dt.total_seconds()
    
    if time_diffs.max() == 0:
        return day_df_sorted['valence'].mean()
    
    weights = time_diffs / time_diffs.max() * 1.0 + 0.5
    return np.average(day_df_sorted['valence'], weights=weights)


def prepare_training_data():
    """学習データを準備"""
    # アンケート結果と実験日を読み込み
    try:
        ex_date_df = pd.read_csv('ex_date.csv')
        questionnaire_df = pd.read_csv('questionnaire.csv')
    except FileNotFoundError as e:
        print(f"エラー: 必要なファイルが見つかりません - {e}")
        return None, None, None
    
    # 元のユーザー名を保持
    questionnaire_df['original_user_name'] = questionnaire_df['User名']
    ex_date_df['original_user_name'] = ex_date_df['User名']
    
    # ユーザー名の正規化（照合用）
    questionnaire_df['User名'] = questionnaire_df['User名'].str.lower()
    ex_date_df['User名'] = ex_date_df['User名'].str.lower()
    
    # デバッグ: ユーザー一覧を表示
    print(f"\nquestionnaire.csvのユーザー一覧: {questionnaire_df['User名'].tolist()}")
    print(f"ex_date.csvのユーザー一覧: {ex_date_df['User名'].tolist()}")
    
    # Firebase接続
    db = initialize_firebase_standalone()
    if db is None:
        print("Firebase接続に失敗しました")
        return None, None, None
    
    training_samples = []
    
    for _, q_row in questionnaire_df.iterrows():
        user_name = q_row['User名']  # 正規化された名前（照合用）
        original_user_name = q_row['original_user_name']  # 元の名前（Firebase用）
        
        print(f"\n処理中のユーザー: {user_name} (元: {original_user_name})")
        
        # ex_date.csvから該当ユーザーの実験日を取得
        user_ex_dates = ex_date_df[ex_date_df['User名'] == user_name]
        
        if user_ex_dates.empty:
            print(f"  → ex_date.csvに{user_name}が見つかりません")
            continue
        
        # Firestoreからデータ取得（元の名前を使用）
        df = fetch_all_emotion_data_standalone(db, original_user_name)
        
        if df.empty:
            print(f"  → Firestoreに{original_user_name}のデータが見つかりません")
            continue
        
        print(f"  → Firestoreから{len(df)}件のデータを取得")
        
        # クラスタ列を追加
        if 'cluster' not in df.columns:
            df['cluster'] = df['valence'].apply(assign_cluster)
        
        # 日付ごとに処理
        df['date'] = df['datetime'].dt.date
        
        for day_num in [1, 2, 3]:
            date_col = f'{day_num}日目'
            if date_col not in user_ex_dates.columns:
                continue
            
            exp_date_str = user_ex_dates.iloc[0][date_col]
            if pd.isna(exp_date_str):
                print(f"  → {day_num}日目: 日付が空です")
                continue
            
            try:
                exp_date = pd.to_datetime(exp_date_str).date()
                print(f"  → {day_num}日目: {exp_date}")
            except:
                print(f"  → {day_num}日目: 日付の解析に失敗 ({exp_date_str})")
                continue
            
            # その日のデータを取得
            day_df = df[df['date'] == exp_date]
            
            if day_df.empty:
                print(f"     {exp_date}のデータが見つかりません")
                continue
            
            print(f"     {exp_date}: {len(day_df)}件のデータを使用")
            
            # 特徴量を抽出
            features = extract_features(day_df)
            
            if features is None:
                continue
            
            # アンケートスコアを取得
            positive_score = q_row[f'{day_num}日目positive']
            neutral_score = q_row[f'{day_num}日目neutral']
            negative_score = q_row[f'{day_num}日目negative']
            
            # 真のラベル（最高スコアのカテゴリ）
            max_score = max(positive_score, neutral_score, negative_score)
            min_score = min(positive_score, neutral_score, negative_score)
            
            # 【変更】最高スコアを持つカテゴリを全て取得（同点対応）
            correct_categories = []
            if positive_score == max_score:
                correct_categories.append('Positive')
            if neutral_score == max_score:
                correct_categories.append('Neutral')
            if negative_score == max_score:
                correct_categories.append('Negative')
            
            # 学習用には最初のカテゴリを使用
            true_label = correct_categories[0] if correct_categories else 'Neutral'
            
            # 【変更】最低スコアを持つカテゴリを全て取得（同点対応）
            worst_categories = []
            if positive_score == min_score:
                worst_categories.append('Positive')
            if neutral_score == min_score:
                worst_categories.append('Neutral')
            if negative_score == min_score:
                worst_categories.append('Negative')
            
            worst_label = worst_categories[0] if worst_categories else 'Neutral'
            
            # 学習サンプルとして追加
            sample = features.copy()
            sample.update({
                'user_id': user_name,  # 正規化された名前を保存
                'date': exp_date,
                'true_label': true_label,
                'worst_label': worst_label,
                'correct_categories': ','.join(correct_categories),  # 【追加】全正解カテゴリを保存
                'worst_categories': ','.join(worst_categories),  # 【追加】全最悪カテゴリを保存
                'positive_score': positive_score,
                'neutral_score': neutral_score,
                'negative_score': negative_score
            })
            
            training_samples.append(sample)
    
    if not training_samples:
        print("学習データが見つかりませんでした")
        return None, None, None
    
    print(f"\n最終的な学習サンプル数: {len(training_samples)}")
    print(f"学習に使用されたユーザー: {set([s['user_id'] for s in training_samples])}")
    
    training_df = pd.DataFrame(training_samples)
    
    # 特徴量とラベルを分離
    feature_columns = [col for col in training_df.columns 
                      if col not in ['user_id', 'date', 'true_label', 'worst_label',
                                    'correct_categories', 'worst_categories',
                                    'positive_score', 'neutral_score', 'negative_score']]
    
    X = training_df[feature_columns]
    y = training_df['true_label']
    
    print(f"\n学習データ: {len(training_df)}サンプル")
    print(f"特徴量: {len(feature_columns)}次元")
    print(f"クラス分布:\n{y.value_counts()}")
    
    return X, y, training_df


def train_and_evaluate():
    """モデルを学習して評価"""
    X, y, training_df = prepare_training_data()
    
    if X is None or y is None:
        return
    
    # レポートファイルを開く
    report_file = open('ml_report.txt', 'w', encoding='utf-8')
    
    def print_and_write(message):
        """コンソールとファイルの両方に出力"""
        print(message)
        report_file.write(message + '\n')
    
    # ラベルエンコーディング
    le = LabelEncoder()
    y_encoded = le.fit_transform(y)
    
    # ランダムフォレストモデル
    model = RandomForestClassifier(
        n_estimators=200,
        max_depth=10,
        min_samples_split=2,
        min_samples_leaf=1,
        random_state=42,
        class_weight='balanced'
    )
    
    # Leave-One-Out クロスバリデーション
    print_and_write("\n=== Leave-One-Out クロスバリデーション ===")
    loo = LeaveOneOut()
    
    predictions = []
    scores_list = []
    
    for train_idx, test_idx in loo.split(X):
        X_train, X_test = X.iloc[train_idx], X.iloc[test_idx]
        y_train, y_test = y_encoded[train_idx], y_encoded[test_idx]
        
        model.fit(X_train, y_train)
        
        # 確率予測を取得
        pred_proba = model.predict_proba(X_test)[0]
        
        # テストサンプルの情報を取得
        test_sample = training_df.iloc[test_idx[0]]
        worst_label = test_sample['worst_label']
        worst_idx = list(le.classes_).index(worst_label)
        
        # 最悪のラベルの確率を0にして再正規化
        adjusted_proba = pred_proba.copy()
        adjusted_proba[worst_idx] = 0
        
        if adjusted_proba.sum() > 0:
            adjusted_proba = adjusted_proba / adjusted_proba.sum()
            pred = np.argmax(adjusted_proba)
        else:
            pred = model.predict(X_test)[0]
        
        predictions.append(le.inverse_transform([pred])[0])
        
        # アンケートスコアを取得
        true_label = test_sample['true_label']
        pred_label = le.inverse_transform([pred])[0]
        
        # 【変更】複数正解対応
        correct_categories = test_sample['correct_categories'].split(',')
        worst_categories = test_sample['worst_categories'].split(',')
        
        # スコアを計算
        true_score = test_sample[f'{true_label.lower()}_score']
        pred_score = test_sample[f'{pred_label.lower()}_score']
        
        # 【変更】正解判定：予測ラベルが正解カテゴリのいずれかに該当すればTrue
        match = pred_label in correct_categories
        
        # 【変更】最悪回避判定：予測ラベルが最悪カテゴリのいずれにも該当しなければTrue
        avoid_worst = pred_label not in worst_categories
        
        scores_list.append({
            'user_id': test_sample['user_id'],
            'date': test_sample['date'],
            'true_label': true_label,
            'correct_categories': test_sample['correct_categories'],
            'worst_label': worst_label,
            'worst_categories': test_sample['worst_categories'],
            'pred_label': pred_label,
            'true_score': true_score,
            'pred_score': pred_score,
            'match': match,
            'avoid_worst': avoid_worst
        })
    
    # 結果をDataFrameに
    results_df = pd.DataFrame(scores_list)
    
    # === 全体の結果 ===
    overall_accuracy = results_df['match'].mean()
    avoid_worst_rate = results_df['avoid_worst'].mean()
    total_true_score = results_df['true_score'].sum()
    total_pred_score = results_df['pred_score'].sum()
    
    print_and_write(f"\n=== 全体の結果 ===")
    print_and_write(f"1位一致率: {overall_accuracy * 100:.1f}%")
    print_and_write(f"最悪回避率: {avoid_worst_rate * 100:.1f}%")
    print_and_write(f"真のラベルの合計スコア: {total_true_score}")
    print_and_write(f"予測ラベルの合計スコア: {total_pred_score}")
    print_and_write(f"スコア比率: {total_pred_score / total_true_score * 100:.1f}%")
    
    # === 被験者ごとの一致率 ===
    print_and_write(f"\n=== 被験者ごとの一致率（1位一致 / 最悪回避） ===")
    user_stats = results_df.groupby('user_id').agg({
        'match': ['sum', 'count', 'mean'],
        'avoid_worst': ['sum', 'mean']
    }).round(3)
    
    user_stats.columns = ['match_count', 'total', 'match_rate', 'avoid_count', 'avoid_rate']
    user_stats['match_pct'] = user_stats['match_rate'] * 100
    user_stats['avoid_pct'] = user_stats['avoid_rate'] * 100
    user_stats = user_stats.sort_values('match_rate', ascending=False)
    
    print_and_write("\nUser ID    1位一致   最悪回避")
    print_and_write("-" * 50)
    for user_id, row in user_stats.iterrows():
        print_and_write(f"{user_id:10s} {int(row['match_count']):2d}/{int(row['total']):2d} ({row['match_pct']:5.1f}%)  "
                      f"{int(row['avoid_count']):2d}/{int(row['total']):2d} ({row['avoid_pct']:5.1f}%)")
    
    # === 被験者ごとのスコア比較 ===
    print_and_write(f"\n=== 被験者ごとのスコア比較 ===")
    user_scores = results_df.groupby('user_id').agg({
        'true_score': 'sum',
        'pred_score': 'sum'
    })
    user_scores['score_ratio'] = (user_scores['pred_score'] / user_scores['true_score'] * 100).round(1)
    user_scores = user_scores.sort_values('score_ratio', ascending=False)
    
    print_and_write("\nUser ID    真スコア  予測スコア  比率")
    print_and_write("-" * 45)
    for user_id, row in user_scores.iterrows():
        print_and_write(f"{user_id:10s} {int(row['true_score']):4d}      {int(row['pred_score']):4d}      {row['score_ratio']:5.1f}%")
    
    # 特徴量の重要度
    model.fit(X, y_encoded)
    feature_importance = pd.DataFrame({
        'feature': X.columns,
        'importance': model.feature_importances_
    }).sort_values('importance', ascending=False)
    
    print_and_write("\n=== 特徴量の重要度（上位10） ===")
    print_and_write(feature_importance.head(10).to_string(index=False))
    
    # 結果をCSVに保存
    results_df.to_csv('ml_predictions.csv', index=False, encoding='utf-8-sig')
    feature_importance.to_csv('feature_importance.csv', index=False, encoding='utf-8-sig')
    user_stats.to_csv('ml_user_accuracy.csv', encoding='utf-8-sig')
    user_scores.to_csv('ml_user_scores.csv', encoding='utf-8-sig')
    
    print_and_write("\n予測結果を ml_predictions.csv に保存しました")
    print_and_write("特徴量の重要度を feature_importance.csv に保存しました")
    print_and_write("被験者ごとの一致率を ml_user_accuracy.csv に保存しました")
    print_and_write("被験者ごとのスコアを ml_user_scores.csv に保存しました")
    print_and_write("分析レポートを ml_report.txt に保存しました")
    
    report_file.close()


if __name__ == "__main__":
    train_and_evaluate()
