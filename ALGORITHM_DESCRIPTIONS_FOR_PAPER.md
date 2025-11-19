## 1. 平均値ベースのアルゴリズム（Average Value Method）

1日に記録されたすべての感情記録の Valence 値の算術平均を用いて、その日全体の感情カテゴリを推定するアルゴリズムである。  
1日に得られた Valence の列を $\{v_1, v_2, \ldots, v_n\}$ とすると、その日の平均 Valence $\bar{v}$ は

\[
\bar{v} = \frac{1}{n} \sum_{i=1}^{n} v_i
\]

と定義される。ここで $n$ はその日に記録されたデータ数である。  
得られた $\bar{v}$ を事前に定めたしきい値に基づいて 3 値の感情カテゴリに写像する。

\[
\text{Emotion} =
\begin{cases}
\text{Negative} & (\bar{v} \le 4.5)\\[4pt]
\text{Neutral}  & (4.5 < \bar{v} \le 6.0)\\[4pt]
\text{Positive} & (\bar{v} > 6.0)
\end{cases}
\]


## 2. 最頻値ベースのアルゴリズム（Most Frequent Method）

1日の中で最も頻繁に出現した感情カテゴリを、その日の代表的な感情として採用するアルゴリズムである。  
まず，各 Valence 値 $v$ を以下の 6 クラスタに離散化する。

\[
\text{Cluster}(v) =
\begin{cases}
\text{1-強いネガティブ} & (v \le 3.5)\\
\text{2-弱いネガティブ} & (3.5 < v \le 4.5)\\
\text{3-ネガティブ寄り中立} & (4.5 < v \le 5.2)\\
\text{4-ポジティブ寄り中立} & (5.2 < v \le 6.0)\\
\text{5-弱いポジティブ} & (6.0 < v \le 7.6)\\
\text{6-強いポジティブ} & (v > 7.6)
\end{cases}
\]

次に，各クラスタを 3 値の感情カテゴリに写像する。

\[
\text{Category}(\text{cluster}) =
\begin{cases}
\text{Negative} & (\text{cluster 名に「ネガティブ」を含む})\\
\text{Positive} & (\text{cluster 名に「ポジティブ」を含む})\\
\text{Neutral}  & (\text{それ以外})
\end{cases}
\]

1日の全記録に対してこの写像を適用し，得られたカテゴリ列 $\{c_1, c_2, \ldots, c_n\}$ の出現回数を集計する。

\[
C_{\text{neg}} = \sum_{i=1}^{n} \mathbb{1}[c_i = \text{Negative}],\quad
C_{\text{neu}} = \sum_{i=1}^{n} \mathbb{1}[c_i = \text{Neutral}],\quad
C_{\text{pos}} = \sum_{i=1}^{n} \mathbb{1}[c_i = \text{Positive}]
\]

ここで $\mathbb{1}[\cdot]$ は指示関数である。  
最終的な日次感情カテゴリは，最頻カテゴリとして

\[
\text{Emotion} = \arg\max \bigl(C_{\text{pos}},\, C_{\text{neg}},\, C_{\text{neu}}\bigr)
\]

により決定する。同数のカテゴリが存在する場合は，Positive，Negative，Neutral の順に優先して選択する。


## 3. 最新値ベースのアルゴリズム（Latest Value Method）

その日に記録された感情のうち，時刻が最も遅い記録の Valence 値のみを用いて，日全体の感情カテゴリを推定するアルゴリズムである。  
1日の各記録に対応する時刻を $\{t_1, t_2, \ldots, t_n\}$，対応する Valence 値を $\{v_1, v_2, \ldots, v_n\}$ とすると，最終記録は

\[
t_k = \max(t_1, t_2, \ldots, t_n),\quad v_{\text{last}} = v_k
\]

として得られる。  
この $v_{\text{last}}$ を用いて，平均値ベースのアルゴリズムと同一のしきい値により感情カテゴリを判定する。

\[
\text{Emotion} =
\begin{cases}
\text{Negative} & (v_{\text{last}} \le 4.5)\\[4pt]
\text{Neutral}  & (4.5 < v_{\text{last}} \le 6.0)\\[4pt]
\text{Positive} & (v_{\text{last}} > 6.0)
\end{cases}
\]
