
# 🎬 Recommending Movies using Duration, Rating, and Year

**Course:** STAT 4051 | **Team:** Heather Yuan, Soochun An, Vivienne Zhang | **Language:** R

---

## 📌 Overview

This project applies and compares three unsupervised clustering algorithms — K-means, Hierarchical Clustering, and GMM — to group movies based on duration and release year, then evaluates each cluster's average rating to build a data-driven movie recommendation framework. The dataset contains 10,000 IMDB movies, reduced to 8,800 after removing missing values and outliers.

---

## 💼 Business Context

The U.S. and Canada box office generated **$8.56 billion** in revenue, and Netflix alone generates **$39 billion** annually — much of which is driven by personalized recommendation systems. Effective movie recommendations increase user engagement, reduce churn, and directly impact platform revenue. This project explores how unsupervised clustering on basic movie features can serve as a lightweight, scalable foundation for recommendation logic — particularly useful when user behavioral data is limited or unavailable.

**Real-world applications include:**
- **Cold-start recommendations:** Suggest movies to new users with no watch history based on cluster patterns
- **Content cataloging:** Automatically segment a movie library into meaningful groups for editorial curation
- **Preference matching:** Match users to clusters based on their previously rated movies to surface similar titles
- **A/B testing baseline:** Use cluster-based recommendations as a control group to benchmark against advanced collaborative filtering models

---

## 📊 Dataset

- **Source:** [IMDB Movies Dataset — Kaggle](https://www.kaggle.com/datasets/amanbarthwal/imdb-movies-data)
- **Original size:** 10,000 movies, 16 variables
- **After NA removal:** 9,447 movies
- **After IQR outlier removal:** 8,800 movies

**Clustering variables:**

| Variable | Description |
|----------|-------------|
| `Year` | Release year of the movie |
| `Duration` | Length of the movie in minutes |

**Evaluation variable:**

| Variable | Description |
|----------|-------------|
| `Rating` | IMDB rating on a scale of 1–10 (used to interpret clusters, not for clustering) |

---

## 🛠️ Methods

| Method | Description |
|--------|-------------|
| **K-means** | Partitions movies into k clusters by minimizing within-cluster sum of squares; elbow plot used to select k = 3 |
| **Hierarchical Clustering** | Builds a dendrogram using Euclidean distance; complete linkage selected after comparing single, complete, and average linkage |
| **GMM (Gaussian Mixture Model)** | Probabilistic clustering that automatically identified 9 optimal clusters; also run with G = 3 for comparison |

All variables were standardized using `scale()` prior to clustering to prevent scale bias between Year and Duration.

---

## 📈 Results

### K-means (k = 3)

| Cluster | Avg Year | Avg Duration (min) | Avg Rating | Count |
|---------|----------|--------------------|------------|-------|
| 1 | 2016 | 97.0 | 6.19 | 4,314 |
| 2 | 1986 | 102.7 | 6.28 | 2,032 |
| 3 | 2012 | 125.8 | **6.90** | 2,454 |

→ **Cluster 3** (longer, moderately recent movies) had the highest average rating

### Hierarchical Clustering — Complete Linkage (k = 3)

| Cluster | Avg Year | Avg Duration (min) | Avg Rating | Count |
|---------|----------|--------------------|------------|-------|
| 1 | 2015 | 122.7 | **6.82** | 2,723 |
| 2 | 2010 | 93.8 | 6.09 | 4,288 |
| 3 | 1992 | 111.6 | 6.53 | 1,789 |

→ **Cluster 1** (recent, long-duration movies) had the highest average rating

### GMM (G = 3)

| Cluster | Avg Year | Avg Duration (min) | Avg Rating | Count |
|---------|----------|--------------------|------------|-------|
| 1 | 2009 | 98.2 | 6.20 | 3,212 |
| 2 | 2022 | 106.4 | 6.42 | 2,635 |
| 3 | 1993 | 115.2 | **6.62** | 2,953 |

→ **Cluster 3** (older, longer movies) had the highest average rating

---

## 💡 Key Findings

- Across all three methods, the **highest-rated cluster consistently contained movies with durations of 123–126 minutes**, suggesting that mid-to-long duration films tend to receive higher audience ratings
- **K-means and Hierarchical Clustering** produced the clearest and most balanced cluster separations; GMM's strength emerged when using its optimal 9-cluster solution
- **Highly rated movies** (avg rating 6.82–6.90) were concentrated in clusters with release years mostly after 2012 and durations around 120+ minutes
- Top-rated movies appearing across multiple clustering methods included titles such as *The Shawshank Redemption*, *The Dark Knight*, *12 Angry Men*, and *Inception* — confirming cross-method consistency

---

## 💼 Business Implications

| Finding | Business Takeaway |
|---------|-------------------|
| High-rated cluster spans ~28–31% of the dataset | A manageable, well-defined segment that can serve as a "quality recommendation pool" for new users with no watch history |
| Longer duration (120+ min) correlates with higher ratings | Content platforms can use duration as a lightweight proxy signal when building initial recommendation rules |
| K-means and Hierarchical produce consistent high-rating clusters | These models are computationally efficient and interpretable — suitable for real-time recommendation pipelines at scale |
| GMM's optimal solution yields 9 clusters | Finer segmentation enables more personalized recommendations when sufficient user behavioral data is available |
| Recent movies may have fewer ratings, distorting clusters | Platforms should weight ratings by volume (e.g. Bayesian average) to avoid cold-start bias in cluster formation |

> **Bottom line:** A two-step strategy is recommended — use K-means or Hierarchical Clustering for initial segmentation based on duration and release year, then refine with GMM's 9-cluster solution as user data accumulates. Incorporating additional features such as genre, cast, and user behavior would further improve recommendation precision.

---

## ⚠️ Limitations

- Only two numerical variables (Year, Duration) were used — categorical features like genre were excluded
- Clustering is descriptive and non-predictive; patterns cannot be generalized as causal relationships
- Movies with very long durations (>150 min) were removed as outliers, limiting coverage of epic films
- Recent movies with low rating volumes may cluster inconsistently due to insufficient data

---

## 🔧 Tech Stack

```r
Language  : R
Libraries : ggplot2, dplyr, cluster, mclust, factoextra
Dataset   : IMDB Movies Dataset — Kaggle
            https://www.kaggle.com/datasets/amanbarthwal/imdb-movies-data
```
