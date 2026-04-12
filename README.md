# Portfolio
# 🔍 Identifying High-Risk Ethereum Accounts Through Multimethod Outlier Detection

**Course:** STAT 4893W | **Professor:** Georgia Huang | **Date:** November 2025 | **Language:** R

---

## 📌 Overview

This project applies and compares four outlier detection techniques to identify potentially fraudulent accounts based on Ethereum transaction data. The original dataset contains 4,681 accounts and 50 variables. After removing features with extensive missing values, 2,029 observations and 10 key variables were used for analysis.

---

## 🛠️ Methods

| Method | Description | Threshold |
|--------|-------------|-----------|
| **Isolation Forest** | Multivariate anomaly detection based on the number of random splits needed to isolate each account | 98th percentile (contamination = 0.02) |
| **Z-score** | Univariate detection by standardizing each variable using mean and standard deviation | \|Z\| > 3 |
| **IQR** | Non-parametric detection using quartile-based bounds | X < Q1 − 1.5·IQR or X > Q3 + 1.5·IQR |
| **LASSO Residual** | Regression-based detection classifying accounts with abnormally large residuals as outliers | 98th percentile (contamination = 0.02) |

---

## 📊 Results

| Method | Outliers Detected | Normal Accounts |
|--------|-------------------|-----------------|
| Isolation Forest | 42 | 1,987 |
| Z-score | 152 | 1,877 |
| IQR | 806 | 1,223 |
| LASSO Residual | 41 | 1,988 |

**Isolation Forest** anomaly scores showed the strongest correlation with `total_transactions` (r = 0.876), followed by `Received_Tnx` (r = 0.706) and `Sent_tnx` (r = 0.645), indicating that accounts with high transaction activity and wide address networks are flagged as anomalies.

**LASSO** selected only `total_ether_received` (+0.94) and `total_ether_balance` (−0.98) as significant predictors, suggesting that normal account behavior is largely explained by a linear relationship between fund inflows and balance.

---

## 💡 Key Findings

- **IQR and Z-score** are highly sensitive to extreme values in heavy-tailed distributions, leading to a high false positive rate — flagging 806 and 152 accounts respectively
- **Isolation Forest** captures structurally abnormal accounts by considering multivariate combinations of transaction frequency, network size, and address diversity — aligning with FATF fraud risk indicators
- **LASSO** complements Isolation Forest by detecting accounts with irregular fund flow relationships, regardless of transaction volume or network size
- **Using Isolation Forest and LASSO in parallel is the most effective approach** for Ethereum fraud detection

---

## 💼 Business Implications

| Finding | Business Takeaway |
|--------|-------------------|
| IQR & Z-score produce 800+ false positives | Relying on simple threshold rules would overwhelm compliance teams with unnecessary investigations, increasing operational cost |
| Isolation Forest flags accounts with high tx frequency & wide networks | These behavioral signals map directly to FATF red flags — making the model actionable for AML teams without additional tuning |
| LASSO identifies irregular fund flow accounts | Catches a different class of suspicious accounts (e.g. structuring or layering behavior) that activity-based models miss |
| Isolation Forest + LASSO combined detect 83 unique high-risk accounts | A dual-model pipeline reduces false negatives while keeping the review queue manageable for compliance analysts |

> **Bottom line:** A combined Isolation Forest + LASSO pipeline offers the best balance between detection coverage and operational efficiency — making it suitable for integration into real-world AML or fraud monitoring systems.

---

## 🔧 Tech Stack

```r
Language  : R
Libraries : isotree, glmnet, ggplot2, dplyr, tidyr, e1071, readr
Dataset   : Detection of Illicit Accounts over the Ethereum Blockchain
            Farrugia et al. (2021), Harvard Dataverse
            https://doi.org/10.34894/GKAQYN
```
