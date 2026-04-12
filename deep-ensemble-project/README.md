# 🧠 Uncertainty Estimation in Neural Networks — Deep Ensemble on MNIST

**Project:** UQ for Astronomical Time-Series Classification (PLAsTiCC) — Ensemble Workstream
**Role:** Soochun An (Statistics, UROP) | **Language:** Python (PyTorch)

---

## 📌 Overview

This project implements and compares three uncertainty quantification (UQ) strategies — **Baseline CNN**, **Evidential Deep Learning (EDL)**, and **Deep Ensemble** — for multiclass image classification on MNIST as a prototype testbed. The ultimate goal is to apply ensemble-based UQ to PLAsTiCC astronomical light-curve data for transient classification and out-of-distribution (OOD) detection.

---

## 🎯 Research Context

As part of a broader UQ research project, this workstream focuses on **Deep Ensemble methods** — training M independent CNN models with different random seeds and aggregating their predictions to produce well-calibrated probabilistic outputs. The ensemble approach is benchmarked against a single Baseline CNN and EDL to evaluate:

- Predictive accuracy and generalization
- Uncertainty estimation quality (predictive entropy, mutual information)
- Robustness to input perturbations (Gaussian noise, rotation)

---

## 🛠️ Methods

### Model Architectures

All three methods share the same **BaselineCNN** backbone:
- 3 convolutional blocks (Conv2d → BatchNorm → ReLU → Pooling)
- Fully connected classifier head
- Trained with cross-entropy loss

| Method | Description |
|--------|-------------|
| **Baseline CNN** | Single deterministic network; softmax confidence as uncertainty proxy |
| **EDL** | Evidential Deep Learning; models a Dirichlet distribution over class probabilities to separate aleatoric and epistemic uncertainty |
| **Deep Ensemble** | M = 5 independent CNNs trained with different random seeds; uncertainty estimated from prediction disagreement across members |

### Uncertainty Metrics (Ensemble)

| Metric | Description |
|--------|-------------|
| **Predictive Entropy** | Entropy of the averaged ensemble prediction — total uncertainty |
| **Expected Entropy** | Average of per-member entropies — aleatoric uncertainty |
| **Mutual Information** | Predictive entropy − Expected entropy — epistemic uncertainty proxy |
| **Probability Variance** | Variance of class probabilities across ensemble members |

---

## 📊 Results

### Training Performance

| Model | Train Accuracy | Validation Accuracy | Generalization |
|-------|---------------|---------------------|----------------|
| Baseline CNN | ~99% | Unstable fluctuations | ❌ Overfitting |
| EDL | Gradually increases | Loss diverges | ❌ Convergence failure |
| **Deep Ensemble** | High | **Stably converges** | ✅ Best generalization |

### Noise Experiment (Gaussian Noise σ: 0.0 → 0.15)

- Noise ↑ → Confidence ↓ (as expected)
- Noise ↑ → Predictive Entropy ↑
- **Deep Ensemble responded most sensitively and consistently** → most reliable uncertainty signal

### Rotation Experiment (0° → 90°)

- Rotation angle ↑ → Confidence ↓
- Rotation angle ↑ → Uncertainty ↑
- **Ensemble showed the most consistent uncertainty response** across all rotation levels

---

## 💡 Key Findings

- **Baseline CNN** achieved high training accuracy but suffered from overconfidence and overfitting — typical of deterministic networks without regularization
- **EDL** showed training instability and loss divergence, making it difficult to apply in practice without extensive hyperparameter tuning
- **Deep Ensemble** outperformed both alternatives across all evaluation dimensions: accuracy, calibration, and uncertainty sensitivity
- Ensemble's variance reduction effect from aggregating M = 5 independent models significantly reduced overfitting compared to a single network

---

## 🔬 Connection to PLAsTiCC (Next Steps)

This MNIST prototype validates the ensemble pipeline before applying it to the main research task:

- Replace MNIST with PLAsTiCC light-curve features (feature-based MLP input)
- Hold out OOD classes (TDE, KNe, rare transients) and evaluate OOD detection via predictive entropy and energy scores
- Stratify OOD into rare classes vs degraded light curves and measure anomaly detection performance
- Explore scalable ensemble variants: **BatchEnsemble**, **MIMO**

---

## 🔧 Tech Stack

```python
Language  : Python
Framework : PyTorch
Libraries : torchvision, numpy, matplotlib, tqdm
Dataset   : MNIST (prototype) → PLAsTiCC light curves (target)
Ensemble  : M = 5 independent CNNs, seeds starting from 1000
```

---

## 📁 Code Structure

```
Minist_templete_copy.py
├── CFG                          # Hyperparameter config (dataclass)
├── BaselineCNN                  # Shared CNN backbone
├── fit_baseline()               # Train single deterministic model
├── fit_edl()                    # Train Evidential Deep Learning model
├── fit_ensemble()               # Train M independent ensemble members
├── ensemble_predict()           # Aggregate predictions + compute uncertainty metrics
├── run_multi_example_perturbation_ensemble()  # Noise & rotation robustness tests
└── main()                       # CLI entry point (--method baseline/edl/ensemble/all)
```
