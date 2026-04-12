
from __future__ import annotations
import os
import math
import copy
import random
import argparse
from dataclasses import dataclass
from typing import Tuple, Dict, Any, Optional

import numpy as np
import matplotlib.pyplot as plt 
import torch
import torch.nn as nn
import torch.nn.functional as F
from torch.utils.data import DataLoader, random_split
import torchvision.transforms.functional as TF 
import torchvision
import torchvision.transforms as T

try:
    from tqdm.auto import tqdm
except Exception:
    tqdm = None


# Config
@dataclass
class CFG:
    dataset: str = "mnist"         # "mnist" or "cifar10"
    data_dir: str = "./data"
    batch_size: int = 128
    num_workers: int = 2
    val_frac: float = 0.1

    epochs: int = 30
    lr: float = 1e-3
    weight_decay: float = 0.0

    # Early stopping
    use_early_stopping: bool = True
    patience: int = 3
    min_delta: float = 1e-4

    # EDL specifics
    edl_kl_coef: float = 1.0
    edl_anneal_epochs: int = 30

    # Ensemble specifics
    ensemble_size: int = 5
    ensemble_seed0: int = 1000

    # Reproducibility
    seed: int = 42


def seed_everything(seed: int = 42):
    random.seed(seed)
    np.random.seed(seed)
    torch.manual_seed(seed)
    torch.cuda.manual_seed_all(seed)  
    torch.backends.cudnn.deterministic = True
    torch.backends.cudnn.benchmark = False


def _iter(loader, desc: str):
    """tqdm wrapper fallback."""
    if tqdm is None:
        return loader
    return tqdm(loader, desc=desc, leave=False)



# Dataset
def get_dataset(cfg: CFG):
    name = cfg.dataset.lower()
    if name == "mnist":
        num_classes = 10
        in_channels = 1
        class_names = [str(i) for i in range(10)]
        image_shape = (1, 28, 28)

        tf_train = T.Compose([
            T.ToTensor(),
            T.Normalize((0.1307,), (0.3081,))
        ])
        tf_test = tf_train

        train_full = torchvision.datasets.MNIST(cfg.data_dir, train=True, download=True, transform=tf_train)
        test = torchvision.datasets.MNIST(cfg.data_dir, train=False, download=True, transform=tf_test)

        denorm = lambda x: (x * 0.3081 + 0.1307).clamp(0, 1)

    elif name == "cifar10":
        
        num_classes = 10
        in_channels = 3
        class_names = ["airplane","automobile","bird","cat","deer","dog","frog","horse","ship","truck"]
        image_shape = (3, 32, 32)

        tf_train = T.Compose([
            T.ToTensor(),
            T.Normalize((0.4914, 0.4822, 0.4465),
                        (0.2023, 0.1994, 0.2010))
        ])
        tf_test = tf_train

        train_full = torchvision.datasets.CIFAR10(cfg.data_dir, train=True, download=True, transform=tf_train)
        test = torchvision.datasets.CIFAR10(cfg.data_dir, train=False, download=True, transform=tf_test)

        denorm = lambda x: (x * torch.tensor([0.2023, 0.1994, 0.2010], device=x.device).view(3,1,1)
                            + torch.tensor([0.4914, 0.4822, 0.4465], device=x.device).view(3,1,1)).clamp(0, 1)
    else:
        raise ValueError(f"Unknown dataset: {cfg.dataset}")

   
    n_total = len(train_full)
    n_val = int(cfg.val_frac * n_total)
    n_train = n_total - n_val
    train, val = random_split(train_full, [n_train, n_val], generator=torch.Generator().manual_seed(cfg.seed))

    return train, val, test, num_classes, in_channels, class_names, image_shape, denorm

#Baseline CNN

class BaselineCNN(nn.Module):
    def __init__(self, in_channels: int, num_classes: int, width: int = 64):
        super().__init__()
        self.features = nn.Sequential(
            nn.Conv2d(in_channels, width, 3, padding=1),
            nn.BatchNorm2d(width),
            nn.ReLU(inplace=True),
            nn.MaxPool2d(2),

            nn.Conv2d(width, width*2, 3, padding=1),
            nn.BatchNorm2d(width*2),
            nn.ReLU(inplace=True),
            nn.MaxPool2d(2),

            nn.Conv2d(width*2, width*4, 3, padding=1),
            nn.BatchNorm2d(width*4),
            nn.ReLU(inplace=True),
            nn.AdaptiveAvgPool2d(1)
        )
        self.classifier = nn.Sequential(
            nn.Flatten(),
            nn.Linear(width*4, width*4),
            nn.ReLU(inplace=True),
            nn.Linear(width*4, num_classes),
        )

    def forward(self, x):
        x = self.features(x)
        x = self.classifier(x)
        return x


@torch.no_grad()
def accuracy_from_logits(logits: torch.Tensor, y: torch.Tensor) -> float:
    pred = logits.argmax(dim=1)
    return (pred == y).float().mean().item()


def train_one_epoch(model: nn.Module, loader: DataLoader, optimizer: torch.optim.Optimizer, device: torch.device):
    model.train()
    total_loss, total_acc, n = 0.0, 0.0, 0

    for xb, yb in _iter(loader, "train"):
        xb, yb = xb.to(device), yb.to(device)

        optimizer.zero_grad(set_to_none=True)
        logits = model(xb)
        loss = F.cross_entropy(logits, yb)
        loss.backward()
        optimizer.step()

        bs = xb.size(0)
        total_loss += loss.item() * bs
        total_acc  += accuracy_from_logits(logits, yb) * bs
        n += bs

    return total_loss / n, total_acc / n


@torch.no_grad()
def evaluate(model: nn.Module, loader: DataLoader, device: torch.device):
    model.eval()
    total_loss, total_acc, n = 0.0, 0.0, 0

    for xb, yb in _iter(loader, "eval"):
        xb, yb = xb.to(device), yb.to(device)
        logits = model(xb)
        loss = F.cross_entropy(logits, yb)

        bs = xb.size(0)
        total_loss += loss.item() * bs
        total_acc  += accuracy_from_logits(logits, yb) * bs
        n += bs

    return total_loss / n, total_acc / n

class EarlyStopping:
    def __init__(self, patience=3, min_delta=1e-4):
        self.patience = patience
        self.min_delta = min_delta
        self.best_loss = float("inf")
        self.counter = 0
        self.best_state = None
        self.early_stop = False

    def step(self, val_loss, model):
        import copy
        if val_loss < self.best_loss - self.min_delta:
            self.best_loss = val_loss
            self.counter = 0
            self.best_state = copy.deepcopy(model.state_dict())
        else:
            self.counter += 1
            if self.counter >= self.patience:
                self.early_stop = True


@torch.no_grad()
def baseline_predict(model: nn.Module, loader: DataLoader, device: torch.device):
    model.eval()
    all_probs, all_pred, all_y, all_conf = [], [], [], []

    for xb, yb in _iter(loader, "predict-baseline"):
        xb = xb.to(device)
        logits = model(xb)
        probs = F.softmax(logits, dim=1)

        conf, pred = probs.max(dim=1)

        all_probs.append(probs.cpu())
        all_pred.append(pred.cpu())
        all_y.append(yb.cpu())
        all_conf.append(conf.cpu())

    return (
        torch.cat(all_probs, dim=0),
        torch.cat(all_pred, dim=0),
        torch.cat(all_y, dim=0),
        torch.cat(all_conf, dim=0),
    )

def fit_baseline(cfg: CFG, train_loader: DataLoader, val_loader: DataLoader,
                 C: int, K: int, device: torch.device):
    seed_everything(cfg.seed)
    model = BaselineCNN(C, K).to(device)
    optimizer = torch.optim.Adam(model.parameters(), lr=cfg.lr, weight_decay=cfg.weight_decay)

    stopper = EarlyStopping(patience=cfg.patience, min_delta=cfg.min_delta)

    history = {
        "train_loss": [],
        "val_loss": [],
        "train_acc": [],
        "val_acc": []
    }

    for epoch in range(cfg.epochs):
        tr_loss, tr_acc = train_one_epoch(model, train_loader, optimizer, device)
        va_loss, va_acc = evaluate(model, val_loader, device)

        history["train_loss"].append(tr_loss)
        history["val_loss"].append(va_loss)
        history["train_acc"].append(tr_acc)
        history["val_acc"].append(va_acc)

        print(f"[baseline] epoch {epoch+1:02d}/{cfg.epochs}  "
              f"train_loss={tr_loss:.4f} train_acc={tr_acc:.4f}  "
              f"val_loss={va_loss:.4f} val_acc={va_acc:.4f}")

        if cfg.use_early_stopping:
            stopper.step(va_loss, model)

            if stopper.early_stop:
                print(f"[baseline] Early stopping triggered at epoch {epoch+1}")
                break

    if cfg.use_early_stopping and stopper.best_state is not None:
        model.load_state_dict(stopper.best_state)

    return model, history



# EDL
class EDLHead(nn.Module):
    """
    backbone outputs [B, K] unconstrained scores.
    Convert scores -> evidence >= 0 -> alpha = evidence + 1.
    """
    def __init__(self, backbone: nn.Module, num_classes: int):
        super().__init__()
        self.backbone = backbone
        self.K = num_classes

    def forward(self, x):
        z = self.backbone(x)      # [B,K]
        evidence = F.softplus(z)  # [B,K] >= 0
        alpha = evidence + 1.0    # [B,K] >= 1
        return alpha


def dirichlet_mean(alpha: torch.Tensor) -> torch.Tensor:
    S = alpha.sum(dim=1, keepdim=True).clamp(min=1e-12)
    return alpha / S


def edl_total_uncertainty(alpha: torch.Tensor, num_classes: int) -> torch.Tensor:
    S = alpha.sum(dim=1).clamp(min=1e-12)
    return num_classes / S


def edl_data_fit_term(alpha: torch.Tensor, y: torch.Tensor) -> torch.Tensor:
    """
    Per-sample expected negative log likelihood under Dirichlet:
      L_data = ψ(S) - ψ(alpha_y)
    """
    S = alpha.sum(dim=1).clamp(min=1e-12)                      
    alpha_y = alpha.gather(1, y.view(-1, 1)).squeeze(1)        
    return torch.digamma(S) - torch.digamma(alpha_y)


def edl_kl_regularizer(alpha: torch.Tensor, num_classes: int) -> torch.Tensor:
    """
    Per-sample KL( Dir(alpha) || Dir(1) ) where Dir(1) is uniform prior.
    KL must be >= 0. Use:
      KL = ln B(beta) - ln B(alpha) + sum_i (alpha_i - beta_i)(psi(alpha_i) - psi(S_alpha))
    """
    beta = torch.ones((1, num_classes), device=alpha.device, dtype=alpha.dtype) 
    beta = beta.expand(alpha.size(0), -1)  

    S_alpha = alpha.sum(dim=1, keepdim=True).clamp(min=1e-12)  
    S_beta  = beta.sum(dim=1, keepdim=True)                    

    logB_alpha = torch.lgamma(alpha).sum(dim=1, keepdim=True) - torch.lgamma(S_alpha)
    logB_beta  = torch.lgamma(beta).sum(dim=1, keepdim=True)  - torch.lgamma(S_beta)

    digamma_alpha = torch.digamma(alpha)
    digamma_S_alpha = torch.digamma(S_alpha)

    t = (alpha - beta) * (digamma_alpha - digamma_S_alpha)  


    kl = (logB_beta - logB_alpha + t.sum(dim=1, keepdim=True))  
    return kl.squeeze(1)


def edl_kl_weight(epoch: int, anneal_epochs: int) -> float:
    if anneal_epochs is None or anneal_epochs <= 0:
        return 1.0
    return float(min(1.0, (epoch + 1) / float(anneal_epochs)))


def edl_loss(alpha: torch.Tensor, y: torch.Tensor, epoch: int, num_classes: int,
             kl_coef: float = 1.0, anneal_epochs: int = 10) -> torch.Tensor:
    data_term = edl_data_fit_term(alpha, y)                         
    kl_term = edl_kl_regularizer(alpha, num_classes)                 
    w = edl_kl_weight(epoch, anneal_epochs)                         
    return (data_term + kl_coef * w * kl_term).mean()


def train_one_epoch_edl(model: EDLHead, loader: DataLoader, optimizer: torch.optim.Optimizer,
                        epoch: int, num_classes: int, device: torch.device,
                        kl_coef: float, anneal_epochs: int):
    model.train()
    total_loss, total_acc, n = 0.0, 0.0, 0

    for xb, yb in _iter(loader, "train-edl"):
        xb, yb = xb.to(device), yb.to(device)

        optimizer.zero_grad(set_to_none=True)
        alpha = model(xb)  # [B,K]
        loss = edl_loss(alpha, yb, epoch=epoch, num_classes=num_classes,
                        kl_coef=kl_coef, anneal_epochs=anneal_epochs)
        loss.backward()
        torch.nn.utils.clip_grad_norm_(model.parameters(), max_norm=5.0)
        optimizer.step()

       
        probs = dirichlet_mean(alpha)
        pred = probs.argmax(dim=1)
        acc = (pred == yb).float().mean().item()

        bs = xb.size(0)
        total_loss += loss.item() * bs
        total_acc  += acc * bs
        n += bs

    return total_loss / n, total_acc / n


@torch.no_grad()
def evaluate_edl(model: EDLHead, loader: DataLoader, device: torch.device,
                 num_classes: int, epoch: int, kl_coef: float, anneal_epochs: int):
    model.eval()
    total_loss, total_acc, n = 0.0, 0.0, 0

    for xb, yb in _iter(loader, "eval-edl"):
        xb, yb = xb.to(device), yb.to(device)
        alpha = model(xb)

        loss = edl_loss(alpha, yb, epoch=epoch, num_classes=num_classes,
                        kl_coef=kl_coef, anneal_epochs=anneal_epochs)

        probs = dirichlet_mean(alpha)
        pred = probs.argmax(dim=1)
        acc = (pred == yb).float().mean().item()

        bs = xb.size(0)
        total_loss += loss.item() * bs
        total_acc += acc * bs
        n += bs

    return total_loss / n, total_acc / n


def fit_edl(cfg: CFG, train_loader: DataLoader, val_loader: DataLoader,
            C: int, K: int, device: torch.device):
    seed_everything(cfg.seed)
    backbone = BaselineCNN(C, K).to(device)
    model = EDLHead(backbone, K).to(device)
    optimizer = torch.optim.Adam(model.parameters(), lr=cfg.lr, weight_decay=cfg.weight_decay)

    stopper = EarlyStopping(patience=cfg.patience, min_delta=cfg.min_delta)

    history = {
        "train_loss": [],
        "val_loss": [],
        "train_acc": [],
        "val_acc": []
    }

    for epoch in range(cfg.epochs):
        tr_loss, tr_acc = train_one_epoch_edl(
            model, train_loader, optimizer, epoch=epoch, num_classes=K,
            device=device, kl_coef=cfg.edl_kl_coef, anneal_epochs=cfg.edl_anneal_epochs
        )

        va_loss, va_acc = evaluate_edl(
            model, val_loader, device,
            num_classes=K, epoch=epoch,
            kl_coef=cfg.edl_kl_coef, anneal_epochs=cfg.edl_anneal_epochs
        )

        history["train_loss"].append(tr_loss)
        history["val_loss"].append(va_loss)
        history["train_acc"].append(tr_acc)
        history["val_acc"].append(va_acc)

        print(f"[edl] epoch {epoch+1:02d}/{cfg.epochs}  "
              f"train_loss={tr_loss:.4f} train_acc={tr_acc:.4f}  "
              f"val_loss={va_loss:.4f} val_acc={va_acc:.4f}")

        if cfg.use_early_stopping:
            stopper.step(va_loss, model)

            if stopper.early_stop:
                print(f"[edl] Early stopping triggered at epoch {epoch+1}")
                break

    if cfg.use_early_stopping and stopper.best_state is not None:
        model.load_state_dict(stopper.best_state)

    return model, history

@torch.no_grad()
def edl_predict(model: EDLHead, loader: DataLoader, device: torch.device, num_classes: int):
    model.eval()
    all_probs, all_pred, all_y, all_u = [], [], [], []

    for xb, yb in _iter(loader, "predict-edl"):
        xb = xb.to(device)
        alpha = model(xb)
        probs = dirichlet_mean(alpha)                         
        pred = probs.argmax(dim=1).detach().cpu()              
        u = edl_total_uncertainty(alpha, num_classes).detach().cpu()  
        all_probs.append(probs.detach().cpu())
        all_pred.append(pred)
        all_y.append(yb.detach().cpu())
        all_u.append(u)

    return (torch.cat(all_probs, dim=0),
            torch.cat(all_pred, dim=0),
            torch.cat(all_y, dim=0),
            torch.cat(all_u, dim=0))


# Deep Ensembles

@torch.no_grad()
def predictive_entropy(probs: torch.Tensor, eps: float = 1e-12) -> torch.Tensor:
    # probs: [N,K]
    p = probs.clamp(min=eps)
    return -(p * p.log()).sum(dim=1)


@torch.no_grad()
def ensemble_predict(models: list[nn.Module], loader: DataLoader, device: torch.device) -> Dict[str, torch.Tensor]:
    """
    Returns:
      mean_probs: [N,K]
      pred: [N]
      y: [N]
      entropy: [N]          (predictive entropy of mean probs)
      mi: [N]               (mutual information proxy: H[mean] - mean(H[p_m]))
      prob_var: [N]         (mean variance across classes)
    """
    for m in models:
        m.eval()

    all_mean_probs, all_pred, all_y = [], [], []
    all_entropy, all_mi, all_pvar = [], [], []

    for xb, yb in _iter(loader, "predict-ens"):
        xb = xb.to(device)

        member_probs = []
        member_ent = []
        for m in models:
            logits = m(xb)
            p = F.softmax(logits, dim=1)     
            member_probs.append(p)
            member_ent.append(predictive_entropy(p))  

        P = torch.stack(member_probs, dim=0)          
        mean_p = P.mean(dim=0)                        

        ent_mean = predictive_entropy(mean_p)         
        ent_members = torch.stack(member_ent, dim=0).mean(dim=0)  
        mi = ent_mean - ent_members                  

        pvar = P.var(dim=0).mean(dim=1)              

        pred = mean_p.argmax(dim=1).detach().cpu()

        all_mean_probs.append(mean_p.detach().cpu())
        all_pred.append(pred)
        all_y.append(yb.detach().cpu())
        all_entropy.append(ent_mean.detach().cpu())
        all_mi.append(mi.detach().cpu())
        all_pvar.append(pvar.detach().cpu())

    return {
        "mean_probs": torch.cat(all_mean_probs, dim=0),
        "pred": torch.cat(all_pred, dim=0),
        "y": torch.cat(all_y, dim=0),
        "entropy": torch.cat(all_entropy, dim=0),
        "mi": torch.cat(all_mi, dim=0),
        "prob_var": torch.cat(all_pvar, dim=0),
    }

@torch.no_grad()
def ensemble_predict_single(models, x_single, device):
    """
    single sample용 ensemble prediction
    returns:
      - mean_probs
      - pred
      - confidence
      - uncertainty (predictive entropy)
      - mi (mutual information)
      - prob_var (mean variance across class probabilities)
    """
    probs = []

    for m in models:
        m.eval()
        logits = m(x_single.to(device))        
        p = F.softmax(logits, dim=1)           
        probs.append(p)

    P = torch.stack(probs, dim=0)               
    mean_p = P.mean(dim=0).squeeze(0)           

    confidence = mean_p.max().item()
    pred = mean_p.argmax().item()

    # predictive entropy = total uncertainty
    entropy = -(mean_p * mean_p.clamp_min(1e-12).log()).sum()

    # average member entropy
    member_entropy = -(P * P.clamp_min(1e-12).log()).sum(dim=2).mean()

    # mutual information = epistemic uncertainty
    mi = entropy - member_entropy

    # variance across ensemble members' probabilities
    prob_var = P.squeeze(1).var(dim=0).mean()

    return {
        "mean_probs": mean_p.detach().cpu(),
        "pred": pred,
        "confidence": confidence,
        "uncertainty": entropy.item(),
        "mi": mi.item(),
        "prob_var": prob_var.item(),
    }

def add_gaussian_noise_normalized(x: torch.Tensor, sigma: float, denorm=None, base_noise=None):
    """
    x: normalized tensor [1, C, H, W]
    sigma: noise scale in normalized space
    base_noise: fixed noise tensor [1, C, H, W]
    """
    if base_noise is None:
        base_noise = torch.randn_like(x)

    x_noisy = x + sigma * base_noise
    return x_noisy


def rotate_normalized_image(x: torch.Tensor, angle: float, denorm):
    """
    x: normalized tensor [1, C, H, W]
    rotate in image space, then renormalize
    """
    x_img = denorm(x.squeeze(0))                         
    x_rot_img = TF.rotate(x_img, angle=angle, fill=0)
    x_rot_img = x_rot_img.clamp(0.0, 1.0).unsqueeze(0)  

    if x.shape[1] == 1:
        mean = torch.tensor([0.1307], device=x.device).view(1,1,1,1)
        std  = torch.tensor([0.3081], device=x.device).view(1,1,1,1)
    else:
        mean = torch.tensor([0.4914, 0.4822, 0.4465], device=x.device).view(1,3,1,1)
        std  = torch.tensor([0.2023, 0.1994, 0.2010], device=x.device).view(1,3,1,1)

    x_rot = (x_rot_img.to(x.device) - mean) / std
    return x_rot


@torch.no_grad()
def find_correct_test_example_ensemble(models, dataset, device, max_tries=500):
    """
    correctly classified sample 하나 찾기
    """
    idxs = torch.randperm(len(dataset))[:max_tries].tolist()

    for idx in idxs:
        x, y = dataset[idx]
        x_in = x.unsqueeze(0).to(device)
        out = ensemble_predict_single(models, x_in, device)
        if out["pred"] == y:
            return idx, x, y

    # fallback
    idx = 0
    x, y = dataset[idx]
    return idx, x, y


@torch.no_grad()
def find_multiple_correct_test_examples_ensemble(models, dataset, device, n_examples=5, max_tries=2000):
    """
    correctly classified sample 여러 개 찾기
    returns: list of (idx, x, y)
    """
    found = []
    used = set()

    idxs = torch.randperm(len(dataset))[:max_tries].tolist()

    for idx in idxs:
        if idx in used:
            continue

        x, y = dataset[idx]
        x_in = x.unsqueeze(0).to(device)
        out = ensemble_predict_single(models, x_in, device)

        if out["pred"] == y:
            found.append((idx, x, y))
            used.add(idx)

        if len(found) >= n_examples:
            break

    if len(found) == 0:
        idx = 0
        x, y = dataset[idx]
        found.append((idx, x, y))

    return found


def run_multi_example_perturbation_ensemble(models, test_set, device, denorm, class_names,
                                            n_examples=5,
                                            noise_levels=np.linspace(0.0, 0.15, 7),
                                            angles=np.arange(0, 91, 10),
                                            save_prefix="ensemble_multi"):
    """
    여러 correctly classified test image에 대해 perturbation curve를 반복 실행
    각 샘플마다 별도 png 저장
    """
    examples = find_multiple_correct_test_examples_ensemble(
        models, test_set, device, n_examples=n_examples
    )

    print(f"[ensemble] found {len(examples)} correctly classified examples for multi-sample perturbation test")

    for j, (idx, x, y) in enumerate(examples, start=1):
        x0 = x.unsqueeze(0).to(device)

        # sample별 fixed noise
        fixed_noise = torch.randn_like(x0)

        print(f"[ensemble][sample {j}] idx={idx}, true class={class_names[y]}")

        noise_results = run_noise_curve_ensemble(
            models, x0, device, denorm, noise_levels, base_noise=fixed_noise
        )

        rot_results = run_rotation_curve_ensemble(
            models, x0, device, denorm, angles
        )

        sample_prefix = f"{save_prefix}_sample{j}_idx{idx}"

        plot_perturbation_results(
            noise_results,
            title_prefix=f"Deep Ensemble Noise (sample {j}, true={class_names[y]})",
            level_name="Noise Level",
            save_prefix=f"{sample_prefix}_noise"
        )

        plot_perturbation_results(
            rot_results,
            title_prefix=f"Deep Ensemble Rotation (sample {j}, true={class_names[y]})",
            level_name="Rotation Angle",
            save_prefix=f"{sample_prefix}_rotation"
        )

        save_perturbed_examples(
            models, x0, y, class_names, device, denorm,
            noise_levels=(0.0, 0.03, 0.06, 0.09, 0.12, 0.15),
            angles=(0, 15, 30, 45, 60),
            save_prefix=sample_prefix,
            noise_base=fixed_noise
        )
@torch.no_grad()
def run_noise_curve_ensemble(models, x0, device, denorm, noise_levels, base_noise=None):
    results = []

    for sigma in noise_levels:
        x_pert = add_gaussian_noise_normalized(
            x0.clone(), float(sigma), denorm, base_noise=base_noise
        )
        out = ensemble_predict_single(models, x_pert, device)

        results.append({
            "level": float(sigma),
            "confidence": out["confidence"],
            "uncertainty": out["uncertainty"],
            "mi": out["mi"],
            "prob_var": out["prob_var"],
            "pred": out["pred"]
        })

    return results

@torch.no_grad()
def run_rotation_curve_ensemble(models, x0, device, denorm, angles):
    results = []

    for angle in angles:
        x_pert = rotate_normalized_image(x0.clone(), float(angle), denorm)
        out = ensemble_predict_single(models, x_pert, device)

        results.append({
            "level": float(angle),
            "confidence": out["confidence"],
            "uncertainty": out["uncertainty"],
            "mi": out["mi"],
            "prob_var": out["prob_var"],
            "pred": out["pred"]
        })

    return results


def plot_perturbation_results(results, title_prefix, level_name, save_prefix):
    levels = [r["level"] for r in results]
    confs  = [r["confidence"] for r in results]
    uncs   = [r["uncertainty"] for r in results]
    mis    = [r["mi"] for r in results]

    # 1) x = uncertainty, y = confidence
    plt.figure(figsize=(6, 5))
    plt.plot(uncs, confs, marker="o")
    for x, y, lv in zip(uncs, confs, levels):
        plt.annotate(f"{lv:g}", (x, y), fontsize=8, xytext=(4,4), textcoords="offset points")
    plt.xlabel("Uncertainty (Predictive Entropy)")
    plt.ylabel("Confidence")
    plt.title(f"{title_prefix}: Confidence vs Uncertainty")
    plt.grid(True, alpha=0.3)
    plt.tight_layout()
    plt.savefig(f"{save_prefix}_conf_vs_unc.png")
    plt.close()

    # 2) level vs confidence
    plt.figure(figsize=(6, 4))
    plt.plot(levels, confs, marker="o")
    plt.xlabel(level_name)
    plt.ylabel("Confidence")
    plt.title(f"{title_prefix}: {level_name} vs Confidence")
    plt.grid(True, alpha=0.3)
    plt.tight_layout()
    plt.savefig(f"{save_prefix}_{level_name.lower().replace(' ', '_')}_vs_conf.png")
    plt.close()

    # 3) level vs uncertainty
    plt.figure(figsize=(6, 4))
    plt.plot(levels, uncs, marker="o")
    plt.xlabel(level_name)
    plt.ylabel("Uncertainty (Predictive Entropy)")
    plt.title(f"{title_prefix}: {level_name} vs Uncertainty")
    plt.grid(True, alpha=0.3)
    plt.tight_layout()
    plt.savefig(f"{save_prefix}_{level_name.lower().replace(' ', '_')}_vs_unc.png")
    plt.close()

    # 4) level vs MI
    plt.figure(figsize=(6, 4))
    plt.plot(levels, mis, marker="o")
    plt.xlabel(level_name)
    plt.ylabel("MI (Mutual Information)")
    plt.title(f"{title_prefix}: {level_name} vs MI")
    plt.grid(True, alpha=0.3)
    plt.tight_layout()
    plt.savefig(f"{save_prefix}_{level_name.lower().replace(' ', '_')}_vs_mi.png")
    plt.close()

def save_perturbed_examples(models, x0, y0, class_names, device, denorm,
                            noise_levels=(0.0, 0.1, 0.2, 0.3, 0.4),
                            angles=(0, 15, 30, 45, 60),
                            save_prefix="ensemble",
                            noise_base=None):
    # noise strip
    imgs = []
    titles = []
    for sigma in noise_levels:
        x_pert = add_gaussian_noise_normalized(
            x0.clone(), float(sigma), denorm, base_noise=noise_base
            )
        out = ensemble_predict_single(models, x_pert, device)
        imgs.append(denorm(x_pert.squeeze(0)).cpu())
        titles.append(f"N={sigma:.2f}\nC={out['confidence']:.2f}\nU={out['uncertainty']:.2f}")

    plt.figure(figsize=(3 * len(imgs), 3))
    for i, (img, title) in enumerate(zip(imgs, titles)):
        plt.subplot(1, len(imgs), i + 1)
        if img.shape[0] == 1:
            plt.imshow(img.squeeze(0).numpy(), cmap="gray")
        else:
            plt.imshow(img.permute(1, 2, 0).numpy())
        plt.title(title)
        plt.axis("off")
    plt.tight_layout()
    plt.savefig(f"{save_prefix}_noise_examples.png")
    plt.close()

    # rotation strip
    imgs = []
    titles = []
    for angle in angles:
        x_pert = rotate_normalized_image(x0.clone(), float(angle), denorm)
        out = ensemble_predict_single(models, x_pert, device)
        imgs.append(denorm(x_pert.squeeze(0)).cpu())
        titles.append(f"A={angle:.0f}\nC={out['confidence']:.2f}\nU={out['uncertainty']:.2f}")

    plt.figure(figsize=(3 * len(imgs), 3))
    for i, (img, title) in enumerate(zip(imgs, titles)):
        plt.subplot(1, len(imgs), i + 1)
        if img.shape[0] == 1:
            plt.imshow(img.squeeze(0).numpy(), cmap="gray")
        else:
            plt.imshow(img.permute(1, 2, 0).numpy())
        plt.title(title)
        plt.axis("off")
    plt.tight_layout()
    plt.savefig(f"{save_prefix}_rotation_examples.png")
    plt.close()

def train_ensemble_member(seed: int, cfg: CFG,
                          train_loader: DataLoader, val_loader: DataLoader,
                          C: int, K: int, device: torch.device):
    seed_everything(seed)
    model = BaselineCNN(C, K).to(device)
    optimizer = torch.optim.Adam(model.parameters(), lr=cfg.lr, weight_decay=cfg.weight_decay)

    stopper = EarlyStopping(patience=cfg.patience, min_delta=cfg.min_delta)

    history = {
        "train_loss": [],
        "val_loss": [],
        "train_acc": [],
        "val_acc": []
    }

    for epoch in range(cfg.epochs):
        tr_loss, tr_acc = train_one_epoch(model, train_loader, optimizer, device)
        va_loss, va_acc = evaluate(model, val_loader, device)

        history["train_loss"].append(tr_loss)
        history["val_loss"].append(va_loss)
        history["train_acc"].append(tr_acc)
        history["val_acc"].append(va_acc)

        print(f"[ens seed={seed}] epoch {epoch+1:02d}/{cfg.epochs}  "
              f"train_loss={tr_loss:.4f} train_acc={tr_acc:.4f}  "
              f"val_loss={va_loss:.4f} val_acc={va_acc:.4f}")

        if cfg.use_early_stopping:
            stopper.step(va_loss, model)

            if stopper.early_stop:
                print(f"[ens seed={seed}] Early stopping triggered at epoch {epoch+1}")
                break

    if cfg.use_early_stopping and stopper.best_state is not None:
        model.load_state_dict(stopper.best_state)

    return model, history

def fit_ensemble(cfg: CFG, train_loader: DataLoader, val_loader: DataLoader,
                 C: int, K: int, device: torch.device):
    models = []
    histories = []

    for i in range(cfg.ensemble_size):
        seed = cfg.ensemble_seed0 + i
        print(f"\n=== Training ensemble member {i+1}/{cfg.ensemble_size} (seed={seed}) ===")
        m, h = train_ensemble_member(seed, cfg, train_loader, val_loader, C, K, device)
        models.append(m)
        histories.append(h)

    return models, histories



def plot_training_curves(history: Dict[str, list], prefix: str):
    epochs = range(1, len(history["train_loss"]) + 1)

    # Loss curve
    plt.figure(figsize=(7, 5))
    plt.plot(epochs, history["train_loss"], marker="o", label="Train Loss")
    plt.plot(epochs, history["val_loss"], marker="o", label="Val Loss")
    plt.xlabel("Epoch")
    plt.ylabel("Loss")
    plt.title(f"Training Curve - Loss ({prefix})")
    plt.legend()
    plt.grid(True, alpha=0.3)
    plt.tight_layout()
    plt.savefig(f"{prefix}_loss_curve.png")
    plt.close()

    # Accuracy curve
    plt.figure(figsize=(7, 5))
    plt.plot(epochs, history["train_acc"], marker="o", label="Train Accuracy")
    plt.plot(epochs, history["val_acc"], marker="o", label="Val Accuracy")
    plt.xlabel("Epoch")
    plt.ylabel("Accuracy")
    plt.title(f"Accuracy Curve ({prefix})")
    plt.legend()
    plt.grid(True, alpha=0.3)
    plt.tight_layout()
    plt.savefig(f"{prefix}_accuracy_curve.png")
    plt.close()

def plot_ensemble_aggregate_curves(histories: list[Dict[str, list]], prefix: str = "ensemble_aggregate"):
    if len(histories) == 0:
        print("No ensemble histories provided.")
        return

    # early stopping 때문에 길이가 다를 수 있어서 가장 짧은 epoch 길이에 맞춤
    min_len = min(len(h["train_loss"]) for h in histories)

    train_loss = np.array([h["train_loss"][:min_len] for h in histories])
    val_loss   = np.array([h["val_loss"][:min_len] for h in histories])
    train_acc  = np.array([h["train_acc"][:min_len] for h in histories])
    val_acc    = np.array([h["val_acc"][:min_len] for h in histories])

    epochs = np.arange(1, min_len + 1)

    # epoch별 mean / std
    train_loss_mean = train_loss.mean(axis=0)
    train_loss_std  = train_loss.std(axis=0)

    val_loss_mean = val_loss.mean(axis=0)
    val_loss_std  = val_loss.std(axis=0)

    train_acc_mean = train_acc.mean(axis=0)
    train_acc_std  = train_acc.std(axis=0)

    val_acc_mean = val_acc.mean(axis=0)
    val_acc_std  = val_acc.std(axis=0)

    # Loss plot
    plt.figure(figsize=(8, 5))
    plt.plot(epochs, train_loss_mean, marker="o", label="Train Loss Mean")
    plt.fill_between(
        epochs,
        train_loss_mean - train_loss_std,
        train_loss_mean + train_loss_std,
        alpha=0.2,
        label="Train Loss ± 1 SD"
    )

    plt.plot(epochs, val_loss_mean, marker="o", label="Val Loss Mean")
    plt.fill_between(
        epochs,
        val_loss_mean - val_loss_std,
        val_loss_mean + val_loss_std,
        alpha=0.2,
        label="Val Loss ± 1 SD"
    )

    plt.xlabel("Epoch")
    plt.ylabel("Loss")
    plt.title("Deep Ensemble Aggregate Loss Curve")
    plt.legend()
    plt.grid(True, alpha=0.3)
    plt.tight_layout()
    plt.savefig(f"{prefix}_loss_curve.png")
    plt.close()

    # Accuracy plot
    plt.figure(figsize=(8, 5))
    plt.plot(epochs, train_acc_mean, marker="o", label="Train Accuracy Mean")
    plt.fill_between(
        epochs,
        train_acc_mean - train_acc_std,
        train_acc_mean + train_acc_std,
        alpha=0.2,
        label="Train Accuracy ± 1 SD"
    )

    plt.plot(epochs, val_acc_mean, marker="o", label="Val Accuracy Mean")
    plt.fill_between(
        epochs,
        val_acc_mean - val_acc_std,
        val_acc_mean + val_acc_std,
        alpha=0.2,
        label="Val Accuracy ± 1 SD"
    )

    plt.xlabel("Epoch")
    plt.ylabel("Accuracy")
    plt.title("Deep Ensemble Aggregate Accuracy Curve")
    plt.legend()
    plt.grid(True, alpha=0.3)
    plt.tight_layout()
    plt.savefig(f"{prefix}_accuracy_curve.png")
    plt.close()

    print(f"Saved: {prefix}_loss_curve.png")
    print(f"Saved: {prefix}_accuracy_curve.png")
# -----------------------------
# Test helper
# -----------------------------
@torch.no_grad()
def test_accuracy_from_probs(probs: torch.Tensor, y: torch.Tensor) -> float:
    pred = probs.argmax(dim=1)
    return (pred == y).float().mean().item()
def perturbation_experiment(model, dataset, device, denorm):

    idx = random.randint(0, len(dataset)-1)
    x, y = dataset[idx]

    x_orig = x.unsqueeze(0).to(device)

    noise = torch.randn_like(x_orig) * 0.2
    x_noise = (x_orig + noise).clamp(-3,3)

    rot = T.RandomRotation((45,45))
    x_rot = rot(x).unsqueeze(0).to(device)

    with torch.no_grad():

        logits_orig = model(x_orig)
        logits_noise = model(x_noise)
        logits_rot = model(x_rot)

        p_orig = F.softmax(logits_orig, dim=1)
        p_noise = F.softmax(logits_noise, dim=1)
        p_rot = F.softmax(logits_rot, dim=1)

        conf_orig, _ = p_orig.max(dim=1)
        conf_noise, _ = p_noise.max(dim=1)
        conf_rot, _ = p_rot.max(dim=1)

        unc_orig = predictive_entropy(p_orig)
        unc_noise = predictive_entropy(p_noise)
        unc_rot = predictive_entropy(p_rot)

    imgs = torch.cat([x_orig, x_noise, x_rot], dim=0)
    imgs = denorm(imgs).cpu()

    titles = [
        f"Original\nConf:{conf_orig.item():.2f}\nUnc:{unc_orig.item():.2f}",
        f"Noise\nConf:{conf_noise.item():.2f}\nUnc:{unc_noise.item():.2f}",
        f"Rotate\nConf:{conf_rot.item():.2f}\nUnc:{unc_rot.item():.2f}",
    ]

    plt.figure(figsize=(9,3))

    for i in range(3):
        plt.subplot(1,3,i+1)
        img = imgs[i].permute(1,2,0).numpy()
        plt.imshow(img)
        plt.title(titles[i])
        plt.axis("off")

    plt.tight_layout()
    plt.savefig("perturbation_example.png")
    plt.close()


def main():
    parser = argparse.ArgumentParser()
    parser.add_argument("--method", type=str, default="all",
                        choices=["baseline", "edl", "ensemble", "all"])
    parser.add_argument("--epochs", type=int, default=None)
    parser.add_argument("--batch_size", type=int, default=None)
    parser.add_argument("--data_dir", type=str, default=None)
    parser.add_argument("--cpu", action="store_true")
    args = parser.parse_args()

    cfg = CFG()
    if args.epochs is not None:
        cfg.epochs = args.epochs
    if args.batch_size is not None:
        cfg.batch_size = args.batch_size
    if args.data_dir is not None:
        cfg.data_dir = args.data_dir

    device = torch.device("cpu" if args.cpu or not torch.cuda.is_available() else "cuda")
    print("device:", device)

    seed_everything(cfg.seed)

    train_set, val_set, test_set, K, C, class_names, image_shape, denorm = get_dataset(cfg)
    train_loader = DataLoader(train_set, batch_size=cfg.batch_size, shuffle=True,
                              num_workers=cfg.num_workers, pin_memory=(device.type == "cuda"))
    val_loader = DataLoader(val_set, batch_size=cfg.batch_size, shuffle=False,
                            num_workers=cfg.num_workers, pin_memory=(device.type == "cuda"))
    test_loader = DataLoader(test_set, batch_size=cfg.batch_size, shuffle=False,
                             num_workers=cfg.num_workers, pin_memory=(device.type == "cuda"))

    # ---- Baseline ----
    if args.method in ["baseline", "all"]:
        print("\n### Baseline CNN ###")
        baseline, baseline_history = fit_baseline(cfg, train_loader, val_loader, C, K, device)
        plot_training_curves(baseline_history, prefix="baseline")
        te_loss, te_acc = evaluate(baseline, test_loader, device)
        print(f"[baseline] TEST  loss={te_loss:.4f} acc={te_acc:.4f}")

        probs, pred, y_test, conf = baseline_predict(baseline, test_loader, device)
        correct = (pred == y_test)

        # confidence histogram
        plt.figure(figsize=(7,4))
        plt.hist(conf[correct].numpy(), bins=20, alpha=0.7, label="correct", density=True)
        plt.hist(conf[~correct].numpy(), bins=20, alpha=0.7, label="wrong", density=True)
        plt.xlabel("Max softmax probability (confidence)")
        plt.ylabel("Density")
        plt.title("Confidence distribution (baseline)")
        plt.legend()
        plt.tight_layout()
        plt.savefig("baseline_confidence_hist.png")
        plt.close()

        # confusion matrix
        cm = torch.zeros(K, K, dtype=torch.int64)
        for t, p in zip(y_test, pred):
            cm[int(t), int(p)] += 1

        plt.figure(figsize=(6,6))
        plt.imshow(cm.numpy())
        plt.title("Confusion Matrix (Baseline)")
        plt.xlabel("Predicted")
        plt.ylabel("True")
        plt.colorbar()
        plt.xticks(range(K), class_names, rotation=45, ha="right")
        plt.yticks(range(K), class_names)
        plt.tight_layout()
        plt.savefig("baseline_confusion_matrix.png")
        plt.close()

        per_class_acc = cm.diag().float() / cm.sum(dim=1).clamp(min=1).float()
        for i, a in enumerate(per_class_acc.tolist()):
            print(f"{class_names[i]:>10s}: {a:.3f}")

        # misclassified examples
        mis_idx = torch.where(~correct)[0]
        n_show = min(24, len(mis_idx))

        if n_show == 0:
            print("No misclassified samples to display.")
        else:
            show_idx = mis_idx[torch.randperm(len(mis_idx))[:n_show]]

            imgs, titles = [], []
            for i in show_idx.tolist():
                x, y = test_set[i]
                imgs.append(x.unsqueeze(0))
                titles.append(
                    f"T:{class_names[y]}\nP:{class_names[int(pred[i])]}\nC:{conf[i].item():.2f}"
                )

            xbatch = torch.cat(imgs, dim=0).to(device)
            xvis = denorm(xbatch).cpu()

            grid = torchvision.utils.make_grid(xvis, nrow=6)
            plt.figure(figsize=(12,6))
            plt.imshow(grid.permute(1,2,0).numpy())
            plt.axis("off")
            plt.title("Misclassified examples (Baseline)")
            plt.tight_layout()
            plt.savefig("baseline_misclassified_examples.png")
            plt.close()

            print("Example titles (first few):")
            print("\n".join(titles[:6]))
        perturbation_experiment(baseline, test_set, device, denorm)

    # ---- EDL ----
    if args.method in ["edl", "all"]:
        print("\n### Evidential Deep Learning (EDL) ###")
        edl_model, edl_history = fit_edl(cfg, train_loader, val_loader, C, K, device)
        plot_training_curves(edl_history, prefix="edl")
        probs, pred, y, u = edl_predict(edl_model, test_loader, device, num_classes=K)
        acc = test_accuracy_from_probs(probs, y)
        print(f"[edl] TEST acc={acc:.4f}  uncertainty_proxy(mean K/S) avg={u.float().mean().item():.4f}")

      # ---- Ensemble ----
    if args.method in ["ensemble", "all"]:
        print("\n### Deep Ensembles ###")
        ens_models, ens_histories = fit_ensemble(cfg, train_loader, val_loader, C, K, device)

        # 개별 member curve
        for i, h in enumerate(ens_histories):
            plot_training_curves(h, prefix=f"ensemble_member_{i+1}")

        # aggregate mean ± std curve
        plot_ensemble_aggregate_curves(ens_histories, prefix="ensemble_aggregate")

        out = ensemble_predict(ens_models, test_loader, device)
        ens_acc = test_accuracy_from_probs(out["mean_probs"], out["y"])
        print(f"[ensemble] TEST acc={ens_acc:.4f}  "
              f"entropy_avg={out['entropy'].float().mean().item():.4f}  "
              f"mi_avg={out['mi'].float().mean().item():.4f}  "
              f"prob_var_avg={out['prob_var'].float().mean().item():.6f}")

           # -----------------------------
        # perturbation curve experiment (multiple images)
        # -----------------------------
        run_multi_example_perturbation_ensemble(
            ens_models,
            test_set,
            device,
            denorm,
            class_names,
            n_examples=5,
            noise_levels=np.linspace(0.0, 0.15, 7),
            angles=np.arange(0, 91, 10),
            save_prefix="ensemble_multi"
        )
        

if __name__ == "__main__":
    main()
