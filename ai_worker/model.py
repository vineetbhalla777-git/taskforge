"""A small, real PyTorch text classifier -- not a mock.

Bag-of-words TF features over a fixed vocabulary -> a two-layer MLP -> softmax over
document categories. It's intentionally small (trains from scratch in under a second
on CPU) so the demo doesn't need a GPU or a multi-hundred-megabyte pretrained download,
but every forward pass is a genuine nn.Module doing genuine tensor math, and the
training loop is genuine backprop over a labelled dataset -- swap in a transformer and
a real corpus later without changing anything else in this worker.
"""
from __future__ import annotations

import re
from pathlib import Path

import torch
import torch.nn as nn

CATEGORIES = ["invoice", "resume", "contract", "email", "other"]

# A small labelled dataset of short synthetic documents per category. Real deployments
# would replace this with a proper labelled corpus; the model architecture doesn't change.
TRAIN_DATA = [
    ("invoice", "invoice number 10234 total amount due 540.00 payment terms net 30 remit to accounts payable"),
    ("invoice", "bill to acme corp quantity unit price subtotal tax total balance due upon receipt"),
    ("invoice", "purchase order invoice date due date itemized charges shipping and handling total owed"),
    ("invoice", "statement of account outstanding balance please remit payment invoice reference number"),
    ("invoice", "tax invoice gst amount line items unit cost quantity total payable within 14 days"),
    ("resume", "experienced software engineer skilled in python java distributed systems seeking new role"),
    ("resume", "curriculum vitae education bachelor of science work experience senior developer references available"),
    ("resume", "objective seeking a challenging position leveraging five years of project management experience"),
    ("resume", "professional summary results driven marketing manager proven track record leadership skills"),
    ("resume", "work history software engineer intern university degree computer science relevant coursework"),
    ("contract", "this agreement is entered into by and between the parties hereinafter referred to as"),
    ("contract", "the term of this agreement shall commence on the effective date and continue until termination"),
    ("contract", "party a and party b agree to the following terms and conditions governing law jurisdiction"),
    ("contract", "confidentiality clause non disclosure indemnification liability limitation of damages hereby"),
    ("contract", "whereas the parties desire to set forth their respective rights and obligations herein"),
    ("email", "hi team just following up on our meeting yesterday let me know if you have any questions"),
    ("email", "thanks for reaching out i will get back to you by end of day best regards"),
    ("email", "reminder the deadline for the report submission is tomorrow please send your updates"),
    ("email", "hope you are doing well wanted to check in about the project status update"),
    ("email", "please find attached the document you requested let me know if anything is missing"),
    ("other", "the quick brown fox jumps over the lazy dog while birds sing in the trees"),
    ("other", "recipe for chocolate cake mix flour sugar eggs bake at 350 degrees for 30 minutes"),
    ("other", "weather forecast for tomorrow sunny skies with a light breeze and mild temperatures"),
    ("other", "the museum exhibit features paintings from the renaissance period through modern art"),
    ("other", "hiking trail guide moderate difficulty scenic views recommended for all skill levels"),
]

_WORD_RE = re.compile(r"[a-z]+")


def tokenize(text: str) -> list[str]:
    return _WORD_RE.findall(text.lower())


def build_vocab(docs: list[str], min_len: int = 2) -> dict[str, int]:
    vocab: dict[str, int] = {}
    for doc in docs:
        for tok in tokenize(doc):
            if len(tok) >= min_len and tok not in vocab:
                vocab[tok] = len(vocab)
    return vocab


def vectorize(text: str, vocab: dict[str, int]) -> torch.Tensor:
    v = torch.zeros(len(vocab))
    for tok in tokenize(text):
        idx = vocab.get(tok)
        if idx is not None:
            v[idx] += 1.0
    total = v.sum()
    if total > 0:
        v = v / total  # term-frequency normalisation
    return v


class DocumentClassifier(nn.Module):
    def __init__(self, vocab_size: int, hidden: int = 32, n_classes: int = len(CATEGORIES)):
        super().__init__()
        self.net = nn.Sequential(
            nn.Linear(vocab_size, hidden),
            nn.ReLU(),
            nn.Dropout(0.1),
            nn.Linear(hidden, n_classes),
        )

    def forward(self, x: torch.Tensor) -> torch.Tensor:
        return self.net(x)


class Classifier:
    """Loads a cached model from disk if present, otherwise trains one and caches it."""

    def __init__(self, cache_path: Path):
        self.cache_path = cache_path
        self.vocab = build_vocab([d for _, d in TRAIN_DATA])
        self.model = DocumentClassifier(len(self.vocab))
        if cache_path.exists():
            state = torch.load(cache_path, map_location="cpu", weights_only=True)
            if state.get("vocab") == self.vocab:  # vocab must match the saved weights' input size
                self.model.load_state_dict(state["model"])
                self.model.eval()
                return
        self._train()
        cache_path.parent.mkdir(parents=True, exist_ok=True)
        torch.save({"model": self.model.state_dict(), "vocab": self.vocab}, cache_path)

    def _train(self, epochs: int = 300, lr: float = 0.05) -> None:
        X = torch.stack([vectorize(doc, self.vocab) for _, doc in TRAIN_DATA])
        y = torch.tensor([CATEGORIES.index(c) for c, _ in TRAIN_DATA])
        opt = torch.optim.Adam(self.model.parameters(), lr=lr)
        loss_fn = nn.CrossEntropyLoss()
        self.model.train()
        for _ in range(epochs):
            opt.zero_grad()
            loss = loss_fn(self.model(X), y)
            loss.backward()
            opt.step()
        self.model.eval()

    @torch.no_grad()
    def classify(self, text: str) -> dict:
        x = vectorize(text, self.vocab).unsqueeze(0)
        logits = self.model(x)
        probs = torch.softmax(logits, dim=1).squeeze(0)
        best = int(torch.argmax(probs).item())
        return {
            "category": CATEGORIES[best],
            "confidence": round(float(probs[best]), 4),
            "scores": {c: round(float(p), 4) for c, p in zip(CATEGORIES, probs)},
        }
