"""Real PyTorch forward passes -- no mocking of the model."""
import pytest

from model import CATEGORIES, Classifier, tokenize, vectorize, build_vocab


def test_tokenize_lowercases_and_strips_punctuation():
    assert tokenize("Invoice #4471! Total: $1,240.00") == ["invoice", "total"]


def test_vectorize_is_term_frequency_normalized():
    vocab = {"a": 0, "b": 1}
    v = vectorize("a a b", vocab)
    assert pytest.approx(v[0].item()) == 2 / 3
    assert pytest.approx(v[1].item()) == 1 / 3
    assert pytest.approx(v.sum().item()) == 1.0


def test_vectorize_empty_text_is_zero_vector():
    vocab = {"a": 0, "b": 1}
    v = vectorize("nothing matches here", vocab)
    assert v.sum().item() == 0.0


@pytest.mark.parametrize("expected,text", [
    ("invoice", "Invoice #4471. Total due: $1,240.00. Payment terms: net 30. Remit to accounts payable."),
    ("resume", "Senior Software Engineer, 8 years experience, distributed systems. MS Computer Science. References available."),
    ("contract", "This Agreement is entered into between Party A and Party B, governing law and jurisdiction herein."),
    ("email", "Hi Sarah, following up on yesterday's call. Let me know if you have questions. Best, Tom"),
    ("other", "The hiking trail offers scenic mountain views, suitable for all skill levels."),
])
def test_classifier_generalizes_to_unseen_text(classifier, expected, text):
    r = classifier.classify(text)
    assert r["category"] == expected
    assert 0.0 <= r["confidence"] <= 1.0
    assert set(r["scores"]) == set(CATEGORIES)
    assert abs(sum(r["scores"].values()) - 1.0) < 1e-3  # a real softmax distribution


def test_classifier_caches_to_disk_and_reload_matches(tmp_path):
    from model import Classifier
    path = tmp_path / "classifier.pt"
    c1 = Classifier(path)
    assert path.exists()
    r1 = c1.classify("Invoice total due 500 dollars net 30")
    c2 = Classifier(path)  # should load cached weights, not retrain from a different random init
    r2 = c2.classify("Invoice total due 500 dollars net 30")
    assert r1 == r2


def test_classifier_rejects_stale_cache_with_different_vocab(tmp_path):
    import torch
    from model import Classifier, DocumentClassifier
    path = tmp_path / "classifier.pt"
    torch.save({"model": DocumentClassifier(5).state_dict(), "vocab": {"x": 0}}, path)
    c = Classifier(path)  # vocab mismatch => must retrain rather than crash loading mismatched weights
    assert len(c.vocab) > 5
