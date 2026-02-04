from tinygrad import Tensor, TinyJit, dtypes
from typing import *

# @TinyJit
def _mpera_rolling(t: Tensor, w: int):
    C, R = t.shape
    out = R - w + 1

    idx = (
      Tensor.arange(out).reshape(1, out, 1) +
      Tensor.arange(w).reshape(1, 1, w)
    ).expand(C, out, w)

    valid = t.reshape(C, R, 1).expand(C, R, w).gather(1, idx)
    pad = Tensor.full((C, w-1, w), float("nan"))
    ret = pad.cat(valid, dim=1)

    return ret

# @TinyJit
def _mpera_orderby(t: Tensor, key: Tensor, ascending: bool=True) -> Tensor:
    perm = key.argsort(dim=0, descending=not ascending) # (N,)
    C, N = t.shape
    idx = perm.reshape(1, N).expand(C, N)
    return t.gather(1, idx)

def _mpera_groupby_base(t: Tensor, key: Tensor):
    key = key.reshape(-1)
    C, N = t.shape
    perm = key.argsort(dim=0)
    key_sorted = key.gather(0, perm)
    idx = perm.reshape(1, N).expand(C, N)
    t_sorted = t.gather(1, idx)
    diff = key_sorted[1:] != key_sorted[:-1]
    boundary = Tensor.ones(1, dtype=dtypes.int32).cat(diff.cast(dtypes.int32), dim=0)
    start_idx = boundary.nonzero().reshape(-1)
    G = start_idx.shape[0]
    group_id = boundary.cumsum(0).cast(dtypes.int32) - 1
    gid = group_id.reshape(1, N).expand(C, N)
    key_unique = key_sorted.gather(0, start_idx).reshape(1, G)
    return key_unique, gid, t_sorted, G, C, N

def _mpera_groupby_sum(t: Tensor, key: Tensor) -> Tensor:
    key_unique, gid, t_sorted, G, C, _ = _mpera_groupby_base(t, key)
    out = Tensor.zeros((C, G), dtype=t.dtype).scatter_reduce(1, gid, t_sorted, reduce="sum", include_self=False)
    return key_unique.cat(out, dim=0)

def _mpera_groupby_product(t: Tensor, key: Tensor) -> Tensor:
    key_unique, gid, t_sorted, G, C, _ = _mpera_groupby_base(t, key)
    out = Tensor.ones((C, G), dtype=t.dtype).scatter_reduce(1, gid, t_sorted, reduce="prod", include_self=False)
    return key_unique.cat(out, dim=0)

def _mpera_groupby_len(t: Tensor, key: Tensor) -> Tensor:
    key_unique, gid, _t_sorted, G, C, N = _mpera_groupby_base(t, key)
    ones = Tensor.ones((C, N), dtype=t.dtype)
    out = Tensor.zeros((C, G), dtype=t.dtype).scatter_reduce(1, gid, ones, reduce="sum", include_self=False)
    return key_unique.cat(out, dim=0)

def _mpera_groupby_mean(t: Tensor, key: Tensor) -> Tensor:
    key_unique, gid, t_sorted, G, C, N = _mpera_groupby_base(t, key)
    sums = Tensor.zeros((C, G), dtype=t.dtype).scatter_reduce(1, gid, t_sorted, reduce="sum", include_self=False)
    ones = Tensor.ones((C, N), dtype=t.dtype)
    counts = Tensor.zeros((C, G), dtype=t.dtype).scatter_reduce(1, gid, ones, reduce="sum", include_self=False)
    out = sums / counts
    return key_unique.cat(out, dim=0)

def _mpera_groupby_std(t: Tensor, key: Tensor) -> Tensor:
    key_unique, gid, t_sorted, G, C, N = _mpera_groupby_base(t, key)
    sums = Tensor.zeros((C, G), dtype=t.dtype).scatter_reduce(1, gid, t_sorted, reduce="sum", include_self=False)
    sums2 = Tensor.zeros((C, G), dtype=t.dtype).scatter_reduce(1, gid, t_sorted * t_sorted, reduce="sum", include_self=False)
    ones = Tensor.ones((C, N), dtype=t.dtype)
    counts = Tensor.zeros((C, G), dtype=t.dtype).scatter_reduce(1, gid, ones, reduce="sum", include_self=False)
    mean = sums / counts
    out = ((sums2 / counts) - mean * mean).sqrt()
    return key_unique.cat(out, dim=0)

def _mpera_join_inner(left: Tensor, right: Tensor, key_left: Tensor, key_right: Tensor) -> Tensor:
    k_left = key_left.reshape(-1)
    k_right = key_right.reshape(-1)
    n_left = k_left.shape[0]
    n_right = k_right.shape[0]

    eq = k_left.reshape(n_left, 1) == k_right.reshape(1, n_right)
    idxs = eq.nonzero()

    left_idx = idxs[:, 0]
    right_idx = idxs[:, 1]

    left_gather = left_idx.reshape(1, -1).expand(left.shape[0], -1)
    right_gather = right_idx.reshape(1, -1).expand(right.shape[0], -1)

    left_out = left.gather(1, left_gather)
    right_out = right.gather(1, right_gather)

    return left_out.cat(right_out, dim=0)

def _mpera_join_left_outer(left: Tensor, right: Tensor, key_left: Tensor, key_right: Tensor) -> Tensor:
    k_left = key_left.reshape(-1)
    k_right = key_right.reshape(-1)
    n_left = k_left.shape[0]
    n_right = k_right.shape[0]

    eq = k_left.reshape(n_left, 1) == k_right.reshape(1, n_right)
    idxs = eq.nonzero()

    left_idx = idxs[:, 0]
    right_idx = idxs[:, 1]

    ones = Tensor.ones((1, left_idx.shape[0]), dtype=left.dtype)
    counts = Tensor.zeros((1, n_left), dtype=left.dtype).scatter_reduce(
        1, left_idx.reshape(1, -1), ones, reduce="sum", include_self=False
    )
    missing = (counts.reshape(-1) == 0)
    missing_idx = missing.nonzero().reshape(-1)

    out_left_idx = left_idx.cat(missing_idx, dim=0)
    out_right_idx = right_idx.cat(
        Tensor.full((missing_idx.shape[0],), -1, dtype=right_idx.dtype), dim=0
    )

    nan_row = Tensor.full((right.shape[0], 1), float("nan"), dtype=right.dtype, device=right.device)
    right_padded = right.cat(nan_row, dim=1)

    mask = (out_right_idx < 0).cast(dtypes.int32)
    out_right_idx = out_right_idx + mask * (n_right + 1)

    left_gather = out_left_idx.reshape(1, -1).expand(left.shape[0], -1)
    right_gather = out_right_idx.reshape(1, -1).expand(right.shape[0], -1)

    left_out = left.gather(1, left_gather)
    right_out = right_padded.gather(1, right_gather)
    return left_out.cat(right_out, dim=0)
