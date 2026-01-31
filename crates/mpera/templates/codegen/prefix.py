from tinygrad import Tensor, TinyJit
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
