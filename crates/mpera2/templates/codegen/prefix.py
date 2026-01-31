from tinygrad import Tensor, TinyJit
from typing import *

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
