from tinygrad import Tensor, TinyJit
from typing import *



def transform(dfs: List[Tensor], name2index: Dict[str, int]):
        output = {};
        x0 = dfs[0][name2index[0]['Close']];
        x1 = x0.mean();
        output['mean'] = x1;
        output = {k: output[k].realize().tolist() for k in output};
        return output

transform([ Tensor([ [1.,2.,3.], [4.,5.,6.] ]) ], name2index=[{"Close": 0, "b": 0}])


