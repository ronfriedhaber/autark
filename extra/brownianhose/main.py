import pandas as pd;import numpy as np;import json;import sys;import time;

bm = lambda n,dt,x0=0.,mu=0.,s=1.,c=None,seed=None: (
    np.clip(x:=x0+np.r_[0,np.cumsum(mu*dt+s*np.sqrt(dt)*np.random.default_rng(seed).standard_normal(n))],*c)
    if c else x
)

ts = lambda n, t0=0, μ=1.0: t0 + np.cumsum(np.random.exponential(μ, n)) # a little glyphs, APL would be proud

def main():
    path = str(sys.argv[1]);path_out = str(sys.argv[2])
    with open(path, "r") as f:c=json.loads(f.read());
    n=c["n"];c=c["columns"];now_t=time.time_ns()//1_000_000;
    o={
        i: (
            bm(n=n,dt=c[i]["params"]["dt"],x0=c[i]["params"]["x0"],mu=c[i]["params"]["mu"],s=c[i]["params"]["s"],
               c=c[i]["params"]["c"],seed=c[i]["params"]["seed"])) if c[i]["type"] == "brownian" else (
            ts(n=n, t0=now_t-int(1e3 * 60 * 60 * 24 * 500)) if c[i]["type"] == "timestamp" else (None)
        ) 
        for i in c
    };df=pd.DataFrame(data=o);df.to_csv(path_out);print("Finished");
if __name__ == "__main__": main()
