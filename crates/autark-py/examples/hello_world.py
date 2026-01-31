from autarkpy import *

df = DataFrame(DataReader.Csv("../../fuzz/data/cpu_usage_a.csv"))

df = df.with_column("avg", col("cpu_usage").rolling_mean(10))
df.realize()
