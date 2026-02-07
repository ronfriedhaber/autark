from autarkpy import CsvReader, OnceFrame

frame = OnceFrame(CsvReader("../../fuzz/data/cpu_usage_a.csv"))
frame.dataframe().col("cpu_usage").rolling(10).reduce("mean").alias("avg")
print(frame.realize())
