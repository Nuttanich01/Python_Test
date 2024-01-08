import polars as pl

df = pl.DataFrame(
    {
        "strings_not_float": ["4", "200.0", "06", "7.0", "8.0"],
    }
)
print("--------")
print(df)
df_cast = df.select([pl.col("strings_not_float").cast(pl.Float64).cast(pl.Int64)])
print(df_cast)
# df_cast_2 = df_cast.select([pl.col("strings_not_float").cast(pl.Int64)])
# print(df_cast_2)
# try:
#     out = df.select([pl.col("strings_not_float").cast(pl.Float64)])
#     print(out)
# except Exception as e:
#     print(e)