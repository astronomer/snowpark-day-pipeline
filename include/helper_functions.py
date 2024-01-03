from snowflake.snowpark.functions import col
import random


def train_test_split(df, test_size=0.2, seed=42):
    random.seed(seed)

    df_with_rand = df.selectExpr("*", "RANDOM() as random_col")

    split_point = 1 - test_size

    train_df = df_with_rand.filter(col("random_col") <= split_point).drop("random_col")
    test_df = df_with_rand.filter(col("random_col") > split_point).drop("random_col")

    return train_df, test_df
