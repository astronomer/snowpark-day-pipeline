"Functions to generate trial customers and post-trial orders for the demo."

import pandas as pd
import numpy as np
import uuid


def generate_trial_customers(num_customers=200, seed=0):
    np.random.seed(seed)
    data = {
        "customer_id": [uuid.uuid4() for _ in range(num_customers)],
        "household_members": np.random.randint(1, 6, num_customers),
        "number_of_children": np.random.randint(0, 4, num_customers),
        "favorite_color": np.random.choice(
            ["red", "blue", "green", "yellow", "purple"], num_customers
        ),
        "favorite_season": np.random.choice(
            ["spring", "summer", "fall", "winter"], num_customers
        ),
        "sweetness_like": np.random.randint(0, 11, num_customers),
        "fun_spending_budget": np.random.randint(20, 201, num_customers),
    }
    return pd.DataFrame(data)


def generate_post_trial_orders(customers_df):
    orders_data = {
        "customer_id": customers_df["customer_id"],
        "Läckerli_boxes": np.zeros(len(customers_df), dtype=int),
        "Willisauer_Ringli_boxes": np.zeros(len(customers_df), dtype=int),
        "Mandelbärli_boxes": np.zeros(len(customers_df), dtype=int),
        "Chocolate_Brownies_boxes": np.zeros(len(customers_df), dtype=int),
    }

    for i, row in customers_df.iterrows():
        total_boxes = np.random.randint(1, row["household_members"] + 10)
        for _ in range(total_boxes):
            probabilities = {
                "Läckerli": 0.25 + (0.8 if row["favorite_color"] == "red" else 0),
                "Willisauer Ringli": 0.25,
                "Mandelbärli": 0.9 + (row["sweetness_like"] / 10) * 1.2,
                "Chocolate Brownies": 0.25 + (row["number_of_children"] / 3) * 0.8,
            }
            total_prob = sum(probabilities.values())
            for k in probabilities:
                probabilities[k] /= total_prob
            cookie_type = np.random.choice(
                list(probabilities.keys()), p=list(probabilities.values())
            )
            if cookie_type == "Läckerli":
                orders_data["Läckerli_boxes"][i] += 1
            elif cookie_type == "Willisauer Ringli":
                orders_data["Willisauer_Ringli_boxes"][i] += 1
            elif cookie_type == "Mandelbärli":
                orders_data["Mandelbärli_boxes"][i] += 1
            elif cookie_type == "Chocolate Brownies":
                orders_data["Chocolate_Brownies_boxes"][i] += 1
    return pd.DataFrame(orders_data)
