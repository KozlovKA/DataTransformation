import pandas as pd
import numpy as np
import datetime as dt


def date_definition(amount):
    day = np.random.randint(1, 28, amount)
    month = np.random.randint(1, 12, amount)
    year = np.random.randint(2000, 2021, amount)
    return year, month, day


def product_id_definition(amount):
    return np.random.randint(1, 10000, amount)


def amount_definition(amount):
    return np.random.randint(1, 10000, amount)


def customer_definition(amount):
    return np.random.randint(0, 9999, amount)


def seller_definition(amount):
    return np.random.randint(0, 9999, amount)


def boolean(amount):
    return np.random.randint(0, 2, amount)


def price(amount):
    return np.random.random_sample(amount) * 100000


def discount(amount):
    return np.random.random_sample(amount)


def total(price, dicsount):
    return price * dicsount


def deal_id(amount):
    num = np.arange(0, amount)
    return num


def table_creation(date, product_id, amount, customer, seller, price, discount, deal_id, size,
                   ):
    time_now = dt.datetime.now()
    year, month, day = date
    append_data = []
    for x in range(0, size):
        buy_date = f"{year[x]}-{month[x]}-{day[x]}"
        total = price[x]-(price[x] * discount[x])
        df = pd.DataFrame(
            data=[[buy_date, product_id[x], amount[x], customer[x], seller[x], price[x], discount[x], total,
                   time_now,
                   deal_id[x]]],
            columns=["date", "product_id", "amount", "buyer", "seller", "price", "discount", "total",
                     "update_timestamp",
                     "deal_id"])
        append_data.append(df)
    append_data = pd.concat(append_data)
    return append_data


if __name__ == '__main__':
    date = date_definition(10000)
    product_id = product_id_definition(10000)
    amount = amount_definition(10000)
    customer = customer_definition(10000)
    seller = seller_definition(10000)
    price = price(10000)
    discount = discount(10000)
    deal_id = deal_id(10000)
    boolean = boolean(10000)
    df = table_creation(date, product_id, amount, customer, seller, price, discount, deal_id, size=10000)
    df['date'] = pd.to_datetime(df["date"])
    df["deal_id"] = df["deal_id"].astype(str)
    df["deal_id"] = "id" + df["deal_id"]
    df.to_csv('file.csv', index=False)
