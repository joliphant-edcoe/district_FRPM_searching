import os
import json

import pandas as pd
from dotenv import load_dotenv
from geocodio import GeocodioClient
from geocodio.exceptions import GeocodioAuthError


path_to_file = os.path.dirname(__file__)
load_dotenv(os.path.join(path_to_file, ".env"))
api_key = os.getenv("geocodioKey")
client = GeocodioClient(api_key)

print("connected to geocodio")


def run_geocode(df, open_from_cache=False):

    addresses = df.Address.to_list()
    print("contacting geocodio with address information")

    if open_from_cache:
        with open("location_cache.json", "r") as f:
            locations = json.loads(f)
    else:
        try:
            locations = client.batch_geocode(addresses, fields=["school"])
        except GeocodioAuthError:
            print("geocodio auth error")
            return
        with open("location_cache.json", "w") as f:
            f.write(json.dumps(locations))

    input_address = []
    output_address = []
    accuracy_score = []
    accuracy_type = []
    elementary_name = []
    elementary_code = []
    secondary_name = []
    secondary_code = []
    unified_name = []
    unified_code = []

    for loc in locations:
        input_address.append(loc["input"]["formatted_address"])
        output_address.append(loc["results"][0]["formatted_address"])
        accuracy_score.append(loc["results"][0]["accuracy"])
        accuracy_type.append(loc["results"][0]["accuracy_type"])

        elementary = loc["results"][0]["fields"]["school_districts"].get("elementary")
        if elementary is not None:
            elementary_name.append(elementary["name"])
            elementary_code.append(elementary["lea_code"])
        else:
            elementary_name.append("See unified")
            elementary_code.append(None)

        secondary = loc["results"][0]["fields"]["school_districts"].get("secondary")
        if secondary is not None:
            secondary_name.append(secondary["name"])
            secondary_code.append(secondary["lea_code"])
        else:
            secondary_name.append("See unified")
            secondary_code.append(None)

        unified = loc["results"][0]["fields"]["school_districts"].get("unified")
        if unified is not None:
            unified_name.append(unified["name"])
            unified_code.append(unified["lea_code"])
        else:
            unified_name.append("See elementary/secondary")
            unified_code.append(None)

    result_df = pd.DataFrame(
        {
            "input_address": input_address,
            "output_address": output_address,
            "accuracy_score": accuracy_score,
            "accuracy_type": accuracy_type,
            "Elementary School District Name": elementary_name,
            "elementary_code": elementary_code,
            "Secondary School District Name": secondary_name,
            "secondary_code": secondary_code,
            "Unified School District Name": unified_name,
            "unified_code": unified_code,
        }
    )
    return pd.concat([df, result_df], axis=1)


def tweak_direct_cert(df):
    return (
        df.query('`School Name` == "Unknown"')
        .drop(columns=["School Name", "SSN"])
        .assign(
            age=(
                (pd.to_datetime("9/01/2024") - df["Date of Birth"]).dt.days // 365.2425
            ).astype("int")
        )
        .assign(estimated_grade=lambda df_: df_.age - 5)
        .reset_index()
    )


df = pd.read_excel("direct_cert.xlsx")

ready = tweak_direct_cert(df)

from_api = run_geocode(ready.query('Gender == "FE"'))

if from_api is not None:
    from_api.to_csv("from_api.csv", index=False)
