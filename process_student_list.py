import pandas as pd


def filter_and_write(df, schools, type):

    if type == "EL":
        check = "See unified"
    elif type == "HS":
        check = "See unified"
    elif type == "UN":
        check = "See elementary/secondary"

    for school in schools:
        if school == check:
            continue

        if type == "EL":
            df_query = (
                "`Elementary School District Name` == @school and estimated_grade < 10"
            )
            message = (
                " and whose age indicates that they are likely less than 9th grade"
            )
        elif type == "HS":
            df_query = (
                "`Secondary School District Name` == @school and estimated_grade > 7"
            )
            message = (
                " and whose age indicates that they are likely greater than 8th grade"
            )
        elif type == "UN":
            df_query = "`Unified School District Name` == @school and `Elementary School District Name` == 'See unified'"
            message = ""

        filtered = df.query(df_query)
        # print(filtered)

        nms = filtered["Child Name"]
        lns = []
        bds = filtered["Date of Birth"]
        for nm in nms:
            split_nm = nm.upper().split()
            if split_nm[-1] == "JR." or split_nm[-1] == "III" or split_nm[-1] == "IV":
                lns.append(split_nm[-2])
            else:
                lns.append(split_nm[-1])

        query_string = "".join(
            [
                'STU.LN : "' + ln[:3] + '" AND STU.BD = "' + bd.strftime('%m/%d/%Y') + '" OR '
                for ln, bd in zip(lns, bds)
            ]
        )
        query_string = (
            "LIST STU FRE STU.SC STU.SN STU.NM STU.BD STU.BD.YEARS STU.GR STU.AD STU.CY STU.RAD STU.RCY FRE.CD IF "
            + query_string
        )
        query_string = query_string[:-3]

        # no birthdays and exact match last names
        lns = list(set(lns))
        query_string2 = "".join(['STU.LN = "' + ln + '" OR ' for ln in lns])
        query_string2 = (
            "LIST STU FRE STU.SC STU.SN STU.NM STU.BD STU.BD.YEARS STU.GR STU.AD STU.CY STU.RAD STU.RCY FRE.CD IF "
            + query_string2
        )
        query_string2 = query_string2[:-3]


        # just birthdays
        bds = list(set(bds))
        query_string3 = "".join(['STU.BD = "' + bd.strftime('%m/%d/%Y') + '" OR ' for bd in bds])
        query_string3 = (
            "LIST STU FRE STU.SC STU.SN STU.NM STU.BD STU.BD.YEARS STU.GR STU.AD STU.CY STU.RAD STU.RCY FRE.CD IF "
            + query_string3
        )
        query_string3 = query_string3[:-3]

        sub_df = filtered.loc[
            :,
            [
                "Pgm",
                "Case #",
                "Child Name",
                "Gender",
                "Date of Birth",
                "Address",
                "age",
                "estimated_grade",
            ],
        ]

        filename = "".join(school.split())
        with open(f"{filename}.txt", "w") as f:
            f.write(school)
            f.write("\n\n")
            f.write(
                f"""
Once a year, El Dorado County HHS provides us with a list of Direct Cert students along with their district of residence.
Often there are several hundred students with Unknown District. Here is a list of students from the Direct Cert Unknown list 
that have been determined to have an address that is within your district boundaries{message}. 
An excel files has also been provided with this same list of students."""
            )
            f.write("\n\n")
            f.write(sub_df.to_string(index=False))
            f.write("\n\n")
            f.write(
                "Some possible queries that you could run in Aeries to determine if these students are in your district:\n"
            )
            f.write(query_string)
            f.write("\n\n")
            f.write(query_string2)
            f.write("\n\n")
            f.write(query_string3)

        sub_df.to_excel(f"{filename}.xlsx", index=False)
        print(school,end='\t\t')
        print(len(filtered))

df1 = pd.read_csv("student_list.csv",parse_dates=['Date of Birth'])
df2 = pd.read_csv("from_api.csv",parse_dates=['Date of Birth'])
df = pd.concat([df1,df2],axis=0).sort_values("age", ascending=True)
print(len(df))

elem_schools = df["Elementary School District Name"].unique()
filter_and_write(
    df,
    elem_schools,
    "EL",
)


hs_schools = df["Secondary School District Name"].unique()
filter_and_write(
    df,
    hs_schools,
    "HS",
)


unified_schools = df["Unified School District Name"].unique()
filter_and_write(
    df,
    unified_schools,
    "UN",
)
