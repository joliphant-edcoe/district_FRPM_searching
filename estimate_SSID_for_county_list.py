import pandas as pd
import os
import sys
from thefuzz import fuzz

sys.path.insert(
    0,
    "C:\\Users\\joliphant\\OneDrive - El Dorado County Office of Education\\Documents\\edcoeUtils",
)

import edcoeUtils


def read_senr_sinf_files(basepath):
    senr_cols = edcoeUtils.extractColumns["SENR"] + ["updated1", "updated2"]
    files = os.listdir(basepath)
    senr_list = []
    for f in files:
        if f.startswith("SENR"):
            senr_df = pd.read_csv(
                os.path.join("extracts_for_county_sibling_database", f),
                sep="^",
                names=senr_cols,
                dtype={"ReportingLEA": "str"},
                parse_dates=["EnrollmentStartDate", "StudentBirthDate"],
            )
            senr_list.append(senr_df)

    # senr has some duplicate SSIDs students are enrolled in two places at once (10 and 20/30)
    # or have moved schools this year.
    # look for most recent enrollment, or primary enrollment (10) if on the same day
    senr = (
        pd.concat(senr_list)
        .reset_index(drop=True)
        .assign(LEA_name=lambda df_: df_.ReportingLEA.map(edcoeUtils.district_names))
        .assign(
            studentName=lambda df_: (
                df_.StudentLegalFirstName
                + " "
                + df_.StudentLegalMiddleName.fillna(" ").str[0]
                + " "
                + df_.StudentLegalLastName
                + " "
                + df_.StudentLegalNameSuffix.fillna("")
            )
            .str.replace(r"\s+", " ", regex=True)
            .str.strip()
        )
        .assign(
            noMiddleName=lambda df_: (
                df_.StudentLegalFirstName
                + " "
                + df_.StudentLegalLastName
                + " "
                + df_.StudentLegalNameSuffix.fillna("")
            )
            .str.replace(r"\s+", " ", regex=True)
            .str.strip()
        )
        # .assign(Gender = lambda df_:df_.StudentGenderCode.map({'M':'MA','F':'FE'})) # CalPADS records 'X' gender...
        .sort_values(
            ["EnrollmentStartDate", "EnrollmentStatusCode"], ascending=[False, True]
        )
        .drop_duplicates(subset=["SSID"], keep="first")
        .drop(
            columns=[
                "RecordTypeCode",
                "TransactionTypeCode",
                "LocalRecordID",
                "AcademicYearID",
                "StudentAliasFirstName",
                "StudentAliasMiddleName",
                "StudentAliasLastName",
                "StudentBirthCity",
                "StudentBirthStateProvinceCode",
                "StudentBirthCountryCode",
                "StudentExitReasonCode",
                "StudentSchoolCompletionStatus",
                "ExpectedReceiverSchoolofAttendance",
                "StudentMetallUCCSURequirementsIndicator",
                "MeritDiplomaIndicator",
                "SealofBiliteracyIndicator",
                "AdultAgeStudentswithDisabilitiesinTransitionStatus",
                "GraduationExemptionIndicator",
                "updated1",
                "updated2",
                "EnrollmentStartDate",
                "EnrollmentStatusCode",
                "EnrollmentExitDate",
                "StudentSchoolTransferCode",
                "DistrictofGeographicResidenceCode",
                "ReportingLEA",
                "LocalStudentID",
                "SchoolofAttendanceNPS",
                "StudentLegalFirstName",
                "StudentLegalMiddleName",
                "StudentLegalLastName",
                "StudentLegalNameSuffix",
            ]
        )
    )

    return senr


senr = read_senr_sinf_files(
    "extracts_for_county_sibling_database"
)  # .loc[:,['SSID','StudentLegalFirstName','StudentLegalLastName']]
print(senr)
print(senr.columns)

county = pd.read_excel("direct_cert.xlsx").reset_index(names="unique_county_id")
print(county)  # 3530 on county list

easy_matches = county.merge(
    senr,
    how="inner",
    left_on=["Child Name", "Date of Birth"],
    right_on=["studentName", "StudentBirthDate"],
)  # matches 1336, 2194 leftover


values_not_in_easy = county[
    ~county["unique_county_id"].isin(easy_matches["unique_county_id"])
]

next_easy_matches = values_not_in_easy.merge(
    senr,
    how="inner",
    left_on=["Child Name", "Date of Birth"],
    right_on=["noMiddleName", "StudentBirthDate"],
)  # matches 642, 2888 leftover, only 508 unique

values_not_in_nexteasy = values_not_in_easy[
    ~values_not_in_easy["unique_county_id"].isin(next_easy_matches["unique_county_id"])
]
# 1686 left

has_exact_match = pd.concat(
    [
        next_easy_matches[next_easy_matches.SSID.notna()],
        easy_matches[easy_matches.SSID.notna()],
    ]
).drop_duplicates()  # 1336+642=1978- duplicates = 1844


count_matches88 = 0
count_matches80 = 0
count_matches70 = 0
count_matches60 = 0
count_matches_leftover = 0
matched_names88 = []
matched_names80 = []
matched_names70 = []
matched_names60 = []
matched_names_leftover = []
how_many_share_birthday = []
for j, county_name in enumerate(values_not_in_nexteasy["Child Name"]):
    max_fr = 0
    county_birthdate = values_not_in_nexteasy["Date of Birth"].iloc[j]
    # force birthday to match first
    for i, senr_name in enumerate(
        senr.query("StudentBirthDate == @county_birthdate").studentName
    ):
        fr = fuzz.ratio(senr_name.upper(), county_name.upper())
        if fr > max_fr:
            max_fr = fr
            max_name = senr_name
            max_i = i
    how_many_share_birthday.append(i)
    if max_fr > 88:
        # found a satisfactory match
        count_matches88 += 1
        matched_names88.append([max_name, county_name, max_fr])
    elif max_fr > 80:
        # found a ok match
        count_matches80 += 1
        matched_names80.append([max_name, county_name, max_fr])
    elif max_fr > 70:
        # found a decent match
        count_matches70 += 1
        matched_names70.append([max_name, county_name, max_fr])
    elif max_fr > 60:
        # found a crappy match
        count_matches60 += 1
        matched_names60.append([max_name, county_name, max_fr])
    else:
        count_matches_leftover += 1
        matched_names_leftover.append([max_name, county_name, max_fr])

# 215 matches with a threshold of 90
# 285 matches with a threshold of 80

print(len(how_many_share_birthday))
print(sum(how_many_share_birthday)/len(how_many_share_birthday))
print(max(how_many_share_birthday))
print(min(how_many_share_birthday))
# I found a blantant error as high as 71
# print(matched_names88)
# print(matched_names80)
# print(matched_names70)
# print(matched_names60)
# print(matched_names_leftover)
print(f"{count_matches88=}")
print(f"{count_matches80=}")
print(f"{count_matches70=}")
print(f"{count_matches60=}")
print(f"{count_matches_leftover=}")

name_matching_dict = {match[0]: match[1] for match in matched_names88}

threshold88_matches = values_not_in_nexteasy.merge(
    senr.assign(match_name=lambda df_: df_.studentName.map(name_matching_dict)),
    how="inner",
    left_on=["Child Name", "Date of Birth"],
    right_on=["match_name", "StudentBirthDate"],
)

# 458 matches
print(threshold88_matches)


values_not_in_threshold88 = values_not_in_nexteasy[
    ~values_not_in_nexteasy["unique_county_id"].isin(
        threshold88_matches["unique_county_id"]
    )
]
#  1228 left


final_writeout = pd.concat(
    [has_exact_match, threshold88_matches, values_not_in_threshold88]
).sort_values("unique_county_id")
# .drop_duplicates()  # 1336+642=1978- duplicates = 1844

print(final_writeout)

final_writeout.to_csv("direct_cert_county_with_estimated_ssid.csv", index=False)
