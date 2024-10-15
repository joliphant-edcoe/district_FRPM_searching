import pandas as pd
import os
import sys
from openpyxl import load_workbook

sys.path.insert(
    0,
    "C:\\Users\\joliphant\\OneDrive - El Dorado County Office of Education\\Documents\\edcoeUtils",
)

import edcoeUtils


def read_senr_sinf_files(basepath):
    senr_cols = edcoeUtils.extractColumns["SENR"] + ["updated1", "updated2"]
    sinf_cols = edcoeUtils.extractColumns["SINF"] + ["updated1", "updated2"]
    files = os.listdir(basepath)
    senr_list = []
    sinf_list = []
    for f in files:
        if f.startswith("SENR"):
            senr_df = pd.read_csv(
                os.path.join("extracts_for_county_sibling_database", f),
                sep="^",
                names=senr_cols,
                dtype={"ReportingLEA": "str"},
                parse_dates=["EnrollmentStartDate"],
            )
            senr_list.append(senr_df)
        elif f.startswith("SINF"):
            sinf_df = pd.read_csv(
                os.path.join("extracts_for_county_sibling_database", f),
                sep="^",
                names=sinf_cols,
                dtype={"ReportingLEA": "str"},
                parse_dates=["EffectiveStartDate"],
            )
            sinf_list.append(sinf_df)

    # senr has some duplicate SSIDs students are enrolled in two places at once (10 and 20/30)
    # or have moved schools this year.
    # look for most recent enrollment, or primary enrollment (10) if on the same day
    senr = (
        pd.concat(senr_list)
        .assign(LEA_name=lambda df_: df_.ReportingLEA.map(edcoeUtils.district_names))
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
                "LocalStudentID",
                "StudentLegalMiddleName",
                "StudentLegalNameSuffix",
                "StudentAliasFirstName",
                "StudentAliasMiddleName",
                "StudentAliasLastName",
                "StudentBirthDate",
                "StudentGenderCode",
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
            ]
        )
    )
    # sinf has some duplicate SSIDs whenever their demographics/address/school
    # gets updated. Let's just grab the most recent one.
    sinf = (
        pd.concat(sinf_list)
        .assign(LEA_name=lambda df_: df_.ReportingLEA.map(edcoeUtils.district_names))
        .sort_values("EffectiveStartDate", ascending=False)
        .drop_duplicates(subset=["SSID"], keep="first")
        .loc[
            :,
            [
                "SSID",
                "EffectiveStartDate",
                "EffectiveEndDate",
                "ReportingLEA",
                "SchoolofAttendance",
                "ResidentialAddressLine1",
                "ResidentialAddressCityName",
                "MailingAddressLine1",
                "MailingAddressCityName",
                "LEA_name",
            ],
        ]
    )

    return senr, sinf


def read_1_18_files(basepath):
    # If a student moves schools, they may show up twice on the 1.18 county list
    # I will keep the records that are 181,182 for NSLP or 'Yes' for DirectCert
    files = os.listdir(basepath)
    return (
        pd.concat([pd.read_csv(os.path.join(basepath, f)) for f in files])
        .reset_index(drop=True)
        .assign(NSLPProgram=lambda df_: df_.NSLPProgram.fillna("No Program"))
        .drop_duplicates()  # simple cleanup
        .sort_values(["NSLPProgram", "DirectCert"], ascending=[True, False])
        .drop_duplicates(subset=["SSID"], keep="first")
        .drop(
            columns=[
                "RaceEthnicity",
                "EnrollmentStatus",
                "Foster",
                "TribalFosterYouth",
                "Homeless",
                "MigrantEdProgram",
                "ELASDesignation",
                "ELFundingEligible",
            ]
        )
    )


def merge_all(senr, sinf, up_sl):
    # do an 'inner' merge between senr and sinf, because if I don't have address
    # information for a student, he/she is not useful in searching for address
    # matches

    # do a 'left' merge with the unduplicated student list
    return senr.merge(sinf, on="SSID", how="inner", suffixes=("_senr", "_sinf")).merge(
        up_sl, on="SSID", how="left"
    )





def sibling_matching(df):
    def check_group(group):
        group = group.assign(
            _DirectCert=lambda df_: df_.DirectCert.fillna("No")
            .map({"Yes": True, "No": False})
            .astype(bool),
            _NSLPProgram=lambda df_: df_.NSLPProgram.fillna("No Program")
            .map({"182 - Reduced": True, "181 - Free": True, "No Program": False})
            .astype(bool),
        )
        if group._DirectCert.sum() > 0 and group._DirectCert.sum() != len(group):
            return group.assign(
                how_identified="DirectCert",
                what_district=group.query("_DirectCert== True").LEA_name_senr.iloc[0],
                SSID_matched=group.query("_DirectCert== True").SSID.iloc[0],
            )
        if group._NSLPProgram.sum() > 0 and group._NSLPProgram.sum() != len(group):
            return group.assign(
                how_identified="Application",
                what_district=group.query("_NSLPProgram == True").LEA_name_senr.iloc[0],
                SSID_matched=group.query("_NSLPProgram == True").SSID.iloc[0],
            )

    pd.set_option("future.no_silent_downcasting", True)
    sibling_group = (
        df[
            df.duplicated(
                subset=["ResidentialAddressLine1", "ResidentialAddressCityName"],
                keep=False,
            )
        ]
        .groupby(["ResidentialAddressLine1", "ResidentialAddressCityName"])
        .apply(check_group, include_groups=False)
        .reset_index()
        .drop(columns=["level_2", "EnrollmentExitDate"])
    )
    return sibling_group


def sort_by_lea(df):
    filename_dict = {
        "0973783": "BlackOakMineUnifiedSchoolDistrict.xlsx",
        "0961838": "BuckeyeUnionElementarySchoolDistrict.xlsx",
        "0961846": "CaminoUnionElementarySchoolDistrict.xlsx",
        "0910090": "EDCOE.xlsx",
        "0961853": "ElDoradoUnionHighSchoolDistrict.xlsx",
        "0961879": "GoldOakUnionElementarySchoolDistrict.xlsx",
        "0961887": "GoldTrailUnionElementarySchoolDistrict.xlsx",
        "0961895": "IndianDiggingsElementarySchoolDistrict.xlsx",
        "0961903": "LakeTahoeUnifiedSchoolDistrict.xlsx",
        "0961911": "LatrobeElementarySchoolDistrict.xlsx",
        "0961929": "MotherLodeUnionElementarySchoolDistrict.xlsx",
        "0961945": "PioneerUnionElementarySchoolDistrict.xlsx",
        "0961952": "PlacervilleUnionElementarySchoolDistrict.xlsx",
        "0961960": "PollockPinesElementarySchoolDistrict.xlsx",
        "0961978": "RescueUnionElementarySchoolDistrict.xlsx",
    }

    report_leas = df.ReportingLEA_senr.unique()
    for lea in report_leas:
        print(lea)
        print()
        direct = df.query(
            "_DirectCert == False and _NSLPProgram == False and ReportingLEA_senr == @lea and how_identified == 'DirectCert'"
        )
        application = df.query(
            "_DirectCert == False and _NSLPProgram == False and ReportingLEA_senr == @lea and how_identified == 'Application'"
        )

        with pd.ExcelWriter(
            os.path.join("final_excel_to_send", filename_dict[lea]),
            engine="openpyxl",
            mode="a",
            if_sheet_exists="replace",
        ) as writer:

            direct.to_excel(writer, sheet_name="AddressMatch-DirectCert", index=False)
            application.to_excel(
                writer, sheet_name="AddressMatch-Application", index=False
            )


senr, sinf = read_senr_sinf_files("extracts_for_county_sibling_database")
up_sl = read_1_18_files("database_of_frpm_applications")

df = merge_all(senr, sinf, up_sl)
print(df)
df.to_csv('all_Student_database.csv')


# df.to_csv("find_columns.csv")
sibling_group = sibling_matching(df)

sort_by_lea(sibling_group)

# sibling_group.to_csv("checking_siblings.csv")
