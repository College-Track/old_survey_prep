from janitor import clean_names
import pandas as pd


def prep_survey(df):
    _df = df.copy()
    df_clean_names = clean_names(
        _df,
        strip_underscores="both",
        preserve_original_columns=True,
        truncate_limit=80,
        remove_special=True,
    )
    df_clean_names.rename(columns=lambda x: x.lstrip("0123456789.-:_ "), inplace=True)
    df_columns = {
        "clean_columns": list(df_clean_names.columns),
        "original_columns": list(df_clean_names.original_columns),
    }
    columns_df = pd.DataFrame(df_columns)

    return df_clean_names, columns_df


def process_df(survey, id_column):

    _survey = survey.rename(columns={id_column: "Contact_Id"})
    _survey.dropna(subset=["Contact_Id"])

    return _survey


def upload_survey(project_id, survey_df, column_df, year, survey_type):
    survey_name = "surveys." + year + "_" + survey_type + "_survey"
    column_name = survey_name + "_columns"

    survey_df.to_gbq(survey_name, project_id=project_id, if_exists="replace")
    column_df.to_gbq(column_name, project_id=project_id, if_exists="replace")
    print("Upload Complete!")
