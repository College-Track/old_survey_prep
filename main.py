import pandas as pd
import helpers


# df = pd.read_csv("data/fy21_alumni.csv")
# df = pd.read_csv("data/fy19_ps.csv")
df = pd.read_csv("data/fy18_ps.csv")


project_id = "data-warehouse-289815"


survey, columns = helpers.prep_survey(df)

survey = helpers.process_df(survey, "student_18_digit_id")

helpers.upload_survey(project_id, survey, columns, "fy18", "ps")
