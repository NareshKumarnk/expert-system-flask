import warnings

import numpy as np

from storeDetailsToDb import *


def get_symptoms_alf(isAlf, column):
    query = "select count({1}) from liver_failure where alf = {0} and {1} = true".format(isAlf, column)
    column_names = [column]
    message = "success"
    data = prod_psql_to_dataframe(query, column_names, message)
    return data


# symptoms = ['obesity','dyslipidemia','pvd','alcohol_consumption','hypertension','familyhypertension','diabetes',
# 'family_diabetes','hepatitis','family_hepatitis','chronic_fatigue']

def get_symptoms_from_db():
    query = "select obesity,dyslipidemia,pvd,alcohol_consumption,hypertension,familyhypertension,diabetes," \
            "family_diabetes,hepatitis,family_hepatitis,chronic_fatigue,alf  from liver_failure"
    symptoms = ['obesity', 'dyslipidemia', 'pvd', 'alcohol_consumption', 'hypertension', 'familyhypertension',
                'diabetes', 'family_diabetes', 'hepatitis', 'family_hepatitis', 'chronic_fatigue', 'alf']
    column_names = symptoms
    message = "success"
    data = prod_psql_to_dataframe(query, column_names, message)
    return data


def symptoms_ratio():
    data = get_symptoms_from_db()
    agg_func_count = {'alf': ['sum']}
    col_sel = ['obesity', 'dyslipidemia', 'pvd', 'alcohol_consumption', 'hypertension', 'familyhypertension',
               'diabetes', 'family_diabetes', 'hepatitis', 'family_hepatitis', 'chronic_fatigue']
    prob_dict = {}
    for i in col_sel:
        prob_dict[i] = None

    for i in col_sel:
        prob_dict[i] = (data.groupby(i).agg(agg_func_count).T.to_dict('list'))

    save_df = pd.DataFrame.from_dict(prob_dict)
    save_df.applymap(lambda x: x[0] if isinstance(x, list) else x)
    save_df = save_df.transpose()
    save_df = save_df.applymap(lambda x: x[0] if isinstance(x, list) else x)
    save_df['symptoms'] = save_df.index
    save_df['ratioFalse'] = ((save_df[bool(0)] / save_df[bool(0)].sum()) * 100).round(decimals=2)
    save_df['ratioTrue'] = ((save_df[bool(1)] / save_df[bool(1)].sum()) * 100).round(decimals=2)

    return save_df


def symptoms_corr():
    # query = "select obesity,dyslipidemia,pvd,alcohol_consumption,hypertension,familyhypertension,diabetes," \
    #         "family_diabetes,hepatitis,family_hepatitis,chronic_fatigue,alf  from liver_failure"
    symptoms = ['obesity', 'dyslipidemia', 'pvd', 'alcohol_consumption', 'hypertension', 'familyhypertension',
                 'diabetes', 'family_diabetes', 'hepatitis', 'family_hepatitis', 'chronic_fatigue', 'alf']
    # column_names = symptoms
    # message = "success"
    data = get_symptoms_from_db()
    data.dropna(axis=0,inplace=True)
    for col in symptoms:
        data[col] = data[col].astype(int)
    corr_data = data.corr()
    return corr_data
