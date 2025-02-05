import pandas as pd
import sqlite3
import string

class CreateKaggleSurveyDB:
    def __init__(self):
        #生成字典，放入分類好的DataFrame:2020問題、2020回答、2021問題、2021回答、2022問題、2022回答
        survey_years = [2020, 2021, 2022]
        df_dict = {}
        for survey_year in survey_years:
            file_path = f'data_scientists_toolbox/data/kaggle_survey_{survey_year}_responses.csv'
            df = pd.read_csv(file_path, low_memory = False, skiprows = [1])#不要題目
            df = df.iloc[:, 1:]#不要填答時間的欄位
            df_dict[survey_year, 'responses'] = df#答案的字典
            df = pd.read_csv(file_path, nrows = 1)#只要題目
            question_descriptions = df.values.ravel()#二維轉一維
            question_descriptions = question_descriptions[1:]#不要填答時間的欄位
            df_dict[survey_year, 'question_descriptions'] = question_descriptions
        self.survey_years = survey_years #將處理後的資料存入物件屬性
        self.df_dict = df_dict
    def tidy_2020_2021_data(self, survey_year:int) -> tuple:
        ##整理2020-2021問題和答案
        #整理問題的格式：將分題號、題型、題目敘述三個list，並將多選題的題號統一格式
        question_indexes, question_types, question_descriptions = [], [], []
        column_names = self.df_dict[survey_year, "responses"].columns#取題號
        descriptions = self.df_dict[survey_year, "question_descriptions"]
        for column_name, question_description in zip(column_names, descriptions):
            column_name_split = column_name.split('_')#切割題號
            question_description_split = question_description.split(' - ')#切割題目
            if len(column_name_split) == 1:#只有題號Q1
                question_index = column_name_split[0]
                question_indexes.append(question_index)
                question_types.append('Multiple choice')
                question_descriptions.append(question_description_split[0])
            else:
                if column_name_split[1] in string.ascii_uppercase:#題號為Q28_A_Part_1，需縮寫成Q28A
                    question_index = column_name_split[0] + column_name_split[1]
                    question_indexes.append(question_index)
                else:#題號為Q14_Part_1，需縮寫成Q14
                    question_index = column_name_split[0]
                    question_indexes.append(question_index)
                question_types.append("Multiple selection")
                question_descriptions.append(question_description_split[0])
        #將整理好的題號、題型、題目敘述三個list加入問券年份，組成一個問題的DataFrame
        question_df = pd.DataFrame()
        question_df["question_index"] = question_indexes
        question_df["question_type"] = question_types
        question_df["question_description"] = question_descriptions
        question_df["surveyed_in"] = survey_year
        question_df = question_df.groupby(["question_index", "question_type", "question_description", "surveyed_in"]).count().reset_index() #將重複的題目分組合併後reset_index成新的DataFrame
        #整理回答：將回答的內容 1.題號調整同問題整理好的格式 2.加入受訪者編號 3.資料轉置，以受訪者編號為主，題號作為變數，觀察回覆 4.加入問卷年份 5.調整受訪者編號的欄位名稱 6.將 NaN 的列刪除，並重新編號索引值
        response_df = self.df_dict[survey_year, "responses"]
        response_df.columns = question_indexes #把欄位名稱（題號）替換成上面已經整理好的題號格式
        response_df_reset_index = response_df.reset_index()#因為原始資料沒有幫受訪者編號，所以需要reset_index()將index作為自訂的受訪者編號
        response_df_melted = pd.melt(response_df_reset_index, id_vars="index", var_name="question_index", value_name="response")#將資料轉置，受訪者編號(index)指定不變，題號作為變數
        response_df_melted["responded_in"] = survey_year #加入一欄是調查資料的年份
        response_df_melted = response_df_melted.rename(columns={"index": "respondent_id"})#將'index'改名為'respondent_id'
        response_df_melted = response_df_melted.dropna().reset_index(drop=True)#把有nan的列刪除，並用.reset_index(drop=True) 確保了重置索引時，不將舊索引保留為新的一列，而是直接丟棄並生成新索引
        return question_df, response_df_melted
    def tidy_2022_data(self, survey_year:int) -> tuple:
        ##整理2022（沒有複選題）
        #問題整理：同樣生成3個list後組成DataFrame
        question_indexes, question_types, question_descriptions = [], [], []
        column_names = self.df_dict[survey_year, "responses"].columns
        descriptions = self.df_dict[survey_year, "question_descriptions"]
        for column_name, question_description in zip(column_names, descriptions):
            column_name_split = column_name.split("_")
            question_description_split = question_description.split(" - ")
            if len(column_name_split) == 1:
                question_types.append("Multiple choice")
            else:
                question_types.append("Multiple selection")
            question_index = column_name_split[0]
            question_indexes.append(question_index)
            question_descriptions.append(question_description_split[0])
        question_df = pd.DataFrame()
        question_df["question_index"] = question_indexes
        question_df["question_type"] = question_types
        question_df["question_description"] = question_descriptions
        question_df["surveyed_in"] = survey_year
        question_df = question_df.groupby(["question_index", "question_type", "question_description", "surveyed_in"]).count().reset_index()
        #回答整理
        response_df = self.df_dict[survey_year, "responses"]
        response_df.columns = question_indexes
        response_df_reset_index = response_df.reset_index()
        response_df_melted = pd.melt(response_df_reset_index, id_vars="index", var_name="question_index", value_name="response")
        response_df_melted["responded_in"] = survey_year
        response_df_melted = response_df_melted.rename(columns={"index": "respondent_id"})
        response_df_melted = response_df_melted.dropna().reset_index(drop=True)
        return question_df, response_df_melted
    def create_database(self):
        question_df = pd.DataFrame()
        response_df = pd.DataFrame()
        for survey_year in self.survey_years:
            if survey_year == 2022:
                q_df, r_df = self.tidy_2022_data(survey_year)
            else:
                q_df, r_df = self.tidy_2020_2021_data(survey_year)
            question_df = pd.concat([question_df, q_df], ignore_index = True)#上下合併
            response_df = pd.concat([response_df, r_df], ignore_index = True)
        connection = sqlite3.connect('data_scientists_toolbox/data/kaggle_survey.db')
        question_df.to_sql('questions', con = connection, if_exists = "replace", index = False)
        response_df.to_sql('responses', con = connection, if_exists = "replace", index = False)
        cur = connection.cursor()#建立檢視表
        drop_view_sql = """
        DROP VIEW IF EXISTS aggregated_responses;
        """
        create_view_sql = """
        CREATE VIEW aggregated_responses AS
        SELECT questions.surveyed_in,
               questions.question_index,
               questions.question_type,
               questions.question_description,
               responses.response,
               COUNT(responses.respondent_id) AS response_count
          FROM responses
          JOIN questions
            ON responses.question_index = questions.question_index AND
               responses.responded_in = questions.surveyed_in
         GROUP BY questions.surveyed_in,
                  questions.question_index,
                  responses.response;
        """
        cur.execute(drop_view_sql)
        cur.execute(create_view_sql)
        connection.close()
create_kaggle_survey_db = CreateKaggleSurveyDB()
create_kaggle_survey_db.create_database()