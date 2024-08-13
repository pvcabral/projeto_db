from extrator import DataExtractor
import pandas as pd


class DataExtractorDataViewr(DataExtractor):
    def __init__(self,conector):
        DataExtractor.__init__(self,conector)

    def hits_by_list(submissions,amount_question):
        if submissions.id.nunique() == 0:
            return [0,0,amount_question]
        else:
            acertos = submissions.query('hitPercentage == 100').question_id.unique()
            parcial = []
            for i in range(len(submissions.query('hitPercentage > 0 and hitPercentage < 100').question_id.unique())):
                if submissions.query('hitPercentage > 0 and hitPercentage < 100').question_id.unique()[i] not in acertos:
                    parcial.append(submissions.query('hitPercentage > 0 and hitPercentage< 100').question_id.unique()[i])
            erros = (amount_question) - len(acertos) - len(parcial)
            return [len(acertos),len(parcial),erros]
        
    def submission_by_subject(self,class_student,lists):
        list_id01 = '4f3452ae-87d4-4a5f-9532-c7634da2cff0'
        list_id02 = '70e6c800-7583-485d-ac9f-9c42c48dd785'
        list_id03 = '42d5c4ad-7b44-4e4e-b2e5-2997785132c8'

        list_id04 = 'dbfcc83c-f014-44ae-8260-f06d4e1370ba'
        list_id05 = '70d1aa7e-00cb-4428-892f-9ac67cc50449'
        list_id06 = 'f5b1d992-68de-4854-97d8-c4bd84bd593b'

        list_id07 = 'dd1cd083-6c00-4605-9ff1-163bd4069363'
        list_id08 = '0a2c49a2-482c-40d7-bc44-ee368f089560'
        list_id09 = 'aca84206-87b2-4478-afa4-6193119cc06a'

        list_id10 = '34691c68-f01d-46ad-808d-6bbd44bcd9a2'
        list_id11 = '813dedaa-0989-489b-9804-3850ae4b83d1'
        list_id12 = 'f5bc770d-2ac7-4060-ab1c-e42b5541f7a7'


        list_id13 = '93cd33c4-109a-401e-87e9-e26ff5929d37'
        list_id14 = 'c4d09d1b-73a5-45c6-8a0d-ead669114b65'
        list_id15 = '1551e3ed-c875-4bb3-8e4e-58407df40776'
        
        df_class = pd.DataFrame(columns = ['user_id','expressoes','estrutura_de_decisao','repeticao_condicional','repeticao_contada','vetores'])
        students = self._get_student_for_class(class_student)
        row = 0
        for student in students.id.unique():
            list_with_result = [f'{student}']
            submission_with_subject = self._get_amount_questions_by_subject()
                