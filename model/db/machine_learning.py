from extrator import DataExtractor
import pandas as pd


class DataExtractorForMachineLearning(DataExtractor):
    def __init__(self,conect):
        DataExtractor.__init__(conect)

    def _submetted_list(self,df_student):
        if df_student.id.nunique() == 0:
            return 0
        else:
            return 1
    
    def _get_metrics_for_students_for_list(self,id_student,questions_list):
        if id_student.id.nunique() == 0:
            submission = 0
            number_of_questions = questions_list
            hits = 0
            mean_submissions = 0
            all_erros= 0
            parcial_hits = 0
            all_hits = 0
            total_time_spent = 0
            mean_time_spent = 0
            std_time = 0
            percentual_hit_question = 0
            sum_char_by_list = 0
            mean_char_by_list = 0
            std_char_by_list = 0
            len_answer = 0
            submitted = 0
        else:
            id_student.hitPercentage = pd.to_numeric(id_student.hitPercentage,errors = 'coerce')
            submission = id_student.id.nunique()
            number_of_questions = questions_list
            hits = self._get_hits_student(id_student)
            mean_submissions = id_student.hitPercentage.mean()
            all_erros= id_student.query('hitPercentage == 0').id.nunique()
            parcial_hits = id_student.query('hitPercentage > 0 and hitPercentage < 100').id.nunique()
            all_hits = id_student.query('hitPercentage == 100').id.nunique()
            total_time_spent = id_student.timeConsuming.sum()
            mean_time_spent = id_student.timeConsuming.mean()
            std_time = id_student.timeConsuming.std()
            percentual_hit_question = ((hits*100)/number_of_questions)
            sum_char_by_list = id_student.char_change_number.sum()
            mean_char_by_list = id_student.char_change_number.mean()
            std_char_by_list = id_student.char_change.numbe.std()
            len_answer = self._get_caracteres_for_question(id_student)
            submitted = 1
        
        return [id_student,submission,number_of_questions,hits,mean_submissions,all_erros,parcial_hits,all_hits,total_time_spent,
                mean_time_spent,std_time,percentual_hit_question,sum_char_by_list,mean_char_by_list,std_char_by_list,
                len_answer,submitted]
    
    def _validated_class(self,class_id):
        listas_de_listas_iniciais = []
        listas_de_listas_finais = []
        classes_iniciais = []
        classes_finais = []
        classes_importantes = []
        
        for i in range(len(self.listas)):
            listas_de_listas_iniciais.append(self._get_lists_for_class(self.listas[i]))
        for i in range(len(self.listas_finais)):
            listas_de_listas_finais.append(self._get_lists_for_class(self.listas[i]))

        for i in range(len(class_id.id.unique())):
            for j in range(len(listas_de_listas_iniciais)):
                if class_id.id.unique()[i] in listas_de_listas_iniciais[j].class_id.unique() and class_id.id.unique()[i] not in classes_iniciais:
                    classes_iniciais.append(class_id.id.unique()[i])
            for j in range(len(listas_de_listas_finais)):
                if class_id.id.unique()[i] in listas_de_listas_finais[j].class_id.unique() and class_id.id.unique()[i] not in classes_finais:
                    classes_finais.append(class_id.id.unique()[i])

        for i in range(len(classes_iniciais)):
            if classes_iniciais[i] in classes_finais:
                classes_importantes.append(classes_iniciais[i])
        return classes_importantes
    
    def get_metrics_for_train(self,years,first_lists,prevision_list):
        columns_intials = ['user_id','class_id','enrollment']
        columns_metrics = ['submissions_list','QuestionsList_list','HitsCorrects_list','submissions_mean_list','wrong_list',
                           'partially_wrong_list','Submissions_Corrects_list','TimeAll_list','TimeMean_list','TimeSD_list',
                           'Percentage_hit_list','sum_char_by_list','mean_char_by_list','std_char_by_list','len_answer','submitted_list']
        
        for i in range(len(first_lists)):
            for j in range(len(columns_metrics)):
                columns_intials.append(f'{columns_metrics[j]}0{i+1}')
        for i in range(len(prevision_list)):
            columns_intials.append(f'submitted_list_final0{i+1}')

        table = pd.DataFrame(columns=columns_intials)
        row = 0
        class_for_year = self._get_class_for_year(years[0],years[1])
        class_validated = self._validated_class(class_for_year)
        
        for i in range(len(class_validated)):
            students = self._get_student_for_class(class_validated[i])
            questions_inicial_list = [self._get_questions_for_list(first_lists[j]) for j in range(len(first_lists))]

            for j in range(len(students.id.unique())):
                submissions_first_lists = [self._get_submissions_for_student(students.loc[j].user_id,first_lists[lista],class_validated[i]) for lista in range(len(first_lists))]
                submissions_last_lists = [self._get_submissions_for_student(students.loc[j].user_id,prevision_list[lista],class_validated[i]) for lista in range(len(prevision_list))]

                data_first_list = [self._get_metrics_for_students_for_list(submissions_first_lists[lista],questions_inicial_list[lista]) for lista in range(len(first_lists))]
                data_last_list = [self._submetted_list(submissions_last_lists[lista]) for lista in range(len(prevision_list))]

                metrics_students = [students.loc[j].user_id,students.loc[j].class_id,students.loc[j].enrollment]

                for lists in range(len(data_first_list)):
                    for metrics in data_first_list[lists]:
                        metrics_students.append(metrics)
                for lists in range(len(data_last_list)):
                    metrics_students.append(data_last_list[lists])

                table.loc[row] = metrics_students
                row +=1
        return table