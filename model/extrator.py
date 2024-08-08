from sqlalchemy import create_engine
from sqlalchemy import text
from sqlalchemy import bindparam
import pandas as pd

class DataExtractor:
    '''The data extractor class is used to perform data extractions from the LOP database in a simple way. 
    For all basic extractions, we only need to provide the necessary parameters, 
    and we will be able to visualize a DataFrame with the requested data.
    '''

    def __init__(self,conect):
        self.__engine = create_engine(conect)
        self.__con = self.__engine.connect()

    def _conect_of(self):
        self.__con.close()

    def _get_class_for_year(self,year,last_year):
        query = text("SELECT id,name FROM class where year >= :year and year <= :last_year").bindparams(year = year,last_year = last_year)
        classes = pd.read_sql(query,self.__con)
        return classes

    def _get_class_for_author_id(self,author_id):
        query = text("SELECT id,name FROM class where author_id = :author_id").bindparams(author_id = author_id)
        classes = pd.read_sql(query,self.__con)
        return classes

    def _get_lists_for_class(self,list_id):
        query = text("SELECT list_id,class_id FROM classhaslistquestion where list_id = :list_id").bindparams(list_id = list_id)
        lists = pd.read_sql(query,self.__con)
        return lists

    def _get_user(self,name):
        query = text("SELECT id,name FROM user where name like :name").bindparams(name = f'%{name}%')
        usuarios = pd.read_sql(query,self.__con)
        return usuarios

    def _get_student_for_class(self,class_id):
        query = text("SELECT id,enrollment,user_id,class_id FROM classhasuser where class_id = :class_id").bindparams(class_id = class_id)
        students = pd.read_sql(query,self.__con)
        return students

    def _get_submissions_for_student(self,user_id,list_id,class_id):
        query = text("SELECT id,hitPercentage,timeConsuming,user_id,question_id,char_change_number,answer \
        FROM submission WHERE listQuestions_id = :list_id and user_id = :user_id and class_id = :class_id").bindparams(list_id = list_id,user_id = user_id, class_id = class_id)
        submission_student = pd.read_sql(query,self.__con)
        return submission_student

    def _get_submissions_for_class_and_list(self,class_id,list_id):
        query = text("SELECT id,hitPercentage,user_id FROM submission where class_id = :class_id\
        and listQuestions_id = :list_id").bindparams(class_id = class_id,list_id = list_id)
        submissions_class = pd.read_sql(query,self.__con)
        return submissions_class

    def _get_submissions_for_class(self,class_id):
        query = text("SELECT id,hitPercentage,user_id FROM submission where class_id = :class_id").bindparams(class_id = class_id)
        submissions = pd.read_sql(query,self.__con)
        return submissions
    
    def _get_questions_for_list(self,list_id):
        query = text("SELECT id FROM listhasquestion where list_id = :list_id").bindparams(list_id = list_id)
        questions = pd.read_sql(query,self.__con)
        return questions

    def _get_list_name(self,list_id):
        query = text("SELECT title FROM listQuestions where id = :list_id").bindparams(list_id = list_id)
        title = pd.read_sql(query,self.__con)
        return title.title.unique()[0]
    
    def _get_hits_student(self,id_student):
        id_student = id_student.query('hitPercentage == 100')
        acertos = id_student.question_id.unique()
        acertos = len(acertos)
        return acertos

    def _get_caracteres_for_question(self,id_student):
        soma_caracter = 0
        for i in range(len(id_student.answer.unique())):
            tamanho_da_string = len(id_student.answer.unique()[i])
            soma_caracter = soma_caracter + tamanho_da_string
        return soma_caracter
    

class DataExtractorForMachineLearning(DataExtractor):
    def __init__(self,conector):
        DataExtractor.__init__(conector)
    
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
    
    
        

