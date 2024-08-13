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
        self.engine = create_engine(conect)
        self.con = self.engine.connect()

    def _conect_of(self):
        self.con.close()

    def _get_class_for_year(self,year,last_year):
        query = text("SELECT id,name FROM class where year >= :year and year <= :last_year").bindparams(year = year,last_year = last_year)
        classes = pd.read_sql(query,self.con)
        return classes

    def _get_class_for_author_id(self,author_id):
        query = text("SELECT id,name FROM class where author_id = :author_id").bindparams(author_id = author_id)
        classes = pd.read_sql(query,self.con)
        return classes

    def _get_lists_for_class(self,list_id):
        query = text("SELECT list_id,class_id FROM classhaslistquestion where list_id = :list_id").bindparams(list_id = list_id)
        lists = pd.read_sql(query,self.con)
        return lists

    def _get_user(self,name):
        query = text("SELECT id,name FROM user where name like :name").bindparams(name = f'%{name}%')
        usuarios = pd.read_sql(query,self.con)
        return usuarios

    def _get_student_for_class(self,class_id):
        query = text("SELECT id,enrollment,user_id,class_id FROM classhasuser where class_id = :class_id").bindparams(class_id = class_id)
        students = pd.read_sql(query,self.con)
        return students

    def _get_submissions_for_student(self,user_id,list_id,class_id):
        query = text("SELECT id,hitPercentage,timeConsuming,user_id,question_id,char_change_number,answer \
        FROM submission WHERE listQuestions_id = :list_id and user_id = :user_id and class_id = :class_id").bindparams(list_id = list_id,user_id = user_id, class_id = class_id)
        submission_student = pd.read_sql(query,self.con)
        return submission_student

    def _get_submissions_for_class_and_list(self,class_id,list_id):
        query = text("SELECT id,hitPercentage,user_id FROM submission where class_id = :class_id\
        and listQuestions_id = :list_id").bindparams(class_id = class_id,list_id = list_id)
        submissions_class = pd.read_sql(query,self.con)
        return submissions_class

    def _get_submissions_for_class(self,class_id):
        query = text("SELECT id,hitPercentage,user_id FROM submission where class_id = :class_id").bindparams(class_id = class_id)
        submissions = pd.read_sql(query,self.con)
        return submissions
    
    def _get_submissions_by_questions_by_class(self,question_id,class_id):
        query = text("SELECT id FROM submission where question_id = :question_id and class_id = :class_id").bindparams(question_id = question_id,class_id = class_id)
        submissions = pd.read_sql(query,self.con)
        return submissions.id.nunique()
    
    def _get_questions_for_list(self,list_id):
        query = text("SELECT id FROM listhasquestion where list_id = :list_id").bindparams(list_id = list_id)
        questions = pd.read_sql(query,self.con)
        return questions

    def _get_list_name(self,list_id):
        query = text("SELECT title FROM listQuestions where id = :list_id").bindparams(list_id = list_id)
        title = pd.read_sql(query,self.con)
        return title.title.unique()[0]
    
    def _get_question_name(self,question_id):
        query = text("SELECT title FROM questions where id = :question_id").bindparams(question_id = question_id)
        title = pd.read_sql(query,self.con)
        return title
    
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
    
    def _get_amount_questions_by_subject(self,lists):
        listas_com_quantidade = []
        for questoes in range(len(lists)):
            query = text("SELECT list_id,question_id from listHasQuestion where list_id = :questoes").bindparams(questoes = lists[questoes])
            df = pd.read_sql(query,self.con)
            df = df.question_id.nunique()
            listas_com_quantidade.append(df)
        return sum(listas_com_quantidade)
    
    def _get_submissions_by_subject(self,listas_por_assunto,user_student,class_student):
        submissoes_por_assunto = []
    
        for assuntos in listas_por_assunto:
            query = text("SELECT id,hitPercentage,timeConsuming,user_id,question_id,char_change_number,answer,class_id \
            FROM submission WHERE listQuestions_id = :list01 \
            AND class_id = :id_class AND user_id = :id_aluno\
            union all \
            SELECT id,hitPercentage,timeConsuming,user_id,question_id,char_change_number,answer,class_id \
            FROM submission WHERE listQuestions_id = :list02 \
            AND class_id = :id_class AND user_id = :id_aluno\
            union all\
            SELECT id,hitPercentage,timeConsuming,user_id,question_id,char_change_number,answer,class_id \
            FROM submission WHERE listQuestions_id = :list03 \
            AND class_id = :id_class AND user_id = :id_aluno").bindparams(list01 = assuntos[0],list02 = assuntos[1],list03 = assuntos[2],id_class = class_student,id_aluno = user_student)
            df = pd.read_sql(query,self.con)
            submissoes_por_assunto.append(df)
        return submissoes_por_assunto




        
    

        

