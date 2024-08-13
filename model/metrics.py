from sqlalchemy import create_engine
from sqlalchemy import text
from sqlalchemy import bindparam
from db.extrator import DataExtractor
import pandas as pd

class questions(DataExtractor):
    def __init__(self,id,conect_db):
        DataExtractor.__init__(self,conect_db)
        self.id = id

    def __str__(self):
        return f'{self._get_question_name(self.id)}'
    
    def submission_by_class(self,class_id):
        return (self._get_submissions_by_questions_by_class(self.id,class_id).id.nunique())

    def hits_question(self,class_id):
        return (self._get_submissions_by_questions_by_class(self.id,class_id)).query('hitPercentage == 100').id.nunique()

    def difficulty_by_class(self,class_id):
        submissions = self.submission_by_class(class_id)
        submissions_corrects = self.hits_question(class_id)
        percentage = round((abs(submissions_corrects - submissions)/submissions)*100,2)
        return percentage
    


class lists:
    pass

class subjects:
    pass



