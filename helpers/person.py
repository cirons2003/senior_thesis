import random 
import math 

class Person():
    def __init__(self): 
        self.gender = ""
        self.drugs = ""
        self.job = ""
        self.drinks = ""
        self.pets = ""
        self.religion = ""
        self.freestyle = ""
        self.age = ""
        self.smokes = ""
       
        self.essays = []
        self.description = ""

        self.query = ""

        self.includeDrugs = 0.4
        self.includeJob = 1
        self.includeDrinks = 0.5
        self.includePets = 0.8
        self.includeReligion = 0.5
        self.includeSmokes = 0.5

    def setGender(self, gender): 
        if gender == 'm':
            self.gender = 'male'
        elif gender == 'f':
            self.gender = 'female'
        else: 
            self.gender = ""

    def setAge(self, age): 
        self.age = str(age)

    def setDiet(self, diet): 
        self.diet = diet

    def setDrugs(self, drugs): 
        self.drugs = drugs

    def setJob(self, job):
        self.job = job

    def setDrinks(self, drinks):
        self.drinks = drinks

    def setPets(self, pets):
        self.pets = pets

    def setReligion(self, religion):
        self.religion = religion

    def setSmokes(self, smokes):
        self.smokes = smokes

    def __createFreestyle(self): 
        if (len(self.essays) == 0):
            return

        descriptionStartText = "The first essay is: " 
        essayDelimiter = "The next essay is: "

        self.description = descriptionStartText + '"' + self.essays[0] + '" '
        for essay in self.essays[1:]: 
            if validateString(essay):
                self.description += essayDelimiter
                self.description += '"' + essay + '" '

    def setEssays(self, essays): 
        for essay in essays: 
            if essay and validateString(essay):
                self.essays.append(essay)

    def generateDescription(self): 
        self.query = ""
        
        if validateString(self.gender) and validateString(self.age):
            self.query += "This person is a " + self.age + " year old " + self.gender + ". "
        else: 
            return "Insufficient information"

        if validateString(self.job) and random.random() <= self.includeJob:
            self.query += "They work in " + self.job + '. '

        if validateString(self.drugs) and random.random() <= self.includeDrugs :
            self.query += "They " + self.drugs + " do drugs. "

        if validateString(self.drinks) and random.random() <= self.includeDrinks:
            self.query += "They " + self.drinks + " drink alcohol. "

        if validateString(self.smokes) and random.random() <= self.includeSmokes:
            self.query += "Their smoking status: " + self.smokes + ". "

        if validateString(self.pets) and random.random() <= self.includePets: 
            self.query += "This person " + self.pets + ". "

        if validateString(self.religion) and random.random() <= self.includeReligion:
            self.query += "Their religion is " + self.religion + ". "
            
        self.__createFreestyle()
        if self.description:
            self.query += "They included the following essay responses for unknown prompts..." + self.description + ". "
        return self.query
    
def validateString(string: str):
    if not string or isinstance(string, float): 
        return False
    return True

def generatePerson(row) -> Person:
    person = Person()
    person.setGender(row["sex"])
    person.setAge(row["age"])
    person.setDrinks(row["drinks"])
    person.setDrugs(row["drugs"])
    person.setJob(row["job"])
    person.setPets(row["pets"])
    person.setReligion(row["religion"])
    person.setSmokes(row["smokes"])
    essays = []
    numEssays = 10
    for i in range(numEssays): 
        essays.append(row["essay"+str(i)])
    person.setEssays(essays)
    return person