import sqlite3
from datetime import datetime
from random import randint, choice
from faker import Faker
import pandas as pd

def create_people_table():
    con = sqlite3.connect('social_network.db')
    cur = con.cursor()
    
    create_people_tbl_query = """
        CREATE TABLE IF NOT EXISTS people
        (
            id INTEGER PRIMARY KEY,
            name TEXT NOT NULL
        );
    """
    
    cur.execute(create_people_tbl_query)
    con.commit()
    con.close()

def populate_people_table():
    con = sqlite3.connect('social_network.db')
    cur = con.cursor()
    
    add_person_query = """
        INSERT OR IGNORE INTO people
        (
            id,
            name
        )
        VALUES (?, ?);
    """
    
    fake = Faker()
    
    for i in range(1, 201):
        name = fake.name()
        cur.execute(add_person_query, (i, name))
    
    con.commit()
    con.close()

def create_relationships_table():
    con = sqlite3.connect('social_network.db')
    cur = con.cursor()
    
    create_relationships_tbl_query = """
        CREATE TABLE IF NOT EXISTS relationships
        (
            id INTEGER PRIMARY KEY,
            person1_id INTEGER NOT NULL,
            person2_id INTEGER NOT NULL,
            type TEXT NOT NULL,
            start_date TEXT NOT NULL,
            FOREIGN KEY (person1_id) REFERENCES people (id),
            FOREIGN KEY (person2_id) REFERENCES people (id)
        );
    """
    
    cur.execute(create_relationships_tbl_query)
    con.commit()
    con.close()

def populate_relationships_table():
    con = sqlite3.connect('social_network.db')
    cur = con.cursor()
    
    add_relationship_query = """
        INSERT INTO relationships
        (
            person1_id,
            person2_id,
            type,
            start_date
        )
        VALUES (?, ?, ?, ?);
    """
    
    fake = Faker()
    
    for _ in range(100):
        person1_id = randint(1, 200)
        person2_id = randint(1, 200)
        while person2_id == person1_id:
            person2_id = randint(1, 200)
        rel_type = choice(('friend', 'spouse', 'partner', 'relative'))
        start_date = fake.date_between(start_date='-50y', end_date='today').isoformat()
        new_relationship = (person1_id, person2_id, rel_type, start_date)
        cur.execute(add_relationship_query, new_relationship)
    
    con.commit()
    con.close()

def generate_married_couples_report():
    con = sqlite3.connect('social_network.db')
    cur = con.cursor()
    
    married_couples_query = """
        SELECT person1.name, person2.name, start_date
        FROM relationships
        JOIN people person1 ON person1_id = person1.id
        JOIN people person2 ON person2_id = person2.id
        WHERE type = 'spouse';
    """
    
    cur.execute(married_couples_query)
    married_couples = cur.fetchall()
    con.close()
    
    df = pd.DataFrame(married_couples, columns=['Person 1', 'Person 2', 'Start Date'])
    df.to_csv('married_couples.csv', index=False)

if __name__ == "__main__":
    create_people_table()
    populate_people_table()
    create_relationships_table()
    populate_relationships_table()
    generate_married_couples_report()
