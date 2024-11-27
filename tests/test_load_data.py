import unittest
import pandas as pd
from datetime import date
from sqlalchemy import create_engine
from sqlalchemy.orm import sessionmaker
from sqlalchemy.ext.declarative import declarative_base
from sqlalchemy import Column, Integer, String, Float, Date

# adding ORM base and model, the data in pipeline.models is not able to import to the file
Base = declarative_base()

class VantaaOpenApplications(Base):
   __tablename__ = "vantaa_open_applications"
   id = Column(Integer, primary_key=True)
   job_title = Column(String)
   job_key = Column(String)
   address = Column(String)
   application_end_date = Column(Date)
   longitude_wgs84 = Column(Float)
   latitude_wgs84 = Column(Float)
   link = Column(String)


from pipeline.etl import SimpleLoader
#from pipeline.const import TABLE_NAME
#from pipeline.utils import initialize_database
#from pipeline.models import VantaaOpenApplications

class TestSimpleLoader(unittest.TestCase):
    def setUp(self):              #Setup the in-memory SQLite database and initialize SimpleLoader.
        self.conn_str = 'sqlite:///:memory:'
        self.loader = SimpleLoader(self.conn_str)

    
        # Create the database schema
        Base.metadata.create_all(self.loader.engine)

    def tearDown(self):    #Drop all tables to ensure a clean slate for each test."""
        Base.metadata.drop_all(self.loader.engine)

    def test_load_data(self):       #Test loading data into the database.
        sample_data = pd.DataFrame([
            {
                "id": 1,
                "job_title": "Engineer",
                "job_key": "ENG001",
                "address": "Helsinki",
                "application_end_date": date(2024, 12, 31),
                "longitude_wgs84": 24.6899,
                "latitude_wgs84": 60.5040,
                "link": "https://example.com/job1"
            },
            {
                "id": 2,
                "job_title": "Test Engineer",
                "job_key": "TES001",
                "address": "Espoo",
                "application_end_date": date(2024, 11, 30),
                "longitude_wgs84": 24.6559,
                "latitude_wgs84": 60.2055,
                "link": "https://example.com/job2"
            }
        ])

        self.setUp()
        # Load data into the database
        self.loader(sample_data)

        # Query the database to verify the data
        Session = sessionmaker(bind=self.loader.engine)
        with Session() as session:
            results = session.query(VantaaOpenApplications).all()
            print(results)
            self.assertEqual(len(results), 2)  # Check if all records are loaded
            self.assertEqual(results[0].job_title, "Engineer")  # Verify first data
            self.assertEqual(results[1].job_title, "Test Engineer")  # Verify second data


    def test_empty_dataframe(self):          #Test behavior when an empty dataframe is provided.
        self.setUp()
        empty_df = pd.DataFrame()
        self.loader(empty_df)
        Session = sessionmaker(bind=self.loader.engine)
        with Session() as session:
            results = session.query(VantaaOpenApplications).all()
            self.assertEqual(len(results), 0)  # Ensure no records are added


if __name__ == '__main__':
    unittest.main()






