#Problem 4. Find a way to export data from Neo4j into a set of CSV files. 
#Delete your database and demonstrate that you can recreate the database
#by loading those CSV files. Please use any programming language of your convenience: Java, Python, R, or Scala.

#import libraries
from neo4j.v1 import GraphDatabase, basic_auth

#conn to neo4j instance
driver = GraphDatabase.driver("bolt://localhost:7687",auth=basic_auth("neo4j", "neo4j2"))
session = driver.session()

#gets all actor node labels write to a csv file and load them into the databse
result = session.run("""
LOAD CSV WITH HEADERS FROM "file:///movies.csv" AS line
CREATE (m:Movie {title:line.title, year:line.year});
""")

#same for actors
result = session.run("""
LOAD CSV WITH HEADERS FROM "file:///actors.csv" AS line
CREATE (a:Actor {name:line.name});
""")

#same for directors
result = session.run("""
LOAD CSV WITH HEADERS FROM "file:///directors.csv" AS line
CREATE (d:Director {name:line.name});
""")


#finally relationships for actors
result = session.run("""
LOAD CSV WITH HEADERS FROM "file:///acts-in.csv" AS line
MATCH (m:Movie) WHERE m.title = line.title 
MATCH (a:Actor) WHERE a.name = line.name
CREATE (a)-[:ACTS_IN {role:line.role}]->(m)
""")

#and for directors
result = session.run("""
LOAD CSV WITH HEADERS FROM "file:///director-of.csv" AS line
MATCH (d:Director) WHERE d.name = line.name
MATCH (m:Movie) WHERE m.title = line.title and m.year=line.year
CREATE (d)-[:DIRECTOR_OF]->(m)
""")


#close session
session.close() 

