#Problem 4. Find a way to export data from Neo4j into a set of CSV files. 
#Delete your database and demonstrate that you can recreate the database
#by loading those CSV files. Please use any programming language of your convenience: Java, Python, R, or Scala.

#import libraries
from neo4j.v1 import GraphDatabase, basic_auth

#conn to neo4j instance
driver = GraphDatabase.driver("bolt://localhost:7687",auth=basic_auth("neo4j", "neo4j2"))
session = driver.session()

#gets all actor node labels write to a csv file
result = session.run("""
MATCH (n:Actor) 
RETURN n.name as name;
""")

f = open('d:\\temp\\actors.csv','w')
f.write("name\n")
for record in result:
	f.write(record["name"] +"\n")
f.close()

#same fo director node lables
result = session.run("""
MATCH (d:Director) 
RETURN d.name as name;
""")

f = open('d:\\temp\\directors.csv','w')
f.write("name\n")
for record in result:
	f.write(record["name"] +"\n")
f.close()

#same for movie node lables
result = session.run("""
MATCH (m:Movie) 
RETURN m.title as title, m.year as year
""")

f = open('d:\\temp\\movies.csv','w')
f.write("title,year\n")
for record in result:
	f.write(record["title"] + "," + str(record["year"]) + "\n")
f.close()

#same for relationship types for actors
f = open('d:\\temp\\acts-in.csv','w')
f.write("name,role,year,title\n")
result = session.run("""
MATCH p=(a:Actor)-[r:ACTS_IN]->(m:Movie) 
RETURN a.name as name, r.role as role, m.year as year, m.title as title
""")
for record in result:
	f.write(record["name"] + "," + record["role"] + "," + str(record["year"]) + "," + record["title"] + "\n")
f.close()		


#same for relationship types for directors
f = open('d:\\temp\\director-of.csv','w')
f.write("name,year,title\n")
result = session.run("""
MATCH p=(d:Director)-[r:DIRECTOR_OF]->(m:Movie) 
RETURN d.name as name, m.year as year, m.title as title
""")
for record in result:
	f.write(record["name"] + "," + str(record["year"]) + "," + record["title"] + "\n")
f.close()		

#close session
session.close() 

