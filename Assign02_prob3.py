#Problem 3. Find a list of actors playing in movies in which Keanu Reeves played. 
#Find directors of movies in which K. Reeves played. Please use any programming
#language of your convenience. Verify your results using Cypher queries in Cypher Browser

#import libraries
from neo4j.v1 import GraphDatabase, basic_auth

#conn to neo4j instance
driver = GraphDatabase.driver("bolt://localhost:7687",auth=basic_auth("neo4j", "neo4j2"))

#run creation queries: actors in same films as Keanue Reeves
session = driver.session()
result = session.run("""
MATCH (k:Actor) WHERE k.name = "Keanu Reeves"
MATCH (a:Actor) WHERE [k:ACTED_IN] and a<>k  return a.name as name
""")

#print actors
print "List of actors that played in same movies as Keanue Reeves:"
for record in result:      
      print("%s" % (record["name"]))


#run creation queries: directors in same films as Keanue Reeves
result = session.run("""
MATCH (k:Actor) WHERE k.name = "Keanu Reeves"
MATCH (d:Director) WHERE [k:ACTED_IN] return d.name as name
""")

#print directors
print "List of directors that directed Keanue Reeves:"
for record in result:      
      print("%s" % (record["name"]))
 
 #close session
session.close() 

