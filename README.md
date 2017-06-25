# Where is my URI?

# Abstract:
One of the Semantic Web foundations is the possibility to dereference URIs to let applications negotiate their semantic content.
However, this exploitation is often infeasible as the availability of such information depends on the reliability of networks, services, and human factors.
Moreover, it has been shown that around 21% of the information published as Linked Open Data is available as data dumps only and 58% of endpoints are offline.
To this end, we propose a Semantic Web service called Where is my URI?.
Our service aims at indexing URIs and their use in order to let Linked Data consumers find the respective RDF data source, in case such information cannot be retrieved from the URI alone.
We rank the corresponding datasets by following the rationale upon which a dataset contributes to the definition of a URI proportionally to the number of datatype triples.
We finally show use-cases of applications that can immediately benefit from our simple yet useful service.

# Instructions to use:
Firstly you need Java 8, Tomcat 6 and MySQL (latest version).
You should create your Database index of URIs from LODStats (takes around 3 days with a 64 cores machine) using the following class:

https://github.com/firmao/wimu/blob/master/wimu_db/src/FirstOptimization.java

# Semantic Web service
The service to access the Database run with the Tomcat 6.

https://github.com/firmao/wimu/tree/master/wimu_ws

# Database:
https://doi.org/10.6084/m9.figshare.5005241.v1

DataBase type: MySQL

DataBase name: linklion2

username: root

password: sameas

Only the structure of the database:
https://github.com/firmao/linklion2/blob/master/DB_Tables_StoredProcedure.sql

Restore Database:

mysql -u root -p linklion2 < file.sql

Backup Database:

mysqldump -u root -p linklion2 > file.sql


# License

licensed under Apache 2.0
