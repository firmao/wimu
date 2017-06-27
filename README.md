# Where is my URI?

# Abstract:
One of the Semantic Web foundations is the possibility to dereference URIs to let applications negotiate their semantic content.
However, this exploitation is often infeasible as the availability of such information depends on the reliability of networks, services, and human factors.
Moreover, it has been shown that around 21% of the information published as Linked Open Data is available as data dumps only and 58% of endpoints are offline.
To this end, we propose a Semantic Web service called Where is my URI?.
Our service aims at indexing URIs and their use in order to let Linked Data consumers find the respective RDF data source, in case such information cannot be retrieved from the URI alone.
We rank the corresponding datasets by following the rationale upon which a dataset contributes to the definition of a URI proportionally to the number of datatype triples.
We finally show use-cases of applications that can immediately benefit from our simple yet useful service.

# Requirements
-`Java 8`.

-`Jena`.

-`Tomcat 6`. 

-`MySQL (latest version) and the Java driver to access the database`.

# Build instructions:

## Database index (creating locally):

Create the structure of the database inside MySql:
https://github.com/firmao/linklion2/blob/master/DB_Tables_StoredProcedure.sql

Donwload the Database index from: https://doi.org/10.6084/m9.figshare.5005241.v1
and import into the Database linklion2, with the command:

<pre>
mysql -u root -p linklion2 < backup_linklion2_final3.sql
</pre>

- `DataBase type`: MySQL

- `DataBase name`: linklion2

- `username`: root

- `password`: sameas

Backup Database:
<pre>
mysqldump -u root -p linklion2 > BackupFile.sql
</pre>

## In video:
https://youtu.be/oPGxZJvDSSw

If you wanna to create your own Database index of URIs from LODStats (takes around 3 days with a 64 cores machine) using the following class:
https://github.com/firmao/wimu/blob/master/wimu_db/src/FirstOptimization.java

## In video:
https://youtu.be/13cwc_UwfPc

# Semantic Web service
The service to access the Database works with the Tomcat 6.

https://github.com/firmao/wimu/tree/master/wimu_ws

## Example of usage (Command line mode):
<pre>
curl http://139.18.8.58:8080/LinkLion2_WServ/Find?uri=http://semanticscience.org/resource/SIO_000272

Output(JSON):
[{"EndPoint":"http://biordf.net/sparql","CDataType":"1"},{"EndPoint":"http://lov.okfn.org/dataset/lov/sparql","CDataType":"4"}]

</pre>

You can also clone the whole repository in which will include both projects (Database index creation and the webservice).

## In video:
https://youtu.be/-ZMcfhfYjm0

# Contact
In case of questions, feel free to contact André Valdestilhas <valdestilhas@informatik.uni-leipzig.de> or Tommaso Soru <tsoru@informatik.uni-leipzig.de>

# Zenodo
Share

Cite as

Andre Valdestilhas. (2017, June 27). firmao/wimu: Where is my URI?. Zenodo. http://doi.org/10.5281/zenodo.819761
## BibTex
<pre>
@misc{andre_valdestilhas_2017_819761,
  author       = {Andre Valdestilhas},
  title        = {firmao/wimu: Where is my URI?},
  month        = jun,
  year         = 2017,
  doi          = {10.5281/zenodo.819761},
  url          = {https://doi.org/10.5281/zenodo.819761}
}
</pre>

## Acknowledgment
This research has been partially supported by CNPq Brazil under grants No. 201536/2014-5 and H2020 projects SLIPO (GA no. 731581) and HOBBIT (GA no. 688227) as well as the DFG project LinkingLOD (project no. NG 105/3-2), the BMWI Projects SAKE (project no. 01MD15006E) and GEISER (project no. 01MD16014).

# License

licensed under Apache 2.0
