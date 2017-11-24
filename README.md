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

- `Apache Lucene 4.4`.

# Build instructions:

## Apache Lucene index

java -jar wimu.jar search <URI> <MAX_RESULTS_LUCENE> <LUCENE_INDEX_DIR>
java -jar wimu.jar searcg <URI> <MAX_RESULTS_LUCENE> <LUCENE_INDEX_DIR> <optional-LUCENE_INDEX_DIR_1,...,LUCENE_INDEX_DIR_N>

java -jar wimu.jar create <DUMP_DIR> <LUCENE_NAME_DIR> <dbpedia>
java -jar wimu.jar create <DUMP_DIR> <LUCENE_NAME_DIR> <lodstats>
java -jar wimu.jar create <DUMP_DIR> <LUCENE_NAME_DIR> <endpoints>
  

## In video:
https://youtu.be/13cwc_UwfPc

# Semantic Web service
The service to access the Database works with the Tomcat 6.

https://github.com/dice-group/wimu/tree/master/wimu_ws

## Example of usage (Command line mode):
<pre>
curl http://139.18.8.58:8080/wimu_ws/Find?uri=http://semanticscience.org/resource/SIO_000272

Output(JSON):
[{"EndPoint":"http://biordf.net/sparql","CDataType":"1"},{"EndPoint":"http://lov.okfn.org/dataset/lov/sparql","CDataType":"4"}]

</pre>
## Demonstration via website:

http://139.18.8.58:8080/LinkLion2_WServ/


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
  author       = {Andre Valdestilhas, Tommaso Soru, Markus Nentwig, Edgard Marx and Axel-Cyrille Ngonga Ngomo},
  title        = {firmao/wimu: Where is my URI?},
  month        = jun,
  year         = 2017,
  doi          = {10.5281/zenodo.819761},
  url          = {https://doi.org/10.5281/zenodo.819761}
}
</pre>

## Acknowledgment
This research has been partially supported by CNPq Brazil under grants No. 201536/2014-5 and H2020 projects SLIPO (GA no. 731581) and HOBBIT (GA no. 688227) as well as the DFG project LinkingLOD (project no. NG 105/3-2), the BMWI Projects SAKE (project no. 01MD15006E) and GEISER (project no. 01MD16014E).

# License

licensed under AGPL 2.0
