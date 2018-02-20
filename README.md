# Where is my URI?

# Abstract:
One of the Semantic Web foundations is the possibility to dereference URIs to let applications negotiate their semantic content.
However, this exploitation is often infeasible as the availability of such information depends on the reliability of networks, services, and human factors.
Moreover, it has been shown that around 21% of the information published as Linked Open Data is available as data dumps only and 58% of endpoints are offline.
To this end, we propose a Semantic Web service called Where is my URI?.
Our service aims at indexing URIs and their use in order to let Linked Data consumers find the respective RDF data source, in case such information cannot be retrieved from the URI alone.
We rank the corresponding datasets by following the rationale upon which a dataset contributes to the definition of a URI proportionally to the number of datatype triples.
We finally show use-cases of applications that can immediately benefit from our simple yet useful service.

## [The manual](https://docs.google.com/document/d/e/2PACX-1vTScPtFq0k1hHvSgColYga5Vt-_69WVsrKkwsf3R7JOsrCVehkVOWKevf8Amjh2zYqLLq9zMENQUe-Q/pub)

# Requirements
-`Java 8`.

-`Jena`.

-`Tomcat 6 (minimal)`. 

# Build instructions:

https://github.com/dice-group/docker-wimu

Download the index in Apache Lucene:

http://wimu-data.aksw.org/WimuLuceneIndex.zip

To create the index:
<pre>
java -jar wimu.jar create dumpDir ldir < dbpedia / dumps / endpoints / hdt / all / * >
</pre>  
where:
- `dbpedia - Process only Dumps from DBpedia.`
- `dumps - Process all Dumps from the whole LODStats.`
- `endpoints - All Endpoints from LODstats.`
- `hdt - Process HDT files from LODLoundromat and rdfhdt.org.`
- `all - everything.` 

To search in the index via command line:
<pre>
java -jar wimu.jar search < URI >
</pre>  
where:
- `URI - specific URI to search.`

Download [wimu.jar](https://goo.gl/wFBydb)

You can also clone the whole repository in which will include both projects (Database index creation and the webservice).
or download the 2 eclipse projects from [here](http://wimu-data.aksw.org/src_wimu_eclipse.zip).

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
