java -jar search < uri > < num_max_results > < lucenedir >

java -jar create < dump_dir > < lucene_name_dir > < dbpedia / lodstats / endpoints / all > < logFileName >

example:
<pre>
java -Xmx80G -jar wimuLucene.jar create dumpDir luceneDir dumps logwimuLucene.txt > logwimuLucene.txt 2>&1 &
</pre>
