SET mapred.child.java.opts -Xmx4096m;
SET io.sort.mb 200;
SET io.sort.factor 100;
SET io.file.buffer.size 131072;
--SET pig.maxCombinedSplitSize 1073741824;
a = load '/temporary/kbig/part-r-00000' using JsonLoader('id:chararray,text:chararray,user:chararray,date:chararray,oriTweetID:chararray');
b = filter a by not oriTweetID matches 'null';
c = foreach b generate flatten(TOKENIZE(text,' ')) as word;
d = group c by word;
e = foreach d generate COUNT(c),group;
f = order e by $0 desc;
store f into '/temporary/result/test3';



/*d = foreach c generate flatten(TOKENIZE($0,' ')) as word;
e = group d by word;
f = foreach e generate COUNT(d), group;
dump f;*/
--f = 
--dump c;
--d = filter c by date matches 'T.*';
--dump d;

--c = filter b by d matches 'T.*';
--d = foreach c generate com.naver.nelo2analyzer.udf.HNNSPLT(b) as word, d as d;
--dump c;


