raw = LOAD 'popular/PopularWords' USING PigStorage(',') AS (word: chararray, sum: int);

ordered = ORDER raw BY sum desc;

limited = LIMIT ordered 10;

STORE limited INTO 'output';