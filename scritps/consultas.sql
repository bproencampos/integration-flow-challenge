-- 10 filmes menos lucrativos
SELECT *   
FROM moviesdb.tbl_movies
ORDER BY CAST(REPLACE(REPLACE(lucro, '$', ''), ',', '') AS SIGNED) ASC 
LIMIT 10;

-- 10 filmes mais lucrativos
SELECT *   
FROM dbteste.tbl_movies_3
ORDER BY CAST(REPLACE(REPLACE(lucro, '$', ''), ',', '') AS SIGNED) DESC 
LIMIT 10;

-- 10 piores avaliações
SELECT * 
FROM dbteste.tbl_movies_3
WHERE vote_count > 1
ORDER BY vote_count ASC
LIMIT 10;

-- 10 melhores avaliações
SELECT * 
FROM dbteste.tbl_movies_3
WHERE vote_count > 1
ORDER BY vote_count DESC
LIMIT 10;