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

# Top 10 generos
SELECT id_genres, COUNT(id_genres) AS qtd_repete_genero 
FROM moviesdb.tbl_genres 
GROUP BY id_genres
ORDER BY qtd_repete_genero DESC
LIMIT 10;

SELECT * FROM moviesdb.tbl_genres ;

# Filme com melhor avaliação por cada genero
SELECT B.id_genres, B.name_genres, A.title, A.id_movies, A.vote_count, B.id_movies AS genres_id_movies
FROM (SELECT DISTINCT id_genres, name_genres FROM moviesdb.tbl_genres) B
LEFT JOIN moviesdb.tbl_movies A
ON A.id_movies = B.genres_id_movies
ORDER BY A.vote_count DESC;


