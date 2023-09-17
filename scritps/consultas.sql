-- 10 filmes menos lucrativos
SELECT *   
FROM moviesdb.tbl_movies
ORDER BY CAST(REPLACE(REPLACE(lucro, '$', ''), ',', '') AS SIGNED) ASC 
LIMIT 10;

-- 10 filmes mais lucrativos
SELECT *   
FROM moviesdb.tbl_movies
ORDER BY CAST(REPLACE(REPLACE(lucro, '$', ''), ',', '') AS SIGNED) DESC 
LIMIT 10;

-- 10 piores avaliações
SELECT * 
FROM moviesdb.tbl_movies
WHERE vote_count > 1
ORDER BY vote_average ASC
LIMIT 10;
-- 10 melhores avaliações
SELECT * 
FROM moviesdb.tbl_movies
ORDER BY vote_average DESC
LIMIT 10;

# Top 10 generos
SELECT id_genres, COUNT(id_genres) AS qtd_repete_genero 
FROM moviesdb.tbl_genres 
GROUP BY id_genres
ORDER BY qtd_repete_genero DESC
LIMIT 10;

SELECT * FROM moviesdb.tbl_genres order by id_genres desc limit 5;
SELECT * FROM moviesdb.tbl_movies limit 5 ;

-- WITH cria a tbl temporaria RankedMovies com todos id_genres e seus filmes e suas avaliacoes ainda duplicados
-- ROW NUMBER + PARTITION BY ira armazenardentro rn de forma enumerada os registros coletados de cada genero
-- Exemplo
-- id_genres = 12 rn = 1
-- id_genres = 12 rn = 2
-- id_genres = 10770 rn = 1
-- id_genres = 10770 rn = 2
-- e ainda pega id_genres com a maior nota e maior vote count e armazena no rn=1
-- no segundo select é inserido o rn = 1 no where
# Filme com melhor avaliação de cada um dos generos
WITH RankedMovies AS (
  SELECT g.id_genres, m.id_movies, m.title, m.vote_average, m.vote_count,
    ROW_NUMBER() OVER(PARTITION BY g.id_genres ORDER BY m.vote_average DESC, m.vote_count DESC) AS rn
  FROM
    moviesdb.tbl_genres g
    INNER JOIN moviesdb.tbl_movies m ON g.id_movies = m.id_movies
)
SELECT id_genres, id_movies, title, vote_average, vote_count
FROM
  RankedMovies
WHERE
  rn = 1;

