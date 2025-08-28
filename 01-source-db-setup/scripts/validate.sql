-- Count movies per genre
SELECT g.GenreName,
       COUNT(m.MovieID) AS MovieCount
FROM Genres g
LEFT JOIN Movies m ON g.GenreID = m.GenreID
GROUP BY g.GenreName
ORDER BY MovieCount DESC;

-- Count reviews per movie
SELECT m.MovieID,
       m.Title,
       COUNT(r.ReviewID) AS ReviewCount
FROM Movies m
LEFT JOIN Reviews r ON m.MovieID = r.MovieID
GROUP BY m.MovieID, m.Title
ORDER BY ReviewCount DESC;

