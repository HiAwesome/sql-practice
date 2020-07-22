select AlbumId, AlbumName AS ABC, ReleaseDate, ArtistId, GenreId
from Music.dbo.Albums AS lll;


-- 查询课程编号 “002” 的成绩比课程编号 “001” 课程低的所有同学的学号、姓名
-- 查询本人课程编号 “002” 的成绩比本人课程编号 “001” 课程低的所有同学的学号、姓名
-- 查询本人课程编号 “002” 的成绩比本人课程编号 “001” 课程低的所有同学的学号


/*select c.sid, Sname
from ((select sid, score
       from sc
       where cid = '002') AS a left join
    (select sid, score
     from sc
     where cid = '001') AS b on a.sid = b.sid where a.score > b.score) AS c
         left join Student on c.sid = Student.sid;
*/

select a.s_id
from (select s_id, score
      from Music.dbo.Score
      where c_id = 2) AS a,
     (select s_id, score
      from Music.dbo.Score
      where c_id = 1) AS b
where a.s_id = b.s_id
  and a.score > b.score