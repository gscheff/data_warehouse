-- analytics

-- time
select t.weekday,
       u.level,
       u.gender,
       count(*) num
from songplays
         join times t on songplays.start_time = t.start_time
         join users u on songplays.user_id = u.user_id
where t.year = 2018
group by t.weekday, u.level, u.level, u.gender
order by t.weekday, u.level, u.level, u.gender;

-- level and gender
select users.level,
       users.gender,
       count(*) num
from songplays
         join users on songplays.user_id = users.user_id
group by users.level, users.gender
order by num desc;


-- top users
select users.first_name,
       users.last_name,
       count(*) num
from songplays
         join users on songplays.user_id = users.user_id
group by users.user_id, users.first_name, users.last_name
order by num desc
limit 10;

-- top song titles
select s.title,
       count(*) num
from songplays
         join artists a on songplays.artist_id = a.artist_id
         join songs s on songplays.song_id = s.song_id
group by s.title
order by num desc
limit 10;

-- top artists
select a.name,
       count(*) num
from songplays
         join artists a on songplays.artist_id = a.artist_id
         join songs s on songplays.song_id = s.song_id
group by a.name
order by num desc;
