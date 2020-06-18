import sqlite3 as lite
import pandas as pd
from pandas import DataFrame
import csv
import re
con = lite.connect('cs1656.sqlite')

with con:
	cur = con.cursor() 

	########################################################################		
	### CREATE TABLES ######################################################
	########################################################################		
	# DO NOT MODIFY - START 
	cur.execute('DROP TABLE IF EXISTS Actors')
	cur.execute("CREATE TABLE Actors(aid INT, fname TEXT, lname TEXT, gender CHAR(6), PRIMARY KEY(aid))")

	cur.execute('DROP TABLE IF EXISTS Movies')
	cur.execute("CREATE TABLE Movies(mid INT, title TEXT, year INT, rank REAL, PRIMARY KEY(mid))")

	cur.execute('DROP TABLE IF EXISTS Directors')
	cur.execute("CREATE TABLE Directors(did INT, fname TEXT, lname TEXT, PRIMARY KEY(did))")

	cur.execute('DROP TABLE IF EXISTS Cast')
	cur.execute("CREATE TABLE Cast(aid INT, mid INT, role TEXT)")

	cur.execute('DROP TABLE IF EXISTS Movie_Director')
	cur.execute("CREATE TABLE Movie_Director(did INT, mid INT)")
	# DO NOT MODIFY - END

	########################################################################		
	### READ DATA FROM FILES ###############################################
	########################################################################		
	# actors.csv, cast.csv, directors.csv, movie_dir.csv, movies.csv
	# UPDATE THIS

	actorData = pd.read_csv("actors.csv", names=['aid', 'fname', 'lname', 'gender'])
	castData = pd.read_csv("cast.csv", names=['aid', 'mid', 'role'])
	directorData = pd.read_csv("directors.csv", names=['did', 'fname', 'lname'])
	movieDirData = pd.read_csv("movie_dir.csv", names=['did', 'mid'])
	movieData = pd.read_csv("movies.csv", names=['mid', 'title', 'year', 'rank'])
	
	actorData.to_sql('ActorsSQL', con, if_exists='append', index=False)
	castData.to_sql('CastSQL', con, if_exists='append', index=False)
	directorData.to_sql('DirectorsSQL', con, if_exists='append', index=False)
	movieDirData.to_sql('MovieDirSQL', con, if_exists='append', index=False)
	movieData.to_sql('MoviesSQL', con, if_exists='append', index=False)


	########################################################################		
	### INSERT DATA INTO DATABASE ##########################################
	########################################################################		
	# UPDATE THIS TO WORK WITH DATA READ IN FROM CSV FILES
	cur.execute('''INSERT INTO Actors (aid, fname, lname, gender)
				SELECT DISTINCT actor.aid, actor.fname, actor.lname, actor.gender
				FROM ActorsSQL actor
				''') 
	cur.execute('''INSERT INTO Cast (aid, mid, role)
				SELECT DISTINCT c.aid, c.mid, c.role
				FROM CastSQL c
				''')
	cur.execute('''INSERT INTO Directors (did, fname, lname)
				SELECT DISTINCT direct.did, direct.fname, direct.lname
				FROM DirectorsSQL direct
				''')
	cur.execute('''INSERT INTO Movie_Director (did, mid)
				SELECT DISTINCT movieDir.did, movieDir.mid
				FROM MovieDirSQL movieDir
				''')
	cur.execute('''INSERT INTO Movies (mid, title, year, rank)
				SELECT DISTINCT movie.mid, movie.title, movie.year, movie.rank
				FROM MoviesSQL movie
				''')

	con.commit()
    
    	

	########################################################################		
	### QUERY SECTION ######################################################
	########################################################################		
	queries = {}

	# DO NOT MODIFY - START 	
	# DEBUG: all_movies ########################
	queries['all_movies'] = '''
SELECT * FROM Movies
'''	
	# DEBUG: all_actors ########################
	queries['all_actors'] = '''
SELECT * FROM Actors
'''	
	# DEBUG: all_cast ########################
	queries['all_cast'] = '''
SELECT * FROM Cast
'''	
	# DEBUG: all_directors ########################
	queries['all_directors'] = '''
SELECT * FROM Directors
'''	
	# DEBUG: all_movie_dir ########################
	queries['all_movie_dir'] = '''
SELECT * FROM Movie_Director
'''	
	# DO NOT MODIFY - END

	########################################################################		
	### INSERT YOUR QUERIES HERE ###########################################
	########################################################################		
	# NOTE: You are allowed to also include other queries here (e.g., 
	# for creating views), that will be executed in alphabetical order.
	# We will grade your program based on the output files q01.csv, 
	# q02.csv, ..., q12.csv

	# Q01 ########################		
	queries['a01'] = '''
DROP VIEW IF EXISTS eighties
'''
	queries['b01'] = '''
DROP VIEW IF EXISTS twenty_first_century
'''
	queries['c01'] = '''
CREATE VIEW eighties AS
SELECT c.aid, m.mid
FROM Cast as c, Movies as m
WHERE m.year >= 1980 AND m.year <= 1990 AND c.mid = m.mid
GROUP BY c.aid
'''
	queries['d01'] = '''
CREATE VIEW twenty_first_century AS
SELECT c.aid, m.mid
FROM Cast as c, Movies as m
WHERE m.year >= 2000 AND c.mid = m.mid
GROUP BY c.aid
'''
	queries['q01'] = '''
SELECT a.fname, a.lname
FROM Actors as a, eighties as e, twenty_first_century as tfc
WHERE a.aid = e.aid AND a.aid = tfc.aid
ORDER BY lname ASC, fname ASC
'''	
	
	# Q02 ########################		
	queries['q02'] = '''
SELECT movie.title, movie.year
FROM Movies as movie
INNER JOIN Movies m on m.title = "Rogue One: A Star Wars Story"
WHERE m.year = movie.year AND movie.rank > m.rank
GROUP BY movie.title, movie.year
ORDER BY movie.title ASC
'''	

	# Q03 ########################		
	queries['a03'] = '''
DROP VIEW IF EXISTS in_star_wars
'''
	queries['b03'] = '''
CREATE VIEW in_star_wars AS
SELECT aid, count(DISTINCT m.mid) as count
FROM Movies as m
INNER JOIN Cast as c on m.title LIKE '%Star Wars%' AND c.mid = m.mid
GROUP BY aid
HAVING count > 0
'''
	queries['q03'] = '''
SELECT a.fname, a.lname
FROM in_star_wars as s, Actors as a
WHERE a.aid = s.aid
ORDER BY s.count DESC, a.lname ASC, a.fname ASC
'''	

	# Q04 ########################		
	queries['a04'] = '''
DROP VIEW IF EXISTS before_eighty_five
'''
	queries['b04'] = '''
DROP VIEW IF EXISTS after_eighty_five
'''
	queries['c04'] = '''
CREATE VIEW before_eighty_five AS
SELECT DISTINCT c.aid
FROM Cast as c, Movies as m
WHERE m.mid = c.mid AND m.year < 1985
'''
	queries['d04'] = '''
CREATE VIEW after_eighty_five AS
SELECT DISTINCT c.aid
FROM Cast as c, Movies as m
WHERE m.mid = c.mid AND m.year >= 1985
'''
	queries['q04'] = '''
SELECT a.fname, a.lname
FROM Actors as a
INNER JOIN before_eighty_five as bef on bef.aid = a.aid
WHERE NOT a.aid IN 
	(SELECT DISTINCT aef.aid
	FROM after_eighty_five as aef)
ORDER BY lname ASC, fname ASC
'''	

	# Q05 ########################		
	queries['q05'] = '''
SELECT d.fname, d.lname, count(DISTINCT movieDir.mid) as count
FROM Movie_Director as movieDir
INNER JOIN Directors as d on d.did = movieDir.did
GROUP BY movieDir.did
ORDER BY count DESC, d.lname ASC, d.fname ASC
LIMIT 10
'''	

	# Q06 ########################		
	queries['a06'] = '''
DROP VIEW IF EXISTS largest_cast
'''
	queries['b06'] = '''
CREATE VIEW largest_cast AS
SELECT m.title, count(DISTINCT c.aid) as count
FROM Movies as m
INNER JOIN Cast as c on c.mid = m.mid
GROUP BY m.mid
HAVING count > 0
'''
	queries['q06'] = '''
SELECT title, count
FROM largest_cast
ORDER BY count DESC
LIMIT 10
'''	

	# Q07 ########################		
	queries['a07'] = '''
DROP VIEW IF EXISTS male_count
'''
	queries['b07'] = '''
DROP VIEW IF EXISTS female_count
'''
	queries['c07'] = '''
CREATE VIEW male_count AS
SELECT count(DISTINCT a.aid) as maleCount, c.mid
FROM Actors as a, Cast as c
WHERE a.gender = 'Male' AND a.aid = c.aid
GROUP BY c.mid
'''
	queries['d07'] = '''
CREATE VIEW female_count AS
SELECT count(DISTINCT a.aid) as femaleCount, c.mid
FROM Actors as a, Cast as c
WHERE a.gender = 'Female' AND a.aid = c.aid
GROUP BY c.mid
'''
	
	queries['q07'] = '''
SELECT title, femaleCount, maleCount
FROM Movies as m, male_count as mc, female_count as fc
WHERE m.mid = mc.mid AND m.mid = fc.mid AND fc.femaleCount > mc.maleCount
ORDER BY title ASC
'''	

	# Q08 ########################		
	queries['a08'] = '''
DROP VIEW IF EXISTS director_count
'''
	queries['b08'] = '''
CREATE VIEW director_count AS
SELECT c.aid, count(DISTINCT movieDir.did) as dCount
FROM Movie_Director as movieDir, Cast as c
WHERE c.mid = movieDir.mid
GROUP BY c.aid
HAVING dCount >= 7
'''
	queries['q08'] = '''
SELECT fname, lname, dCount
FROM director_count as dc, Actors as a
WHERE dc.aid = a.aid
GROUP BY a.aid
ORDER BY dCount DESC, lname ASC, fname ASC
'''	

	# Q09 ########################		
	queries['a09'] = '''
DROP VIEW IF EXISTS total_movies_acted
'''
	queries['b09'] = '''
DROP VIEW IF EXISTS prem_year
'''
	queries['c09'] = '''
CREATE VIEW total_movies_acted AS
SELECT a.fname, a.lname, a.aid, m.year
FROM Actors as a
INNER JOIN Cast as c on a.aid = c.aid
INNER JOIN Movies as m on c.mid = m.mid
'''
	queries['d09'] = '''
CREATE VIEW prem_year AS
SELECT tma.fname, tma.lname, tma.aid, MIN(tma.year), count(*) as mCount
FROM total_movies_acted as tma
WHERE SUBSTR(tma.fname, 1, 1) = 'D'
GROUP BY tma.aid, tma.year
'''
	queries['q09'] = '''
SELECT year.fname, year.lname, year.mCount
FROM prem_year as year
GROUP BY year.aid
ORDER BY year.mCount DESC, year.lname ASC, year.fname ASC
'''	

	# Q10 ########################		
	queries['q10'] = '''
SELECT a.lname, m.title
FROM Cast as c, Movie_Director as movieDir, Actors as a, Directors as d, Movies as m
WHERE a.lname = d.lname AND a.fname != d.fname 
AND c.mid = movieDir.mid AND a.aid = c.aid AND d.did = movieDir.did AND c.mid = m.mid
ORDER BY a.lname ASC
'''

	# Q11 ########################		
	queries['a11'] = '''
DROP VIEW IF EXISTS bacon_in_movie
'''
	queries['b11'] = '''
DROP VIEW IF EXISTS in_movie_with_bacon
'''
	queries['c11'] = '''
DROP VIEW IF EXISTS actor_movies
'''
	queries['d11'] = '''
DROP VIEW IF EXISTS second_level_bacon
'''
	queries['e11'] = '''
CREATE VIEW bacon_in_movie AS
SELECT m.mid
FROM Movies as m
INNER JOIN Cast as c on c.mid = m.mid
INNER JOIN Actors as a on a.aid = c.aid AND a.fname = "Kevin" AND a.lname = "Bacon"
GROUP BY m.mid
'''
	queries['f11'] = '''
CREATE VIEW in_movie_with_bacon AS
SELECT c.aid, c.mid
FROM bacon_in_movie as bim
INNER JOIN Cast as c on c.mid = bim.mid
INNER JOIN Actors as a on a.aid = c.aid AND a.fname != "Kevin" AND a.lname != "Bacon"
'''
	queries['g11'] = '''
CREATE VIEW actor_movies AS
SELECT c.aid, c.mid
FROM Cast as c, in_movie_with_bacon as imb
WHERE c.aid = imb.aid
'''	
	queries['h11'] = '''
CREATE VIEW second_level_bacon AS
SELECT c.aid, c.mid
FROM actor_movies as am
INNER JOIN Cast as c on c.mid = am.mid AND c.aid != am.aid
'''
	queries['q11'] = '''
SELECT a.fname, a.lname
FROM Actors as a, second_level_bacon as s
WHERE a.aid = s.aid AND a.fname != "Kevin" AND a.lname != "Bacon"
GROUP BY a.aid
ORDER BY a.lname DESC, a.fname DESC
'''

	# Q12 ########################		
	queries['a12'] = '''
DROP VIEW IF EXISTS avg_rating
'''
	queries['b12'] = '''
CREATE VIEW avg_rating AS
SELECT sum(m.rank) as sum, count(m.mid) as count, c.aid
FROM Movies as m, Cast as c
WHERE m.mid = c.mid
GROUP BY c.aid
'''	
	queries['q12'] = '''
SELECT a.fname, a.lname, avg.count, avg.sum/avg.count
FROM Actors as a, avg_rating as avg
WHERE a.aid = avg.aid
GROUP BY a.aid
ORDER BY avg.sum/avg.count DESC
LIMIT 20
'''	


	########################################################################		
	### SAVE RESULTS TO FILES ##############################################
	########################################################################		
	# DO NOT MODIFY - START 	
	for (qkey, qstring) in sorted(queries.items()):
		try:
			cur.execute(qstring)
			all_rows = cur.fetchall()
			
			print ("=========== ",qkey," QUERY ======================")
			print (qstring)
			print ("----------- ",qkey," RESULTS --------------------")
			for row in all_rows:
				print (row)
			print (" ")

			save_to_file = (re.search(r'q0\d', qkey) or re.search(r'q1[012]', qkey))
			if (save_to_file):
				with open(qkey+'.csv', 'w') as f:
					writer = csv.writer(f)
					writer.writerows(all_rows)
					f.close()
				print ("----------- ",qkey+".csv"," *SAVED* ----------------\n")
		
		except lite.Error as e:
			print ("An error occurred:", e.args[0])
	# DO NOT MODIFY - END
	
