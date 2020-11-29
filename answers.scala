//Part 1
//Ex1

rddMovies.filter(_.title.contains("City")).map(_.title).foreach(println)

//Ex2
val rateMin = 3.3
val rateMax = 5.7
rddMovies.filter(m => m.rating > rateMin && m.rating <= rateMax).sortBy(_.rating).map(m => (m.rating, m.title)).foreach(println)

//EX3 - MARCHE PAS
rddMovies.flatMap(_.genres).groupBy(m => m).map(m => (m, size(m._2))).foreach(println)
