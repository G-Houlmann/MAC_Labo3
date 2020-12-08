//Part 1
//Ex1

rddMovies.filter(_.title.contains("City")).map(_.title).foreach(println)

//Ex2
val rateMin = 3.3
val rateMax = 5.7
rddMovies.filter(m => m.rating > rateMin && m.rating <= rateMax).sortBy(_.rating).map(m => (m.rating, m.title)).foreach(println)

//EX3 - TODO check les espaces ou jsais pas quoi
val amountToShow = 5
rddMovies.flatMap(_.genres).map(m => (m, 1)).reduceByKey((x,y) => x+y).sortBy(_._2, ascending=false).take(amountToShow).foreach(println)

//Ex4 - TODO affichage
rddMovies.map(m => (m.year, (m.votes, 1))).reduceByKey((x, y) => (x._1 + y._1, x._2 + y._2)).mapValues({case (v, c) => v.toFloat/c}).sortBy(_._2, ascending=false).foreach(println)


//Part 2

//WIP
rddMovies.map(m => (m.year, (m.votes, 1))).reduceByKey((x, y) => (x._1 + y._1, x._2 + y._2)).mapValues({case (v, c) => v.toFloat/c}).sortBy(_._2, ascending=false).foreach(println)






