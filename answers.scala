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

//Ex4
rddMovies.map(m => (m.year, (m.votes, 1))).reduceByKey((x, y) => (x._1 + y._1, x._2 + y._2)).mapValues({case (v, c) => v.toFloat/c}).sortBy(_._2, ascending=false).map({case (y, v) => "year: " + y + " average votes: " + v}).foreach(println)

//Part 2

//WIP
val toRemove = ",;'.:!?".toSet
rddMovies.map(m=> (m.id, m.description)).flatMapValues(_.split(" ")).mapValues(_.toLowerCase().trim().filterNot(toRemove)).take(50).foreach(println)




def createInvertedIndex(movies: RDD[Movie]): RDD[(String, Iterable[Int])] = {
        // Define helper functions directly inside this function. In scala you can declare inner functions
        // and use them only inside the function they were declared. Useful to encapsulate/restrict 
        // their use outside this function.
        
        // Split the given string into an array of words (without any formatting), then return it.
        def tokenizeDescription(description: String): Seq[String] = {
            return description.split(" ")
        }
        
        // Remove the blank spaces (trim) in the given word, transform it in lowercase, then return it.
        def normalizeWord(word: String): String = {
            val toRemove = ",;'.:!?".toSet
            return word.toLowerCase().trim().filterNot(toRemove)
        }
        
        // For the sake of simplicity let's ignore the implementation (in a real case we would return true if w is a stopword, otherwise false).
        // TODO student nothing here but still call this function in your invertedIndex creation process.
        def isStopWord(w: String): Boolean = {
          false
        }
        
        // For the sake of simplicity let's ignore the implementation (in a real case we would apply stemming to w and return the result, e.g. w=automation -> w=automat).
        // TODO student nothing here but still call this function in your invertedIndex creation process.
        def applyStemming(w: String): String = {
          w
        }
      
       // TODO student
       // Here we are going to work on the movies RDD, by tokenizing and normalizing the description of every movie, then by building a key-value object that contains the tokens as keys, and the IDs of the movies as values
       // (see the example on 4).
       // The goal here is to do everything by chaining the possible transformations and actions of Spark.
       // Possible steps:
       //   1) What we first want to do here is applying the 4 previous methods on any movie's description. Be aware of the fact that we also want to keep the IDs of the movies.
       //   2) For each tokenized word, create a tuple as (word, id), where id is the current movie id
       //        [
       //          ("toto", 120), ("mange", 120), ("des", 120), ("pommes", 120),
       //          ("toto", 121), ("lance", 121), ("des", 121), ("photocopies", 121)
       //        ]
       //      Hint: you can use a `map` function inside another `map` function.
       //   3) We finally need to find a way to remove duplicated keys and thus only having one entry per key, with all the linked IDs as values. For example:
       //        [
       //          ("toto", [120, 121]),
       //          ("mange", [120]),
       //          ...
       //        ]
       val invertedIndex = rddMovies.map(m => (m.id, m.description)).flatMapValues(tokenizeDescription).mapValues(normalizeWord).filter(w => !isStopWord(w._2)).map(pair => pair.swap).groupByKey()//.map(x => (x._1, x._2.toList))

       // Return the new-built inverted index.
       invertedIndex
  }




  // TODO student
// Here we are going to operate the analytic and display its result on a given inverted index (that will be obtained from the previous function).
def topN(invertedIndex: RDD[(String, Iterable[Int])], N: Int): Unit = {
  // TODO student
  // We are going to work on the given invertedIndex array to do our analytic:
  //   1) Find a way to get the number of movie in which a word appears.
  //   2) Keep only the top N words and their occurence.
  val topMovies = invertedIndex.mapValues(x => x.size).sortBy(_._2, ascending = false).take(N)
  
  // Print the words and the number of descriptions in which they appear.
  println("Top '" + N + "' most used words")
  topMovies.foreach(println)
}