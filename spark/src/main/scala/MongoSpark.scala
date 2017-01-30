import org.apache.log4j.{Logger, Level}
import org.apache.spark.{SparkConf, SparkContext}
import org.apache.spark.rdd.RDD

import org.apache.hadoop.conf.Configuration
import org.bson.BSONObject

import com.mongodb.{
    MongoClient,
    MongoException,
    WriteConcern,
    DB,
    DBCollection,
    BasicDBObject,
    BasicDBList,
    DBObject,
    DBCursor
}

import com.mongodb.hadoop.{
    MongoInputFormat,
    MongoOutputFormat,
    BSONFileInputFormat,
    BSONFileOutputFormat
}

import com.mongodb.hadoop.io.MongoUpdateWritable


object MongoSpark {
    def main(args: Array[String]) {
        /* Uncomment to turn off Spark logs */
        Logger.getLogger("org").setLevel(Level.OFF)
        Logger.getLogger("akka").setLevel(Level.OFF)
        Logger.getLogger("com").setLevel(Level.OFF)

        val conf = new SparkConf()
            .setMaster("local[*]")
            .setAppName("MongoSpark")
            .set("spark.driver.memory", "2g")
            .set("spark.executor.memory", "4g")

        val sc = new SparkContext(conf)

        val article_input_conf = new Configuration()
        article_input_conf.set("mongo.input.uri", "mongodb://localhost:27017/dblp.Article")

        val inproceedings_input_conf = new Configuration()
        inproceedings_input_conf.set("mongo.input.uri", "mongodb://localhost:27017/dblp.Inproceedings")
        
        val article = sc.newAPIHadoopRDD(
            article_input_conf,         // config
            classOf[MongoInputFormat],  // input format
            classOf[Object],            // key type
            classOf[BSONObject]         // val type
        )

        val inproceedings = sc.newAPIHadoopRDD(
            inproceedings_input_conf,
            classOf[MongoInputFormat],
            classOf[Object],
            classOf[BSONObject]
        )

        q1(article, inproceedings)
        //q2(inproceedings)
        q3a(inproceedings)
        q3b(inproceedings)
        q3c(inproceedings)
        q3d(article, inproceedings)
        q3e(article, inproceedings)
        q4a(article, inproceedings)
        q4b(article, inproceedings)
    }

    /* Q1. 
     * Count the number of tuples. */
    def q1(article: RDD[(Object,BSONObject)], inproceedings: RDD[(Object,BSONObject)]) {
        val q1a=article.count()
        println("*********** Q1 ************")
        println("Article: " +q1a.toString)
        val q1i=inproceedings.count()
        println("Inproceedings : " +q1i.toString)
    }
    
    /* Q2.
     * Add a column "Area" in the Inproceedings table.
     * Then, populate the column "Area" with the values from the above table if
     * there is a match, otherwise set it to "UNKNOWN" */
    def q2(inproceedings: RDD[(Object,BSONObject)]) {
        val outputConfig = new Configuration()
        outputConfig.set("mongo.output.uri",
        "mongodb://localhost:27017/dblp.Inproceedings")

        var updates = inproceedings.mapValues(
        value => new MongoUpdateWritable(
        new BasicDBObject("_id", value.get("_id")),  // Query
        new BasicDBObject("$set", new BasicDBObject("area", "Unknown")),  // Update operation
        false,  // Upsert
        false,   // Update multiple documents
        false
        )
        )

        updates.saveAsNewAPIHadoopFile(
        "file:///this-is-completely-unused",
        classOf[Object],
        classOf[MongoUpdateWritable],
        classOf[MongoOutputFormat[Object, MongoUpdateWritable]],
        outputConfig)

        var inpro=inproceedings.map(word => if(booktitle(word._2)==1) {word._2.put("area","Database");word}
            else if(booktitle(word._2)==2) {word._2.put("area","Theory");word}
            else if(booktitle(word._2)==3) {word._2.put("area","Systems");word}
            else if(booktitle(word._2)==4) {word._2.put("area","ML-AI");word}
            else {word._2.put("area","Unknown");word}) 

        updates = inpro.mapValues(
        value => new MongoUpdateWritable(
        new BasicDBObject("_id", value.get("_id")),  // Query
        new BasicDBObject("$set", new BasicDBObject("area", value.get("area"))),  // Update operation
        false,  // Upsert
        false,   // Update multiple documents
        false
        )
        )

        updates.saveAsNewAPIHadoopFile(
        "file:///this-is-completely-unused",
        classOf[Object],
        classOf[MongoUpdateWritable],
        classOf[MongoOutputFormat[Object, MongoUpdateWritable]],
        outputConfig)

    }

    def booktitle(word:BSONObject):Int={
        if( (word.get("booktitle")=="SIGMOD Conference") || (word.get("booktitle")=="VLDB") || (word.get("booktitle")=="ICDE") || (word.get("booktitle")=="PODS")){
            return 1
        }

        else if( (word.get("booktitle")=="STOC") || (word.get("booktitle")=="FOCS") || (word.get("booktitle")=="SODA") || (word.get("booktitle")=="ICALP") ){
            return 2
        }

        else if( (word.get("booktitle")=="SIGCOMM") || (word.get("booktitle")=="ISCA") || (word.get("booktitle")=="HPCA") || (word.get("booktitle")=="PLDI")){
            return 3
        }

        else if( (word.get("booktitle")=="ICML") || (word.get("booktitle")=="NIPS") || (word.get("booktitle")=="AAAI") || (word.get("booktitle")=="IJCAI")){
            return 4
        }

        else{
            return 0
        }
            
    }

    /* Q3a.
     * Find the number of papers in each area above. */
    def q3a(inproceedings: RDD[(Object,BSONObject)]) {
        val a=inproceedings.filter(_._2.get("area")=="Database").count()
        val b=inproceedings.filter(_._2.get("area")=="Theory").count()
        val c=inproceedings.filter(_._2.get("area")=="Systems").count()
        val d=inproceedings.filter(_._2.get("area")=="ML-AI").count()
        val e=inproceedings.filter(_._2.get("area")=="Unknown").count()

        println("*********** Q3a ************")
        println("Database: "+a.toString)
        println("Theory: "+b.toString)
        println("Systems: "+c.toString)
        println("ML-AI: "+d.toString) 
        println("Unknown: "+e.toString)       
    }

    /* Q3b.
     * Find the TOP­20 authors who published the most number of papers in
     * "Database" (author published in multiple areas will be counted in all those
     * areas). */
    def q3b(inproceedings: RDD[(Object,BSONObject)]) {
        var a=inproceedings.filter(_._2.get("area")=="Database")
        .map(word => (word._2.get("authors")))
        .flatMap(count_num)
        .map(word => (word, 1))
        .reduceByKey((a,b) => a+b)
        .takeOrdered(20)(Ordering[Long].reverse.on(word =>word._2))
        .map(word => (word._1) )
        
        println("*********** Q3b ************")
        println(a.deep.mkString("\n"))
    }

    def count_num(word:Object): Array[(Object)]= {
        val list= word.asInstanceOf[BasicDBList]
                  .toArray
        return list         
    }

    /* Q3c.
     * Find the number of authors who published in exactly two of the four areas
     * (do not consider UNKNOWN). */
    def q3c(inproceedings: RDD[(Object,BSONObject)]) {
        var a=inproceedings.filter(_._2.get("area")=="Database")
        .map(word => (word._2.get("authors")))
        .flatMap(count_num)
        .map(word => (word, 1))
        .reduceByKey((a,b) => a+b)
        .map(word => (word._1, 1))

        var b=inproceedings.filter(_._2.get("area")=="Theory")
        .map(word => (word._2.get("authors")))
        .flatMap(count_num)
        .map(word => (word, 1))
        .reduceByKey((a,b) => a+b)
        .map(word => (word._1, 1))

        var c=inproceedings.filter(_._2.get("area")=="Systems")
        .map(word => (word._2.get("authors")))
        .flatMap(count_num)
        .map(word => (word, 1))
        .reduceByKey((a,b) => a+b)
        .map(word => (word._1, 1))


        var d=inproceedings.filter(_._2.get("area")=="ML-AI")
        .map(word => (word._2.get("authors")))
        .flatMap(count_num)
        .map(word => (word, 1))
        .reduceByKey((a,b) => a+b)
        .map(word => (word._1, 1))

        var all=a.union(b).union(c).union(d)
        .reduceByKey((a,b) => a+b)
        .filter(_._2==2)
        .count()

        println("*********** Q3c ************")
        println(all)
    }

    /* Q3d.
     * Find the number of authors who wrote more journal papers than conference
     * papers (irrespective of research areas). */
    def q3d(article: RDD[(Object,BSONObject)], inproceedings: RDD[(Object,BSONObject)]) {
        var a=inproceedings
        .map(word => (word._2.get("authors")))
        .flatMap(count_num)
        .map(word => (word, 1))
        .reduceByKey((a,b) => a+b)
        .map(word => (word._1,(0,word._2)))

        var b=article
        .map(word => (word._2.get("authors")))
        .flatMap(count_num)
        .map(word => (word, 1))
        .reduceByKey((a,b) => a+b)
        .map(word => (word._1,(word._2,0)))

        var c=a.union(b)
        .reduceByKey((a,b) => sum(a,b))
        .map(word => (word._1, compare(word._2)))
        .filter(_._2==1)
        .count()

        println("*********** Q3d ************")
        println(c)
    }

    def sum(a:(Int,Int),b:(Int,Int)):(Int,Int)={
        val c=(a._1+b._1,a._2+b._2)
        return c
    }

    def compare(a:(Int,Int)):Int={
        if(a._1>a._2){return 1}
        else{return 2}
    }

    /* Q3e.
     * Find the top­5 authors who published the maximum number of papers (journal
     * OR conference) since 2000, among the authors who have published at least one
     * "Database" paper (in a conference). */
    def q3e(article: RDD[(Object,BSONObject)], inproceedings: RDD[(Object,BSONObject)]) {
        var a=inproceedings
        .filter(_._2.asInstanceOf[BasicDBObject].getString("year") != null)
        .filter(_._2.asInstanceOf[BasicDBObject].getString("year").toInt>=2000)
        .map(word => (word._2.get("authors")))
        .flatMap(count_num)
        .map(word => (word, 1))
        .reduceByKey((a,b) => a+b)

        var b=article
        .filter(_._2.asInstanceOf[BasicDBObject].getString("year") != null)
        .filter(_._2.asInstanceOf[BasicDBObject].getString("year").toInt>=2000)
        .map(word => (word._2.get("authors")))
        .flatMap(count_num)
        .map(word => (word, 1))
        .reduceByKey((a,b) => a+b)

        var c=a.union(b)
        .reduceByKey((a,b) => a+b)
        .map(word => (word._1, (word._2, 0)) )

        var d=inproceedings.filter(_._2.get("area")=="Database")
        .map(word => (word._2.get("authors")))
        .flatMap(count_num)
        .map(word => (word, 1))
        .reduceByKey((a,b) => a+b)
        .map(word => (word._1,(0,1)))

        var e=c.union(d)
        .reduceByKey((a,b) => sum(a,b))
        .filter(_._2._2 ==1)
        .map(word =>(word._1, word._2._1))
        .takeOrdered(5)(Ordering[Long].reverse.on(word =>word._2))
        .map(word => (word._1))

        println("*********** Q3e ************")
        println(e.deep.mkString("\n"))
    }

    /* Q4a.
     * Plot a linegraph with two lines, one for the number of journal papers and
     * the other for the number of conference paper in every decade starting from
     * 1950.
     * Therefore the decades will be 1950-1959, 1960-1969, ...., 2000-2009,
     * 2010-2015. */
    def q4a(article: RDD[(Object,BSONObject)], inproceedings: RDD[(Object,BSONObject)]) {
        var a1=article
        .filter(_._2.asInstanceOf[BasicDBObject].getString("year") != null)
        .filter(_._2.asInstanceOf[BasicDBObject].getString("year").toInt>=1950)
        .filter(_._2.asInstanceOf[BasicDBObject].getString("year").toInt<=1959)
        .count()
        .toString

        var a2=article
        .filter(_._2.asInstanceOf[BasicDBObject].getString("year") != null)
        .filter(_._2.asInstanceOf[BasicDBObject].getString("year").toInt>=1960)
        .filter(_._2.asInstanceOf[BasicDBObject].getString("year").toInt<=1969)
        .count()
        .toString

        var a3=article
        .filter(_._2.asInstanceOf[BasicDBObject].getString("year") != null)
        .filter(_._2.asInstanceOf[BasicDBObject].getString("year").toInt>=1970)
        .filter(_._2.asInstanceOf[BasicDBObject].getString("year").toInt<=1979)
        .count()
        .toString

        var a4=article
        .filter(_._2.asInstanceOf[BasicDBObject].getString("year") != null)
        .filter(_._2.asInstanceOf[BasicDBObject].getString("year").toInt>=1980)
        .filter(_._2.asInstanceOf[BasicDBObject].getString("year").toInt<=1989)
        .count()
        .toString

        var a5=article
        .filter(_._2.asInstanceOf[BasicDBObject].getString("year") != null)
        .filter(_._2.asInstanceOf[BasicDBObject].getString("year").toInt>=1990)
        .filter(_._2.asInstanceOf[BasicDBObject].getString("year").toInt<=1999)
        .count()
        .toString

        var a6=article
        .filter(_._2.asInstanceOf[BasicDBObject].getString("year") != null)
        .filter(_._2.asInstanceOf[BasicDBObject].getString("year").toInt>=2000)
        .filter(_._2.asInstanceOf[BasicDBObject].getString("year").toInt<=2009)
        .count()
        .toString

        var a7=article
        .filter(_._2.asInstanceOf[BasicDBObject].getString("year") != null)
        .filter(_._2.asInstanceOf[BasicDBObject].getString("year").toInt>=2010)
        .filter(_._2.asInstanceOf[BasicDBObject].getString("year").toInt<=2019)
        .count()
        .toString

        println("*********** Q4a ************")
        println("************** article **************")
        println("1950s: "+a1)
        println("1960s: "+a2)
        println("1970s: "+a3)
        println("1980s: "+a4)
        println("1990s: "+a5)
        println("2000s: "+a6)
        println("2010s: "+a7)

        var b1=inproceedings
        .filter(_._2.asInstanceOf[BasicDBObject].getString("year") != null)
        .filter(_._2.asInstanceOf[BasicDBObject].getString("year").toInt>=1950)
        .filter(_._2.asInstanceOf[BasicDBObject].getString("year").toInt<=1959)
        .count()
        .toString

        var b2=inproceedings
        .filter(_._2.asInstanceOf[BasicDBObject].getString("year") != null)
        .filter(_._2.asInstanceOf[BasicDBObject].getString("year").toInt>=1960)
        .filter(_._2.asInstanceOf[BasicDBObject].getString("year").toInt<=1969)
        .count()
        .toString

        var b3=inproceedings
        .filter(_._2.asInstanceOf[BasicDBObject].getString("year") != null)
        .filter(_._2.asInstanceOf[BasicDBObject].getString("year").toInt>=1970)
        .filter(_._2.asInstanceOf[BasicDBObject].getString("year").toInt<=1979)
        .count()
        .toString

        var b4=inproceedings
        .filter(_._2.asInstanceOf[BasicDBObject].getString("year") != null)
        .filter(_._2.asInstanceOf[BasicDBObject].getString("year").toInt>=1980)
        .filter(_._2.asInstanceOf[BasicDBObject].getString("year").toInt<=1989)
        .count()
        .toString

        var b5=inproceedings
        .filter(_._2.asInstanceOf[BasicDBObject].getString("year") != null)
        .filter(_._2.asInstanceOf[BasicDBObject].getString("year").toInt>=1990)
        .filter(_._2.asInstanceOf[BasicDBObject].getString("year").toInt<=1999)
        .count()
        .toString

        var b6=inproceedings
        .filter(_._2.asInstanceOf[BasicDBObject].getString("year") != null)
        .filter(_._2.asInstanceOf[BasicDBObject].getString("year").toInt>=2000)
        .filter(_._2.asInstanceOf[BasicDBObject].getString("year").toInt<=2009)
        .count()
        .toString

        var b7=inproceedings
        .filter(_._2.asInstanceOf[BasicDBObject].getString("year") != null)
        .filter(_._2.asInstanceOf[BasicDBObject].getString("year").toInt>=2010)
        .filter(_._2.asInstanceOf[BasicDBObject].getString("year").toInt<=2019)
        .count()
        .toString

        println("************** inproceedings **************")
        println("1950s: "+b1)
        println("1960s: "+b2)
        println("1970s: "+b3)
        println("1980s: "+b4)
        println("1990s: "+b5)
        println("2000s: "+b6)
        println("2010s: "+b7)

    }

    /* Q4b.
     * Plot a barchart showing how the average number of collaborators varied in
     * these decades for conference papers in each of the four areas in Q3.
     * Again, the decades will be 1950-1959, 1960-1969, ...., 2000-2009, 2010-2015.
     * But for every decade, there will be four bars one for each area (do not
     * consider UNKNOWN), the height of the bars will denote the average number of
     * collaborators. */
    def q4b(article: RDD[(Object,BSONObject)], inproceedings: RDD[(Object,BSONObject)]) {
        var a=inproceedings.filter(_._2.asInstanceOf[BasicDBObject].getString("year") != null)
        .map(word => (word._2.get("authors").asInstanceOf[BasicDBList].toArray,word._2.get("area").toString,word._2.get("year").toString.toInt))
        .filter(_._2 != "Unknown")
        .map(word => (word._1, word._2, decade(word._3)))
        .filter(_._3 != "None")
        .flatMap(flat1)
        .map(word => ((word._1, word._3),word._2))
        .distinct()

        var b=inproceedings.filter(_._2.asInstanceOf[BasicDBObject].getString("year") != null)
        .map(word => (word._2.get("authors").asInstanceOf[BasicDBList].toArray,word._2.get("year").toString.toInt))
        .map(word => (word._1, decade(word._2)))
        .filter(_._2 != "None")
        .filter(_._1.length >1)
        .flatMap(flat2)

        var c=article.filter(_._2.asInstanceOf[BasicDBObject].getString("year") != null)
        .map(word => (word._2.get("authors").asInstanceOf[BasicDBList].toArray,word._2.get("year").toString.toInt))
        .map(word => (word._1, decade(word._2)))
        .filter(_._2 != "None")
        .filter(_._1.length >1)
        .flatMap(flat2)

        var d=b.distinct().union(c.distinct()).distinct()
        .map(word => ((word._1, word._3),1) )
        .reduceByKey((a,b)=>a+b)

        var e=a.join(d)
        .map(word => ((word._1._2, word._2._1), word._1._1,word._2._2 ) )

        var f=e.map(word => (word._1, 1))
        .reduceByKey((a,b)=>a+b)

        var g=e.map(word => (word._1, word._3))
        .reduceByKey((a,b)=>a+b)

        var h=f.join(g)
        .map(word => (word._1, word._2._1, word._2._2))
        .map(word => (word._1, word._3.toFloat/word._2))
        .sortByKey()
        .collect()

        println("*********** Q4b ************")
        println(h.deep.mkString("\n"))
    }

    def decade(a:Int):String={
        if(a>=1950 && a<=1959){
            return "1950s"
        }

        else if(a>=1960 && a<=1969){
            return "1960s"
        }

        else if(a>=1970 && a<=1979){
            return "1970s"
        }

        else if(a>=1980 && a<=1989){
            return "1980s"
        }

        else if(a>=1990 && a<=1999){
            return "1990s"
        }

        else if(a>=2000 && a<=2009){
            return "2000s"
        }

        else if(a>=2010 && a<=2019){
            return "2010s"
        }

        else{
            return "None"
        }
    }

    def flat1(a:(Array[Object],String,String)):Array[(String,String,String)]={
        val b=a._1.length;
        var c=new Array[(String,String,String)](b)
        for(i <- 0 to b-1){
            var c1=a._1(i).toString
            var c2=a._2
            var c3=a._3
            c(i)=(c1,c2,c3)
        }
        return c
    }

    def flat2(a:(Array[Object],String)):Array[(String,String,String)]={
        var d=Array[(String,String,String)]()
        for (c <- a._1){
            for(b<- a._1){
                if(c!=b)
                    d=(c.toString,b.toString,a._2) +:d
            }
        }
        return d
    }

}
