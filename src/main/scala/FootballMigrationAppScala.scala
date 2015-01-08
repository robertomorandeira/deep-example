import com.stratio.deep.commons.entity.{Cell, Cells}
import com.stratio.deep.core.context.DeepSparkContext
import com.stratio.deep.jdbc.config.{JdbcConfigFactory, JdbcDeepJobConfig}
import com.stratio.deep.mongodb.config.{MongoConfigFactory, MongoDeepJobConfig}
import org.apache.spark.SparkContext._
import org.apache.spark.rdd.RDD
import org.apache.spark.{SparkConf, SparkContext}
import scala.collection.JavaConverters._

object FootballMigrationAppScala extends App {
  private val CLUSTER: String = "local"
  private val APP_NAME: String = "deepMySQLToMongodbMigration"
  private val MYSQL_DRIVER: String = "com.mysql.jdbc.Driver"
  private val MYSQL_HOST: String = "172.28.128.3"
  private val MYSQL_PORT: Integer = 3306
  private val MYSQL_USER: String = "root"
  private val MYSQL_PASS: String = "root"
  private val MYSQL_DBNAME: String = "football"
  private val MONGODB_HOST: String = "172.28.128.3:27017"
  private val MONGODB_DBNAME: String = "football"
  private val MONGODB_COLLECTION: String = "teams"

  // Creating the Deep Context where args are Spark Master and Job Name
  val sparkConf: SparkConf = new SparkConf().setAppName(APP_NAME).setMaster(CLUSTER)
  val ctx: SparkContext = new SparkContext(sparkConf)
  val deepContext: DeepSparkContext = new DeepSparkContext(ctx)

  // Creating a configuration for the team RDD and initialize it
  val mySQLTeamConfig: JdbcDeepJobConfig[Cells] = JdbcConfigFactory.createJdbc
    .host(MYSQL_HOST).port(MYSQL_PORT)
    .username(MYSQL_USER).password(MYSQL_PASS)
    .database(MYSQL_DBNAME).table("team")
    .driverClass(MYSQL_DRIVER)
    .initialize

  // Creating a configuration for the player RDD and initialize it
  val mySQLPlayerConfig: JdbcDeepJobConfig[Cells] = JdbcConfigFactory.createJdbc
    .host(MYSQL_HOST).port(MYSQL_PORT)
    .username(MYSQL_USER).password(MYSQL_PASS)
    .database(MYSQL_DBNAME).table("player")
    .driverClass(MYSQL_DRIVER)
    .initialize

  // Creating the RDDs
  val teamRDD: RDD[Cells] = deepContext.createRDD(mySQLTeamConfig)
  val playerRDD: RDD[Cells] = deepContext.createRDD(mySQLPlayerConfig)

  // Map teams to pair with (team id, team)
  val teamPairRDD: RDD[(Long, Cells)] = teamRDD.map(team => (team.getLong("id").longValue(), team))

  // Map players to pair with (team id, player) and group by team_id
  val playerPairRDD: RDD[(Long, Iterable[Cells])] = playerRDD.map(cells =>
    (cells.getLong("team_id").longValue(), cells))
    .groupByKey

  val joinedPairRDD: RDD[(Long, (Cells, Option[Iterable[Cells]]))] = teamPairRDD.leftOuterJoin(playerPairRDD)

  // Creating a configuration for the mongodb result RDD and initialize it
  val outputMongodbConfig: MongoDeepJobConfig[Cells] = MongoConfigFactory.createMongoDB
    .host(MONGODB_HOST)
    .database(MONGODB_DBNAME).collection(MONGODB_COLLECTION)
    .initialize

  // Transforming the joined result to the desirable structure in mongodb
  // Ej: {_id: <team_id>, name: <team_name>, players: [<player_name_1>, <player_name_2>]}
  val outputRDD: RDD[Cells] = joinedPairRDD.map(joined => {
    val cells: Cells = new Cells
    cells.add(Cell.create("_id", joined._1))
    cells.add(Cell.create("name", joined._2._1.getString("name")))
    cells.add(Cell.create("players", joined._2._2 match {
      case Some(players) => players.map(player => player.getString("lastname") + ", " + player.getString("firstname")).asJava
      case _ => null
    }))
    cells
  })

  DeepSparkContext.saveRDD(outputRDD, outputMongodbConfig)

  deepContext.stop
  System.exit(0)
}

