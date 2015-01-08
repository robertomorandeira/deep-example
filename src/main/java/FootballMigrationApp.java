import com.google.common.base.Optional;
import com.stratio.deep.commons.entity.Cell;
import com.stratio.deep.commons.entity.Cells;
import com.stratio.deep.core.context.DeepSparkContext;
import com.stratio.deep.jdbc.config.JdbcConfigFactory;
import com.stratio.deep.jdbc.config.JdbcDeepJobConfig;
import com.stratio.deep.mongodb.config.MongoConfigFactory;
import com.stratio.deep.mongodb.config.MongoDeepJobConfig;

import org.apache.spark.SparkConf;
import org.apache.spark.api.java.JavaPairRDD;
import org.apache.spark.api.java.JavaRDD;
import org.apache.spark.api.java.JavaSparkContext;
import org.apache.spark.api.java.function.Function;
import org.apache.spark.api.java.function.PairFunction;

import scala.Tuple2;

import javax.annotation.Nullable;

import java.util.ArrayList;
import java.util.List;

public class FootballMigrationApp {

    // spark properties
    private static final String CLUSTER = "local";
    private static final String APP_NAME = "deepMySQLToMongodbMigration";

    // mySQL properties
    private static final String MYSQL_DRIVER = "com.mysql.jdbc.Driver";
    private static final String MYSQL_HOST = "172.28.128.3";
    private static final Integer MYSQL_PORT = 3306;
    private static final String MYSQL_USER = "root";
    private static final String MYSQL_PASS = "root";
    private static final String MYSQL_DBNAME = "football";

    // mongoDB properties
    private static final String MONGODB_HOST = "172.28.128.3:27017";
    private static final String MONGODB_DBNAME = "football";
    private static final String MONGODB_COLLECTION = "teams";

    public static void main(String[] args) {

        // Creating the Deep Context where args are Spark Master and Job Name
        SparkConf sparkConf = new SparkConf().setAppName(APP_NAME).setMaster(CLUSTER);
        JavaSparkContext ctx = new JavaSparkContext(sparkConf);
        DeepSparkContext deepContext = new DeepSparkContext(ctx.sc());

        // Creating a configuration for the team RDD and initialize it
        JdbcDeepJobConfig<Cells> mySQLTeamConfig = JdbcConfigFactory.createJdbc()
                .host(MYSQL_HOST).port(MYSQL_PORT)
                .username(MYSQL_USER).password(MYSQL_PASS)
                .database(MYSQL_DBNAME).table("team")
                .driverClass(MYSQL_DRIVER)
                .initialize();

        // Creating a configuration for the player RDD and initialize it
        JdbcDeepJobConfig<Cells> mySQLPlayerConfig = JdbcConfigFactory.createJdbc()
                .host(MYSQL_HOST).port(MYSQL_PORT)
                .username(MYSQL_USER).password(MYSQL_PASS)
                .database(MYSQL_DBNAME).table("player")
                .driverClass(MYSQL_DRIVER)
                .initialize();

        // Creating the RDDs
        JavaRDD<Cells> teamRDD = deepContext.createJavaRDD(mySQLTeamConfig);
        JavaRDD<Cells> playerRDD = deepContext.createJavaRDD(mySQLPlayerConfig);

        // Map teams to pair with (team id, team)
        JavaPairRDD<Long, Cells> teamPairRDD = teamRDD.mapToPair(new PairFunction<Cells, Long, Cells>() {
            @Override
            public Tuple2<Long, Cells> call(Cells cells) throws Exception {
                return new Tuple2<>(cells.getLong("id"), cells);
            }
        });

        // Map players to pair with (team id, player) and group by team_id
        JavaPairRDD<Long, Iterable<Cells>> playerPairRDD = playerRDD.mapToPair(new PairFunction<Cells, Long, Cells>() {
            @Override
            public Tuple2<Long, Cells> call(Cells cells) throws Exception {
                return new Tuple2<>(cells.getLong("team_id"), cells);
            }
        }).groupByKey();

        JavaPairRDD<Long, Tuple2<Cells, Optional<Iterable<Cells>>>> joinedPairRDD = teamPairRDD
                .leftOuterJoin(playerPairRDD);

        // Creating a configuration for the mongodb result RDD and initialize it
        MongoDeepJobConfig<Cells> outputMongodbConfig = MongoConfigFactory.createMongoDB()
                .host(MONGODB_HOST)
                .database(MONGODB_DBNAME).collection(MONGODB_COLLECTION)
                .initialize();

        // Transforming the joined result to the desirable structure in mongodb
        // Ej: {_id: <team_id>, name: <team_name>, players: [<player_name_1>, <player_name_2>]}
        JavaRDD<Cells> outputRDD = joinedPairRDD
                .map(new Function<Tuple2<Long, Tuple2<Cells, Optional<Iterable<Cells>>>>, Cells>() {

                    @Override
                    public Cells call(Tuple2<Long, Tuple2<Cells, Optional<Iterable<Cells>>>> joined) throws Exception {
                        Cells cells = new Cells();
                        cells.add(Cell.create("_id", joined._1()));
                        cells.add(Cell.create("name", joined._2()._1().getString("name")));
                        cells.add(Cell.create("players", mapCellsToPlayerName(joined._2()._2())));

                        return cells;
                    }

                    /**
                     * Transforms a list of players in a list of players' names
                     * @param teamPlayers list of team players
                     * @return List of player names
                     */
                    private List<String> mapCellsToPlayerName(final Optional<Iterable<Cells>> teamPlayers) {
                        return teamPlayers
                                .transform(new com.google.common.base.Function<Iterable<Cells>, List<String>>() {
                                    @Nullable
                                    @Override
                                    public List<String> apply(Iterable<Cells> teamPlayers) {
                                        List<String> playersNames = new ArrayList<>();
                                        for (Cells player : teamPlayers) {
                                            playersNames.add(
                                                    player.getString("lastname") + ", " + player
                                                            .getString("firstname"));
                                        }

                                        return playersNames;
                                    }
                                }).orNull();
                    }

                });

        DeepSparkContext.saveRDD(outputRDD.rdd(), outputMongodbConfig);

        deepContext.stop();

        System.exit(0);

    }
}
