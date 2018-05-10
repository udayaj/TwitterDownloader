/*
 * To change this template, choose Tools | Templates
 * and open the template in the editor.
 */
package twitterdownload;

import java.util.Date;
import java.util.Timer;
import java.util.TimerTask;
import org.apache.commons.daemon.*;

import java.io.File;
import java.sql.Connection;
import java.sql.DriverManager;
import java.sql.PreparedStatement;
import java.sql.SQLException;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.Date;
import org.apache.commons.configuration.*;
import org.apache.commons.lang.exception.NestableException;
import twitter4j.*;
import twitter4j.conf.ConfigurationBuilder;


public class Main implements Daemon {

    private static String CONSUMER_KEY = "";
    private static String CONSUMER_KEY_SECRET = "";
    private static String ACCESS_TOKEN = "";
    private static String ACCESS_TOKEN_SECRET = "";
    private static String DB_HOST = "";
    private static String DB_PORT = "";
    private static String DB_USER = "";
    private static String DB_PASSWORD = "";
    private static String DB_NAME = "";
    private static final String NON_ALPHA_NUMERIC_PATTERN = "^$|^[^a-zA-Z\\d:]*$";
    private static final String OHIO_PATTERN = "(.*[^a-zA-Z\\d:]{1}|^)(oh|OH|ohio|Ohio|OHIO){1}([^a-zA-Z\\d:]{1}.*|$)";
    private static Connection dbcon = null;

    public static void main(String[] args) throws TwitterException {


        System.out.println("Streaming started at " + new Date().toString());
        try {
            DefaultConfigurationBuilder builder = new DefaultConfigurationBuilder();
            File f = new File("config/config.xml");
            builder.setFile(f);
            CombinedConfiguration config = builder.getConfiguration(true);

            CONSUMER_KEY = config.getString("auth.consumer-key");
            CONSUMER_KEY_SECRET = config.getString("auth.consumer-secret");
            ACCESS_TOKEN = config.getString("auth.access-token");
            ACCESS_TOKEN_SECRET = config.getString("auth.access-token-secret");

            System.out.println("CONSUMER_KEY: " + CONSUMER_KEY);
            System.out.println("CONSUMER_KEY_SECRET: " + CONSUMER_KEY_SECRET);
            System.out.println("ACCESS_TOKEN: " + ACCESS_TOKEN);
            System.out.println("ACCESS_TOKEN_SECRET: " + ACCESS_TOKEN_SECRET);

            DB_HOST = config.getString("database.host");
            DB_PORT = config.getString("database.port");
            DB_USER = config.getString("database.user");
            DB_PASSWORD = config.getString("database.password");
            DB_NAME = config.getString("database.name");

            //XMLConfiguration config = new XMLConfiguration("config\\config.xml");
            //System.out.println("consumer key: " + config.getString("database.url"));     
            Class.forName("org.postgresql.Driver");
            String dbUrl = "jdbc:postgresql://" + DB_HOST + ":" + DB_PORT + "/" + DB_NAME;

            dbcon = DriverManager.getConnection(dbUrl, DB_USER, DB_PASSWORD);
            //System.exit(0);
        } catch (org.apache.commons.configuration.ConfigurationException cex) {
            System.out.println("Error occurred while reading configurations....");
            System.out.println(cex);
            System.exit(0);
        } catch (ClassNotFoundException ex) {
            System.out.println("Database Driver not found....");
            System.out.println(ex);
            System.exit(0);
        } catch (SQLException ex) {
            System.out.println("Database Connection failed....");
            System.out.println(ex);
            System.exit(0);
        }

        //String[] arrParams = {"flu,fever"};
        String[] arrParams = {};
        String[] arrLang = {"en"};
        //TO-DO: Move the bounding box to the config flies
        double[][] arrLocations = {{-84.812071, 38.400511}, {-80.519996, 41.986872}};
        //-122.75,36.8,-121.75,37.8

        StatusListener listener = new StatusListener() {
            @Override
            public void onStatus(Status status) {
                //System.out.println("@" + status.getUser().getScreenName() + " - " + status.getText());
                //System.out.print(status);
                //System.out.println();
                insertToTable(status);
            }

            @Override
            public void onDeletionNotice(StatusDeletionNotice statusDeletionNotice) {
                System.out.println("Got a status deletion notice id:" + statusDeletionNotice.getStatusId());
            }

            @Override
            public void onTrackLimitationNotice(int numberOfLimitedStatuses) {
                System.out.println("Got track limitation notice:" + numberOfLimitedStatuses);
            }

            @Override
            public void onScrubGeo(long userId, long upToStatusId) {
                System.out.println("Got scrub_geo event userId:" + userId + " upToStatusId:" + upToStatusId);
            }

            @Override
            public void onStallWarning(StallWarning warning) {
                System.out.println("Got stall warning:" + warning);
            }

            @Override
            public void onException(Exception ex) {
                ex.printStackTrace();
            }
        };

        ConfigurationBuilder cb = new ConfigurationBuilder();
        cb.setDebugEnabled(true);
        cb.setOAuthConsumerKey(CONSUMER_KEY);
        cb.setOAuthConsumerSecret(CONSUMER_KEY_SECRET);
        cb.setOAuthAccessToken(ACCESS_TOKEN);
        cb.setOAuthAccessTokenSecret(ACCESS_TOKEN_SECRET);

        TwitterStream twitterStream = new TwitterStreamFactory(cb.build()).getInstance();

        twitterStream.addListener(listener);
        ArrayList<Long> follow = new ArrayList<Long>();
        ArrayList<String> track = new ArrayList<String>();
        for (String arg : arrParams) {
            if (isNumericalArgument(arg)) {
                for (String id : arg.split(",")) {
                    follow.add(Long.parseLong(id));
                }
            } else {
                track.addAll(Arrays.asList(arg.split(",")));
            }
        }
        long[] followArray = new long[follow.size()];
        for (int i = 0; i < follow.size(); i++) {
            followArray[i] = follow.get(i);
        }
        String[] trackArray = track.toArray(new String[track.size()]);

        // filter() method internally creates a thread which manipulates TwitterStream and calls these adequate listener methods continuously.
        FilterQuery filterQuery = new FilterQuery();
        filterQuery.locations(arrLocations);
        filterQuery.language(arrLang);
        filterQuery.count(0);
        twitterStream.filter(filterQuery);
        //twitterStream.filter(new FilterQuery(0, null, null, arrLocations, arrLang));        
    }

    private static boolean isNumericalArgument(String argument) {
        String args[] = argument.split(",");
        boolean isNumericalArgument = true;
        for (String arg : args) {
            try {
                Integer.parseInt(arg);
            } catch (NumberFormatException nfe) {
                isNumericalArgument = false;
                break;
            }
        }
        return isNumericalArgument;
    }

    private static void insertToTable(Status status) {
        try {
            String sql = "INSERT INTO public.\"Message\" (\"TwitterId\", \"MsgText\", \"Source\", \"Longitude\", "
                    + "\"Latitude\", \"CreatedTime\", \"PlaceCountry\", \"PlaceCountryCode\", \"PlaceFullName\", "
                    + "\"PlaceTwitterId\", \"PlaceName\", \"PlaceType\", \"PlacePolygon\", \"RetweetCount\", \"UserLocation\")"
                    + "VALUES (?, ?, ?, ?,"
                    + "?, ?, ?, ?, ?,"
                    + "?, ?, ?, ?, ?, ?);";

            PreparedStatement preparedStatement = dbcon.prepareStatement(sql);
            preparedStatement.setLong(1, status.getId());//twitter id
            preparedStatement.setString(2, status.getText());//text
            preparedStatement.setString(3, status.getSource());//source

            GeoLocation origLocation = status.getGeoLocation();
            if (origLocation == null || origLocation.getLongitude() == 0) {
                preparedStatement.setNull(4, java.sql.Types.DOUBLE);//Long
                preparedStatement.setNull(5, java.sql.Types.DOUBLE);//Lat
            } else {
                preparedStatement.setDouble(4, origLocation.getLongitude());//Long
                preparedStatement.setDouble(5, origLocation.getLatitude());//Lat
            }

            preparedStatement.setTimestamp(6, new java.sql.Timestamp(status.getCreatedAt().getTime()));//CreatedTime

            Place origPlace = status.getPlace();
            if (origPlace == null) {
                preparedStatement.setNull(7, java.sql.Types.CHAR);//PlaceCountry
                preparedStatement.setNull(8, java.sql.Types.CHAR);//PlaceCountryCode
                preparedStatement.setNull(9, java.sql.Types.CHAR);//PlaceFullName
                preparedStatement.setNull(10, java.sql.Types.CHAR);//PlaceTwitterId
                preparedStatement.setNull(11, java.sql.Types.CHAR);//PlaceName
                preparedStatement.setNull(12, java.sql.Types.CHAR);//PlaceType
                preparedStatement.setNull(13, java.sql.Types.CHAR);//PlacePolygon
            } else {
                if (origPlace.getCountry() == "Canada" || !origPlace.getFullName().matches(OHIO_PATTERN)) {
                    return;
                }
                preparedStatement.setString(7, origPlace.getCountry());//PlaceCountry
                preparedStatement.setString(8, origPlace.getCountryCode());//PlaceCountryCode
                preparedStatement.setString(9, origPlace.getFullName());//PlaceFullName
                preparedStatement.setString(10, origPlace.getId());//PlaceTwitterId
                preparedStatement.setString(11, origPlace.getName());//PlaceName
                preparedStatement.setString(12, origPlace.getPlaceType());//PlaceType

                String strPlacePolygon = getCoordinateString(origPlace.getBoundingBoxCoordinates());
                if (strPlacePolygon == null) {
                    preparedStatement.setNull(13, java.sql.Types.CHAR);//PlacePolygon
                } else {
                    preparedStatement.setString(13, strPlacePolygon);//PlacePolygon
                }
            }

            preparedStatement.setInt(14, status.getRetweetCount());//RetweetCount

            User origUser = status.getUser();
            if (origUser == null || origUser.getLocation() == null || origUser.getLocation().matches(NON_ALPHA_NUMERIC_PATTERN)) {
                preparedStatement.setNull(15, java.sql.Types.CHAR);//UserLocation  
            } else {
                preparedStatement.setString(15, status.getUser().getLocation());//UserLocation     
            }

            preparedStatement.executeUpdate();

            //dbcon.close();
        } catch (SQLException ex) {
            System.out.println(ex);
        }
    }

    private static String getCoordinateString(GeoLocation[][] arrGeoLocations) {
        if (arrGeoLocations == null || arrGeoLocations.length < 1) {
            return null;
        }
        String str = "";
        for (int i = 0; i < arrGeoLocations.length; i++) {
            for (int j = 0; j < arrGeoLocations[i].length; j++) {
                if (i == 0 && j == 0) {
                    str = arrGeoLocations[i][j].getLongitude() + " " + arrGeoLocations[i][j].getLatitude();
                } else {
                    str = str + "," + arrGeoLocations[i][j].getLongitude() + " " + arrGeoLocations[i][j].getLatitude();
                }
            }
        }

        str = "POLYGON((" + str + "))";
        //System.out.println(str);
        return str;
    }

    @Override
    public void init(DaemonContext dc) throws DaemonInitException, Exception {
        System.out.println("initializing ...");
    }

    @Override
    public void start() throws Exception {
        System.out.println("starting ...");
        main(null);
    }

    @Override
    public void stop() throws Exception {
        System.out.println("stopping ...");
    }

    @Override
    public void destroy() {
        System.out.println("Stopped !");
    }
}
