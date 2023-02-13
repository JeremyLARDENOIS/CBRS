package com.mycompany.app;
import java.io.File;
import java.io.FileNotFoundException;
import java.util.ArrayList;
import java.util.Scanner;
import org.neo4j.driver.*;

public class App
{
    /////////////////////////////
    // UTILS
    public static boolean isNotInArray(String value, ArrayList<String> array) {
        for (String s : array) {
            if (s.equals(value)) {
                return false;
            }
        }
        return true;
    }
    /////////////////////////////
    /* Create an import function that imports movies.csv data to form such Graph patterns:
        (movie:Movie {movieId, title})
        (g:Genre {genre})
        (Movie)-[:HAS]->(Genre)
    */
    private static void importData(String fileName) {
        File file = new File(fileName);
        ArrayList<String> genres = new ArrayList<String>();
        Integer nMovies = 0;
        // Driver without authentication
        Driver driver = GraphDatabase.driver( "bolt://localhost:7687", AuthTokens.none());
        try {
            Scanner inputStream = new Scanner(file);
            // Skip first line of the file
            inputStream.nextLine();
            inputStream.useDelimiter(System.getProperty("line.separator"));
            while (inputStream.hasNext()) {
                String line = inputStream.next();
                // movieId,title,movieGenres
                String[] values = line.split(",");
                // System.out.println(values[0] + " " + values[1] + " " + values[2]);
                // Movie title is from the second value to the last one
                String movieTitle = "";
                for (int i = 1; i < values.length - 1; i++) {
                    movieTitle += values[i];
                }
                // Movie genres is the last value of the line
                String[] movieGenres = values[values.length - 1].split("\\|");
                for (String genre : movieGenres) {
                    // if genre is not in genres, create genre node
                    // Don't do a request if genre is already in genres
                    if (isNotInArray(genre, genres)) {
                        try (Session session = driver.session()) {
                            session.run("CREATE (g:Genre {genre: $genre})",
                                    Values.parameters("genre", genre));
                        }
                        genres.add(genre);
                    }
                }
                // Create movie node
                try (Session session = driver.session()) {
                    session.run("CREATE (m:Movie {movieId: $movieId, title: $title})",
                            Values.parameters("movieId", Integer.parseInt(values[0]), "title", movieTitle));
                    nMovies++;
                }
                // // Create HAS relationship
                for (String genre : movieGenres) {
                    try (Session session = driver.session()) {
                        session.run("MATCH (m:Movie {movieId: $movieId}), (g:Genre {genre: $genre}) CREATE (m)-[:HAS]->(g)",
                                Values.parameters("movieId", Integer.parseInt(values[0]), "genre", genre));
                    }
                }
            }
            inputStream.close();
        } catch (FileNotFoundException e) {
            e.printStackTrace();
        } finally {
            driver.close();
        }
        System.out.println("Imported " + nMovies + " movies and " + genres.size() + " genres");
    }

    private static void importUsers(String string) {
        File file = new File(string);
        Integer nRatings = 0;
        // Driver without authentication
        Driver driver = GraphDatabase.driver( "bolt://localhost:7687", AuthTokens.none());
        try {
            Scanner inputStream = new Scanner(file);
            // Skip first line of the file
            inputStream.nextLine();
            inputStream.useDelimiter(System.getProperty("line.separator"));
            while (inputStream.hasNext()) {
                String line = inputStream.next();
                // userId,movieId,rating,timestamp
                String[] values = line.split(",");
                // System.out.println(values[0] + " " + values[1] + " " + values[2]);
                // Create user node if not exists
                try (Session session = driver.session()) {
                    session.run("MERGE (u:User {userId: $userId})",
                            Values.parameters("userId", Integer.parseInt(values[0])));
                }
                // Create RATED relationship
                try (Session session = driver.session()) {
                    session.run("MATCH (u:User {userId: $userId}), (m:Movie {movieId: $movieId}) CREATE (u)-[:RATED {rating: $rating, timestamp: $timestamp}]->(m)",
                            Values.parameters("userId", Integer.parseInt(values[0]), "movieId", Integer.parseInt(values[1]), "rating", Double.parseDouble(values[2]), "timestamp", values[3]));
                    nRatings++;
                }
            }
            inputStream.close();
        } catch (FileNotFoundException e) {
            e.printStackTrace();
        } finally {
            driver.close();
        }
        System.out.println("Imported " + nRatings + " ratings");
    }

    public static void main( String[] args )
    {
        deleteAllData();
        importData("/home/jerem/Documents/s9/dbgraph/CBRS/ml-latest-small/movies.csv");
        importUsers("/home/jerem/Documents/s9/dbgraph/CBRS/ml-latest-small/ratings.csv");
        // exportAllData();
    }

    private static void exportAllData() {
        // With apoc export
        // Driver without authentication
        Driver driver = GraphDatabase.driver( "bolt://localhost:7687", AuthTokens.none());
        try (Session session = driver.session()) {
            session.run("CALL apoc.export.csv.all('/home/jerem/Documents/s9/dbgraph/CBRS/ml-latest-small/export.csv', {useTypes: true})");
        }
        driver.close();
        System.out.println("All data exported");
    }
    private static void deleteAllData() {
        // Driver without authentication
        Driver driver = GraphDatabase.driver( "bolt://localhost:7687", AuthTokens.none());
        try (Session session = driver.session()) {
            session.run("MATCH (n) DETACH DELETE n");
        }
        driver.close();
        System.out.println("All data deleted");
    }
}
