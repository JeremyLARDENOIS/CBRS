package com.mycompany.app;
import java.io.File;
import java.io.FileNotFoundException;
import java.util.Scanner;
import org.neo4j.driver.*;

import reactor.util.function.Tuple3;


public class App
{
    /* Create an import function that imports movies.csv data to form such Graph patterns:
        (movie:Movie {movieId, title})
        (g:Genre {genre})
        (Movie)-[:HAS]->(Genre)
    */
    // This function return a tuple of (movieId, title, genres)
    private static void importData(String fileName) {
        File file = new File(fileName);
        // Driver without authentication
        Driver driver = GraphDatabase.driver( "bolt://localhost:7687", AuthTokens.none());
        try {
            Scanner inputStream = new Scanner(file);
            inputStream.useDelimiter(System.getProperty("line.separator"));
            while (inputStream.hasNext()) {
                String line = inputStream.next();
                // movieId,title,genres
                String[] values = line.split(",");
                System.out.println(values[0] + " " + values[1] + " " + values[2]);
                String[] genres = values[2].split("\\|");
                for (String genre : genres) {
                    System.out.println(genre);
                }
                // Create movie node
                try (Session session = driver.session()) {
                    session.run("CREATE (m:Movie {movieId: $movieId, title: $title})",
                            Values.parameters("movieId", values[0], "title", values[1]));
                }

            }
        } catch (FileNotFoundException e) {
            e.printStackTrace();
        } finally {
            driver.close();
        }
    }
    public static void main( String[] args )
    {
        importData("/home/jerem/Documents/s9/dbgraph/CBRS/ml-latest-small/movies.csv");
    }
}
