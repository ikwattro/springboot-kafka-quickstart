package com.example;


import org.neo4j.driver.v1.*;

import java.util.*;

public class KafkaListener {

    private final Driver driver;

    public KafkaListener() {
        this.driver = GraphDatabase.driver( "bolt://localhost:7687",
                AuthTokens.basic( "neo4j", "neo4j" ),
                Config.build().withEncryptionLevel(Config.EncryptionLevel.NONE).toConfig()
        );
    }

    @org.springframework.kafka.annotation.KafkaListener(topics = "test3")
    public void listen(String message) {

        String[] parts = message.split(",");

        List<List<String>> collection = new ArrayList<>();

        int startYear = 2013;

        for (int i =0; i < 1000; i++) {
            List<String> elt1 = new ArrayList<>();
            startYear++;
            elt1.add(message+"-"+i);
            elt1.add(message+"-"+i+1);
            elt1.add(String.valueOf(startYear));
            collection.add(elt1);
        }

        System.out.println(message);




        try (Session session = driver.session()){
            session.run("UNWIND {coll} AS elt MERGE (n:Person {name: elt[0] }) " +
                    "MERGE (n2:Person {name: elt[1]}) " +
                    "MERGE (n)-[:KNOWS {since: elt[2]} ]->(n2)",
                    Collections.singletonMap("coll", collection));
        }

//
//        Long start = System.currentTimeMillis();
//
//        System.out.println("query took " + (System.currentTimeMillis() - start));
    }
}
