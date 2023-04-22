package org.example;

import org.junit.jupiter.api.Test;

import java.util.HashMap;
import java.util.Map;
import java.util.TreeSet;

import static org.junit.jupiter.api.Assertions.assertEquals;

public class TestParse {
    String line = "{\"reviewerID\": \"A2VNYWOPJ13AFP\", \"asin\": \"0981850006\", \"reviewerName\": \"Amazon Customer \\\"carringt0n\\\"\", \"helpful\": [6, 7], \"reviewText\": \"This was a gift for my other husband.  He's making us things from it all the time and we love the food.  Directions are simple, easy to read and interpret, and fun to make.  We all love different kinds of cuisine and Raichlen provides recipes from everywhere along the barbecue trail as he calls it. Get it and just open a page.  Have at it.  You'll love the food and it has provided us with an insight into the culture that produced it. It's all about broadening horizons.  Yum!!\", \"overall\": 5.0, \"summary\": \"Delish\", \"unixReviewTime\": 1259798400, \"reviewTime\": \"12 3, 2009\", \"category\": \"Patio_Lawn_and_Garde\"}";

    @Test
    void getCategoryTest() {
        assertEquals(ChiSquare.getCategory(line), "Patio_Lawn_and_Garde".toLowerCase());
    }

    @Test
    void getReviewTest() {
        System.out.println(ChiSquare.getOnlyReviewText(line));
        assertEquals(ChiSquare.getOnlyReviewText(line),
                "This was a gift for my other husband.  He's making us things from it all the time and we love the food.  Directions are simple, easy to read and interpret, and fun to make.  We all love different kinds of cuisine and Raichlen provides recipes from everywhere along the barbecue trail as he calls it. Get it and just open a page.  Have at it.  You'll love the food and it has provided us with an insight into the culture that produced it. It's all about broadening horizons.  Yum!!".toLowerCase()
        );
    }

    @Test
    void testCustomPairSet() {
        HashMap<String, TreeSet<CustomPair>> sortMap = new HashMap<>();
        sortMap.put("book", new TreeSet<>());
        sortMap.get("book").add(new CustomPair("nice", 2.f));
        sortMap.get("book").add(new CustomPair("bad", 1.f));
        sortMap.get("book").add(new CustomPair("no", 5.f));
        sortMap.get("book").add(new CustomPair("iii", 3.f));
        sortMap.get("book").pollFirst();
        for (Map.Entry<String, TreeSet<CustomPair>> entry : sortMap.entrySet()) {
            for (CustomPair pair : entry.getValue()) {
                System.out.println(pair.getValue());
            }
        }
    }

}
