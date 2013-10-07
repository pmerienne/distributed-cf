/**
 * Copyright 2013-2015 Pierre Merienne
 *
 * Licensed under the Apache License, Version 2.0 (the "License");
 * you may not use this file except in compliance with the License.
 * You may obtain a copy of the License at
 *
 * 		http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */
package com.github.pmerienne.cf.testing.dataset;

import java.io.BufferedReader;
import java.io.FileInputStream;
import java.io.IOException;
import java.io.InputStreamReader;
import java.util.ArrayList;
import java.util.Collections;
import java.util.HashMap;
import java.util.List;
import java.util.Map;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import com.github.pmerienne.cf.rating.Rating;

public class FileDatasetUtil {

	private static final Logger LOGGER = LoggerFactory.getLogger(FileDatasetUtil.class);

	private final static String MOVIELENS_FILE = "src/test/resources/movielens.csv";
	private final static RatingParser MOVIELENS_PARSER = new MovieLensRatingParser();

	private static Map<String, List<Rating>> LOADED_RATINGS = new HashMap<>();

	public static List<Rating> getMovieLensRatings() {
		return get(MOVIELENS_FILE, MOVIELENS_PARSER);
	}

	public static List<Rating> get(String filename, RatingParser ratingParser) {
		List<Rating> ratings = null;
		if (!LOADED_RATINGS.containsKey(filename)) {
			ratings = loadRatings(filename, ratingParser);
			LOADED_RATINGS.put(filename, ratings);
		} else {
			ratings = LOADED_RATINGS.get(filename);
		}

		return ratings;
	}

	private static List<Rating> loadRatings(String filename, RatingParser ratingParser) {
		LOGGER.info("Loading ratings from " + filename);

		List<Rating> ratings = new ArrayList<Rating>();

		FileInputStream is = null;
		BufferedReader br = null;
		try {
			is = new FileInputStream(filename);
			br = new BufferedReader(new InputStreamReader(is));

			String line;
			while ((line = br.readLine()) != null) {
				try {
					Rating rating = ratingParser.parse(line);
					ratings.add(rating);
				} catch (Exception ex) {
					System.err.println("Skipped rating : " + line);
				}
			}
		} catch (IOException ioe) {
			ioe.printStackTrace();
		} finally {
			try {
				is.close();
				br.close();
			} catch (IOException e) {
				e.printStackTrace();
			}
		}

		Collections.shuffle(ratings);
		return ratings;
	}

	public static interface RatingParser {

		Rating parse(String line) throws Exception;
	}

	public static class MovieLensRatingParser implements RatingParser {

		@Override
		public Rating parse(String line) throws Exception {
			String[] values = line.split("\t");

			long user = Long.parseLong(values[0]);
			long item = Long.parseLong(values[1]);
			double stars = Integer.parseInt(values[2]);

			return new Rating(user, item, stars);
		}

	}
}