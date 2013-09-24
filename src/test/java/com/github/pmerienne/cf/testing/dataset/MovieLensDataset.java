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
import java.io.File;
import java.io.FileInputStream;
import java.io.IOException;
import java.io.InputStreamReader;
import java.util.ArrayList;
import java.util.Collections;
import java.util.List;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import com.github.pmerienne.cf.rating.Rating;

public class MovieLensDataset {

	private static final Logger LOGGER = LoggerFactory.getLogger(MovieLensDataset.class);

	private final static File MOVIE_LENS_FILE = new File("src/test/resources/movielens.csv");

	private static List<Rating> RATINGS = null;

	public static List<Rating> get() {
		if (RATINGS == null) {
			RATINGS = loadRatings();
		}
		return new ArrayList<Rating>(RATINGS);
	}

	private static List<Rating> loadRatings() {
		LOGGER.info("Loading movielens preferences from " + MOVIE_LENS_FILE);

		List<Rating> ratings = new ArrayList<Rating>();

		FileInputStream is = null;
		BufferedReader br = null;
		try {
			is = new FileInputStream(MOVIE_LENS_FILE);
			br = new BufferedReader(new InputStreamReader(is));

			String line;
			while ((line = br.readLine()) != null) {
				try {
					String[] values = line.split("\t");

					long user = Long.parseLong(values[0]);
					long item = Long.parseLong(values[1]);
					double stars = Integer.parseInt(values[2]);
					double value = (stars / 5);

					ratings.add(new Rating(user, item, value));
				} catch (Exception ex) {
					System.err.println("Skipped movie lens sample : " + line);
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

}