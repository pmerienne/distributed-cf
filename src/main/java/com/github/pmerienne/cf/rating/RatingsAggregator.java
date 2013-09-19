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
package com.github.pmerienne.cf.rating;

import java.util.ArrayList;
import java.util.Arrays;
import java.util.List;

import storm.trident.operation.CombinerAggregator;
import storm.trident.tuple.TridentTuple;

public class RatingsAggregator implements CombinerAggregator<List<Rating>> {

	private static final long serialVersionUID = 7700181561922363727L;

	@Override
	public List<Rating> init(TridentTuple tuple) {
		long i = tuple.getLong(0);
		long j = tuple.getLong(1);
		double value = tuple.getDouble(2);

		return Arrays.asList(new Rating(i, j, value));
	}

	@Override
	public List<Rating> combine(List<Rating> val1, List<Rating> val2) {
		List<Rating> ratings = new ArrayList<>(val1.size() + val2.size());
		for (Rating rating : val1) {
			ratings.add(rating);
		}
		for (Rating rating : val2) {
			ratings.add(rating);
		}
		return ratings;
	}

	@Override
	public List<Rating> zero() {
		return new ArrayList<>();
	}
}
