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

import java.util.HashSet;
import java.util.Set;

import storm.trident.operation.CombinerAggregator;
import storm.trident.tuple.TridentTuple;

public class RatingsAggregator implements CombinerAggregator<Set<Rating>> {

	private static final long serialVersionUID = 7700181561922363727L;

	@Override
	public Set<Rating> init(TridentTuple tuple) {
		long i = tuple.getLong(0);
		long j = tuple.getLong(1);
		double value = tuple.getDouble(2);

		Set<Rating> ratings = this.zero();
		ratings.add(new Rating(i, j, value));

		return ratings;
	}

	@Override
	public Set<Rating> combine(Set<Rating> val1, Set<Rating> val2) {
		Set<Rating> ratings = new HashSet<>(val1.size() + val2.size());
		for (Rating rating : val2) {
			ratings.add(rating);
		}
		for (Rating rating : val1) {
			ratings.add(rating);
		}
		return ratings;
	}

	@Override
	public Set<Rating> zero() {
		return new HashSet<>();
	}
}
