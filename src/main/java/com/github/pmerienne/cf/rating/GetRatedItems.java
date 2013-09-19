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

import static com.github.pmerienne.cf.util.MapStateUtil.singleValue;
import static com.github.pmerienne.cf.util.MapStateUtil.toKeys;

import java.util.ArrayList;
import java.util.HashSet;
import java.util.List;
import java.util.Set;

import storm.trident.operation.TridentCollector;
import storm.trident.state.BaseQueryFunction;
import storm.trident.state.map.MapState;
import storm.trident.tuple.TridentTuple;
import backtype.storm.tuple.Values;

public class GetRatedItems extends BaseQueryFunction<MapState<List<Rating>>, List<Rating>> {

	private static final long serialVersionUID = 3569123559849366626L;

	@Override
	public List<List<Rating>> batchRetrieve(MapState<List<Rating>> state, List<TridentTuple> tuples) {
		List<List<Rating>> results = new ArrayList<>(tuples.size());

		List<List<Object>> keys;
		List<Rating> blockRatings;
		for (TridentTuple tuple : tuples) {
			long p = tuple.getLong(1);
			long q = tuple.getLong(2);

			keys = toKeys(p, q);
			blockRatings = singleValue(state.multiGet(keys));

			results.add(blockRatings);
		}

		return results;
	}

	@Override
	public void execute(TridentTuple tuple, List<Rating> blockRatings, TridentCollector collector) {
		long user = tuple.getLong(0);
		Set<Long> ratedItems = new HashSet<>();

		for (Rating rated : blockRatings) {
			if (rated.i == user) {
				ratedItems.add(rated.j);
			}
		}

		collector.emit(new Values(ratedItems));
	}
}
