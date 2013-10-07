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
import java.util.Collection;
import java.util.LinkedList;
import java.util.List;
import java.util.Map;

import storm.trident.operation.TridentCollector;
import storm.trident.state.BaseQueryFunction;
import storm.trident.tuple.TridentTuple;
import backtype.storm.tuple.Values;

import com.github.pmerienne.cf.util.Pair;
import com.github.pmerienne.trident.state.MapMultimapState;

public class GetRatingsFromBlock extends BaseQueryFunction<MapMultimapState<Pair<Long, Long>, Pair<Long, Long>, Rating>, Collection<Rating>> {

	private static final long serialVersionUID = 2676085272525301635L;

	@Override
	public List<Collection<Rating>> batchRetrieve(MapMultimapState<Pair<Long, Long>, Pair<Long, Long>, Rating> state, List<TridentTuple> tuples) {
		List<Collection<Rating>> results = new ArrayList<>(tuples.size());

		long p, q;
		Map<Pair<Long, Long>, Rating> ratings;
		for (TridentTuple tuple : tuples) {
			p = tuple.getLong(0);
			q = tuple.getLong(1);

			ratings = state.getAll(new Pair<Long, Long>(p, q));
			results.add(new LinkedList<>(ratings.values()));
		}

		return results;
	}

	@Override
	public void execute(TridentTuple tuple, Collection<Rating> result, TridentCollector collector) {
		collector.emit(new Values(result));
	}

}
