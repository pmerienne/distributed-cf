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
