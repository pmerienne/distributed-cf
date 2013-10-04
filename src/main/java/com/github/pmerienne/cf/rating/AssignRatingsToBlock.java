package com.github.pmerienne.cf.rating;

import java.util.List;

import storm.trident.operation.TridentCollector;
import storm.trident.state.BaseStateUpdater;
import storm.trident.tuple.TridentTuple;

import com.github.pmerienne.cf.util.IndexHelper;
import com.github.pmerienne.cf.util.Pair;
import com.github.pmerienne.trident.state.MapMultimapState;

public class AssignRatingsToBlock extends BaseStateUpdater<MapMultimapState<Pair<Long, Long>, Pair<Long, Long>, Rating>> {

	private static final long serialVersionUID = -7820553092528072641L;

	private long d;

	public AssignRatingsToBlock() {
	}

	public AssignRatingsToBlock(long d) {
		this.d = d;
	}

	@Override
	public void updateState(MapMultimapState<Pair<Long, Long>, Pair<Long, Long>, Rating> state, List<TridentTuple> tuples, TridentCollector collector) {
		for (TridentTuple tuple : tuples) {
			long i = tuple.getLong(0);
			long j = tuple.getLong(1);
			double value = tuple.getDouble(2);

			long p = IndexHelper.toBlockIndex(i, this.d);
			long q = IndexHelper.toBlockIndex(j, this.d);

			state.put(new Pair<Long, Long>(p, q), new Pair<Long, Long>(i, j), new Rating(i, j, value));
		}
	}

}
