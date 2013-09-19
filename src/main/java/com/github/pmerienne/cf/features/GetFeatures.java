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
package com.github.pmerienne.cf.features;

import static com.github.pmerienne.cf.util.MapStateUtil.singleValue;
import static com.github.pmerienne.cf.util.MapStateUtil.toKeys;

import java.util.ArrayList;
import java.util.List;

import storm.trident.operation.TridentCollector;
import storm.trident.state.BaseQueryFunction;
import storm.trident.state.map.MapState;
import storm.trident.tuple.TridentTuple;
import backtype.storm.tuple.Values;

import com.github.pmerienne.cf.block.MatrixBlock;
import com.github.pmerienne.cf.util.IndexHelper;

public class GetFeatures extends BaseQueryFunction<MapState<MatrixBlock>, double[]> {

	private static final long serialVersionUID = 8168057749116807981L;

	private final long d;

	public GetFeatures(long d) {
		this.d = d;
	}

	@Override
	public List<double[]> batchRetrieve(MapState<MatrixBlock> state, List<TridentTuple> tuples) {
		List<double[]> features = new ArrayList<>(tuples.size());
		List<List<Object>> keys;
		MatrixBlock matrixBlock;

		for (TridentTuple tuple : tuples) {
			long index = tuple.getLong(0);
			long blockIndex = IndexHelper.toBlockIndex(index, this.d);

			keys = toKeys(blockIndex);
			matrixBlock = singleValue(state.multiGet(keys));

			double[] feature = matrixBlock != null ? matrixBlock.get(index) : null;
			features.add(feature);
		}

		return features;
	}

	@Override
	public void execute(TridentTuple tuple, double[] result, TridentCollector collector) {
		collector.emit(new Values(result));
	}

}
