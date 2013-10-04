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
package com.github.pmerienne.cf.recommendation;

import java.util.Collection;

import storm.trident.operation.CombinerAggregator;
import storm.trident.tuple.TridentTuple;

import com.github.pmerienne.cf.block.MatrixBlock;
import com.github.pmerienne.cf.util.MathUtil;

public class TopKItemsAggregator implements CombinerAggregator<TopKItems> {

	private static final long serialVersionUID = 211694137351634944L;

	private final int k;

	public TopKItemsAggregator(int k) {
		this.k = k;
	}

	@SuppressWarnings("unchecked")
	@Override
	public TopKItems init(TridentTuple tuple) {
		TopKItems initial = new TopKItems(this.k);

		double[] ui = (double[]) tuple.get(0);
		Collection<Long> ratedItems = (Collection<Long>) tuple.get(1);
		MatrixBlock vq = (MatrixBlock) tuple.get(2);

		double[] vj;
		double score;
		for (long item : vq.indexes()) {
			if (!ratedItems.contains(item)) {
				vj = vq.get(item);
				score = MathUtil.dot(ui, vj);
				initial.add(new Recommendation(item, score));
				initial.ensureSize();
			}
		}

		return initial;
	}

	@Override
	public TopKItems combine(TopKItems val1, TopKItems val2) {
		TopKItems combined = new TopKItems(this.k);
		combined.addAll(val1);
		combined.addAll(val2);
		combined.ensureSize();

		return combined;
	}

	@Override
	public TopKItems zero() {
		TopKItems initial = new TopKItems(this.k);
		return initial;
	}

}
