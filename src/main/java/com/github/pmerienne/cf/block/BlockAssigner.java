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
package com.github.pmerienne.cf.block;

import com.github.pmerienne.cf.util.IndexHelper;

import storm.trident.operation.BaseFunction;
import storm.trident.operation.TridentCollector;
import storm.trident.tuple.TridentTuple;
import backtype.storm.tuple.Values;

public class BlockAssigner extends BaseFunction {

	private static final long serialVersionUID = 312330145173988794L;

	private final long d;

	public BlockAssigner(long d) {
		this.d = d;
	}

	@Override
	public void execute(TridentTuple tuple, TridentCollector collector) {
		long i = tuple.getLong(0);
		long j = tuple.getLong(1);
		long p = IndexHelper.toBlockIndex(i, this.d);
		long q = IndexHelper.toBlockIndex(j, this.d);

		collector.emit(new Values(p, q));
	}
}
