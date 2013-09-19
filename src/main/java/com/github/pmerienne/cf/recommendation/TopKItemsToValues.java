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

import storm.trident.operation.BaseFunction;
import storm.trident.operation.TridentCollector;
import storm.trident.tuple.TridentTuple;
import backtype.storm.tuple.Values;

public class TopKItemsToValues extends BaseFunction {

	private static final long serialVersionUID = 6862039869715365207L;

	@Override
	public void execute(TridentTuple tuple, TridentCollector collector) {
		TopKItems topKItems = (TopKItems) tuple.get(0);
		for (Recommendation recommendation : topKItems) {
			collector.emit(new Values(recommendation.item, recommendation.score));
		}
	}

}
