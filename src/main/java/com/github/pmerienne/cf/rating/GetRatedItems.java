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

import java.util.Collection;
import java.util.HashSet;
import java.util.Set;

import storm.trident.operation.TridentCollector;
import storm.trident.tuple.TridentTuple;
import backtype.storm.tuple.Values;

public class GetRatedItems extends GetRatingsFromBlock {

	private static final long serialVersionUID = 3569123559849366626L;

	@Override
	public void execute(TridentTuple tuple, Collection<Rating> blockRatings, TridentCollector collector) {
		Set<Long> ratedItems = new HashSet<>();
		
		final long user = tuple.getLong(0);
		for(Rating rating : blockRatings) {
			if(rating.i == user) {
				ratedItems.add(rating.j);
			}
		}

		collector.emit(new Values(ratedItems));
	}
}
