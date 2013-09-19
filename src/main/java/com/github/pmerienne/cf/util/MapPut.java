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
package com.github.pmerienne.cf.util;

import java.util.ArrayList;
import java.util.Arrays;
import java.util.List;

import storm.trident.operation.TridentCollector;
import storm.trident.state.BaseStateUpdater;
import storm.trident.state.map.MapState;
import storm.trident.tuple.TridentTuple;

@SuppressWarnings({ "unchecked", "rawtypes" })
public class MapPut extends BaseStateUpdater<MapState> {

	private static final long serialVersionUID = -6301074755036758874L;

	@Override
	public void updateState(MapState state, List<TridentTuple> tuples, TridentCollector collector) {
		List keys = new ArrayList<>(tuples.size());
		List values = new ArrayList<>(tuples.size());

		for (TridentTuple tuple : tuples) {
			keys.add(Arrays.asList(tuple.get(0)));
			values.add(tuple.get(1));
		}

		state.multiPut(keys, values);
	}

}
