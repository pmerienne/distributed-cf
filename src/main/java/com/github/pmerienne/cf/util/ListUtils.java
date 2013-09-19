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
import java.util.List;

import org.apache.commons.lang.math.RandomUtils;

public class ListUtils {

	public static <T> List<T> randomSubList(List<T> items, int m) {
		List<T> res = new ArrayList<T>(m);
		int n = items.size();
		for (int i = n - m; i < n; i++) {
			int pos = RandomUtils.nextInt(i + 1);
			T item = items.get(pos);
			if (res.contains(item))
				res.add(items.get(i));
			else
				res.add(item);
		}
		return res;
	}
}
