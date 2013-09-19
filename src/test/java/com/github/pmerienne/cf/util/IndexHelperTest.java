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

import java.util.HashMap;
import java.util.Map;

import org.apache.commons.lang.math.RandomUtils;
import org.fest.assertions.Assertions;
import org.junit.Test;

import com.github.pmerienne.cf.util.IndexHelper;

public class IndexHelperTest {

	@Test
	public void should_distribute_uniformely() {
		// Given
		long d = 20;
		int indexPerBlock = RandomUtils.nextInt(100);

		// When
		Map<Long, Integer> distribution = new HashMap<>();
		for (long i = 0; i < d * indexPerBlock; i++) {
			long block = IndexHelper.toBlockIndex(i, d);
			int count = distribution.containsKey(block) ? distribution.get(block) : 0;
			distribution.put(block, count + 1);
		}

		// Then
		Assertions.assertThat(distribution.values()).containsOnly(indexPerBlock);
	}
}
