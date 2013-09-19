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

import static org.fest.assertions.Assertions.assertThat;

import org.apache.commons.lang.math.RandomUtils;
import org.junit.Test;

import com.github.pmerienne.cf.recommendation.Recommendation;
import com.github.pmerienne.cf.recommendation.TopKItems;

public class TopKItemsTest {

	@Test
	public void should_not_exceed_max_size() {
		// Given
		int maxSize = 10;
		TopKItems topKItems = new TopKItems(maxSize);
		for (int i = 0; i < maxSize * 2; i++) {
			topKItems.add(new Recommendation(RandomUtils.nextLong(), RandomUtils.nextDouble()));
		}

		// When
		topKItems.ensureSize();

		// Then
		assertThat(topKItems.size()).isLessThanOrEqualTo(maxSize);
	}

	@Test
	public void should_sort_recommendations() {
		// Given
		int maxSize = 10;

		// When
		TopKItems topKItems = new TopKItems(maxSize);
		for (int i = 0; i < maxSize; i++) {
			topKItems.add(new Recommendation(RandomUtils.nextLong(), RandomUtils.nextDouble()));
		}

		// Then
		double previousScore = Double.MAX_VALUE;
		for (Recommendation recommendation : topKItems) {
			assertThat(recommendation.getScore()).isLessThan(previousScore);
			previousScore = recommendation.getScore();
		}
	}
}
