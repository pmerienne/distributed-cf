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
import static org.fest.assertions.Delta.delta;
import static org.mockito.Mockito.mock;
import static org.mockito.Mockito.times;
import static org.mockito.Mockito.verify;
import static org.mockito.Mockito.when;

import java.util.List;

import org.fest.assertions.Assertions;
import org.fest.assertions.Delta;
import org.junit.Test;
import org.mockito.ArgumentCaptor;

import com.github.pmerienne.cf.recommendation.Recommendation;
import com.github.pmerienne.cf.recommendation.TopKItems;
import com.github.pmerienne.cf.recommendation.TopKItemsToValues;

import storm.trident.operation.TridentCollector;
import storm.trident.tuple.TridentTuple;
import backtype.storm.tuple.Values;

public class TopKItemsToValuesTest {

	@Test
	public void should_emit_values_for_each_recommendation() {
		// Given
		TopKItemsToValues function = new TopKItemsToValues();
		TridentCollector collector = mock(TridentCollector.class);

		TridentTuple tuple = mock(TridentTuple.class);
		TopKItems topKItems = new TopKItems(3);
		topKItems.add(new Recommendation(3, 0.3));
		topKItems.add(new Recommendation(2, 0.2));
		topKItems.add(new Recommendation(1, 0.1));
		when(tuple.get(0)).thenReturn(topKItems);

		// When
		function.execute(tuple, collector);

		// Then
		final ArgumentCaptor<Values> valuesCaptor = ArgumentCaptor.forClass(Values.class);
		verify(collector, times(3)).emit(valuesCaptor.capture());

		List<Values> actualValues = valuesCaptor.getAllValues();
		assertThat(actualValues).hasSize(3);
		assertThat((long) actualValues.get(0).get(0)).isEqualTo(3);
		assertThat((double) actualValues.get(0).get(1)).isEqualTo(0.3, Delta.delta(10e-3));
		assertThat((long) actualValues.get(1).get(0)).isEqualTo(2);
		assertThat((double) actualValues.get(1).get(1)).isEqualTo(0.2, Delta.delta(10e-3));
		assertThat((long) actualValues.get(2).get(0)).isEqualTo(1);
		assertThat((double) actualValues.get(2).get(1)).isEqualTo(0.1, Delta.delta(10e-3));
	}

	protected void assertEmited(TridentCollector collector, long item, double score) {
		final ArgumentCaptor<Double> captor = ArgumentCaptor.forClass(Double.class);
		verify(collector).emit(new Values(item, captor.capture()));
		Assertions.assertThat(score).isEqualTo(score, delta(10E-3));
	}

}
