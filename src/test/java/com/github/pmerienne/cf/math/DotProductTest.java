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
package com.github.pmerienne.cf.math;

import static org.apache.commons.lang.math.RandomUtils.nextDouble;
import static org.fest.assertions.Assertions.assertThat;
import static org.mockito.Mockito.mock;
import static org.mockito.Mockito.verify;
import static org.mockito.Mockito.when;

import org.junit.Test;
import org.mockito.ArgumentCaptor;

import storm.trident.operation.TridentCollector;
import storm.trident.tuple.TridentTuple;
import backtype.storm.tuple.Values;

import com.github.pmerienne.cf.math.DotProduct;
import com.github.pmerienne.cf.util.MathUtil;

public class DotProductTest {

	@Test
	public void should_emit_dot_product() {
		// Given
		DotProduct function = new DotProduct();
		TridentCollector collector = mock(TridentCollector.class);

		double[] v1 = new double[] { nextDouble(), nextDouble(), nextDouble() };
		double[] v2 = new double[] { nextDouble(), nextDouble(), nextDouble() };
		TridentTuple tuple = mock(TridentTuple.class);
		when(tuple.get(0)).thenReturn(v1);
		when(tuple.get(1)).thenReturn(v2);

		// When
		function.execute(tuple, collector);

		// Then
		final ArgumentCaptor<Values> valuesCaptor = ArgumentCaptor.forClass(Values.class);
		verify(collector).emit(valuesCaptor.capture());
		assertThat((double)valuesCaptor.getValue().get(0)).isEqualTo(MathUtil.dot(v1, v2));
	}
}
