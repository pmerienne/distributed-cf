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

import static com.github.pmerienne.cf.util.IndexHelper.toBlockIndex;
import static org.mockito.Mockito.mock;
import static org.mockito.Mockito.verify;
import static org.mockito.Mockito.when;

import org.junit.Test;

import com.github.pmerienne.cf.block.BlockAssigner;

import storm.trident.operation.TridentCollector;
import storm.trident.tuple.TridentTuple;
import backtype.storm.tuple.Values;

public class BlockAssignerTest {

	@Test
	public void should_emit_block_indexes() {
		// Given
		long d = 5;
		BlockAssigner function = new BlockAssigner(d);

		TridentCollector collector = mock(TridentCollector.class);
		long i = 1;
		long j = 5;
		TridentTuple tuple = mock(TridentTuple.class);
		when(tuple.getLong(0)).thenReturn(i);
		when(tuple.getLong(1)).thenReturn(j);

		// When
		function.execute(tuple, collector);

		// Then
		verify(collector).emit(new Values(toBlockIndex(i, d), toBlockIndex(j, d)));
	}
}
