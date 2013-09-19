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

import static org.fest.assertions.Assertions.assertThat;

import java.util.ArrayList;
import java.util.List;

import org.junit.Test;

import com.github.pmerienne.cf.block.Block;
import com.github.pmerienne.cf.block.BlockLocker;

// TODO : test concurrency
public class BlockLockerTest {

	@Test
	public void should_block_processing() {
		// Given
		long d = 4;
		BlockLocker blockLocker = initBlockLocker(d);

		// When
		List<Block> processing = new ArrayList<>();
		for (int i = 0; i < d; i++) {
			processing.add(blockLocker.getAndLockRandomBlock());
		}

		// Then
		assertThat(blockLocker.getAndLockRandomBlock()).isNull();
	}

	@Test
	public void should_unlock_blocks() {
		// Given
		long d = 4;
		BlockLocker blockLocker = initBlockLocker(d);
		List<Block> processing = new ArrayList<>();
		for (int i = 0; i < d; i++) {
			processing.add(blockLocker.getAndLockRandomBlock());
		}

		// When
		blockLocker.unlock(processing.get(0));

		// Then
		assertThat(blockLocker.getAndLockRandomBlock()).isNotNull();
	}

	protected BlockLocker initBlockLocker(long d) {
		BlockLocker blockLocker = new BlockLocker();

		for (long p = 0; p < d; p++) {
			for (long q = 0; q < d; q++) {
				blockLocker.add(new Block(p, q));
			}
		}
		return blockLocker;
	}
}
