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

import java.io.Serializable;
import java.util.ArrayList;
import java.util.Collections;
import java.util.HashSet;
import java.util.List;
import java.util.Set;

import com.google.common.base.Predicate;
import com.google.common.collect.Iterables;

public class BlockLocker implements Serializable {

	private static final long serialVersionUID = 8457268238107199404L;

	private final List<Block> toProcesses = new ArrayList<>();

	private final Set<Block> lockedBlocks = new HashSet<Block>();
	private final Set<Long> lockedRows = new HashSet<Long>();
	private final Set<Long> lockedColumns = new HashSet<Long>();

	public synchronized void add(Block block) {
		this.toProcesses.add(block);
	}

	public synchronized Block getAndLockRandomBlock() {
		Block unlockedBlock = Iterables.tryFind(this.toProcesses, new Predicate<Block>() {
			@Override
			public boolean apply(Block block) {
				return isUnlocked(block);
			}
		}).orNull();

		if (unlockedBlock != null) {
			this.lock(unlockedBlock);
		}

		return unlockedBlock;
	}

	public synchronized Set<Block> getAndLockRandomBlocks() {
		Collections.shuffle(this.toProcesses);

		Set<Block> unlockedBlocks = new HashSet<>();
		Block unlockedBlock = null;
		do {
			unlockedBlock = this.getAndLockRandomBlock();
			if (unlockedBlock != null) {
				unlockedBlocks.add(unlockedBlock);
			}
		} while (unlockedBlock != null);

		return unlockedBlocks;
	}

	/**
	 * Unlock all blocks in row p and all blocks in column q.
	 * 
	 * @param p
	 * @param q
	 */
	public synchronized void unlock(Block block) {
		this.lockedBlocks.remove(block);
		this.refreshLockedRowsAndColumns();
	}

	/**
	 * Lock all blocks in row p and all blocks in column q.
	 * 
	 * @param p
	 * @param q
	 */
	protected synchronized void lock(Block block) {
		this.lockedBlocks.add(block);
		this.refreshLockedRowsAndColumns();
	}

	protected boolean isUnlocked(Block block) {
		return !this.lockedRows.contains(block.getP()) && !this.lockedColumns.contains(block.getQ());
	}

	protected synchronized void refreshLockedRowsAndColumns() {
		this.lockedRows.clear();
		this.lockedColumns.clear();

		for (Block lockedBlock : this.lockedBlocks) {
			this.lockedRows.add(lockedBlock.getP());
			this.lockedColumns.add(lockedBlock.getQ());
		}
	}
}
