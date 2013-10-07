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
package com.github.pmerienne.cf.testing.dataset;

import java.util.ArrayList;
import java.util.Collections;
import java.util.HashSet;
import java.util.Iterator;
import java.util.List;
import java.util.Set;

import org.apache.commons.lang.math.RandomUtils;

import com.github.pmerienne.cf.rating.Rating;
import com.github.pmerienne.cf.util.ListUtils;
import com.google.common.base.Predicate;
import com.google.common.collect.Iterables;
import com.google.common.collect.Lists;

public class DatasetUtils {

	public static List<Rating> extractEval(List<Rating> ratings, double trainingPercent) {
		int evalSize = (int) (ratings.size() * (1.0 - trainingPercent));
		List<Rating> eval = new ArrayList<>(evalSize);

		Set<Long> knownUsers = new HashSet<>();
		Set<Long> knownItems = new HashSet<>();

		Iterator<Rating> it = ratings.iterator();
		while (it.hasNext() && eval.size() < evalSize) {
			Rating rating = it.next();

			if (knownUsers.contains(rating.i) && knownItems.contains(rating.j)) {
				eval.add(rating);
				it.remove();
			}
			knownUsers.add(rating.i);
			knownItems.add(rating.j);
		}

		return eval;
	}

	public static List<Rating> generateSparseRatings(long userCount, long itemCount, boolean asc) {
		return generateSparseRatings(0, 0, userCount, itemCount, asc);
	}

	public static List<Rating> generateSparseRatings(long userOffset, long itemOffset, long userCount, long itemCount, boolean asc) {
		return generateSparseRatings(0.20, userOffset, itemOffset, userCount, itemCount, asc);
	}

	public static List<Rating> generateSparseRatings(double sparsity, long userCount, long itemCount, boolean asc) {
		return generateSparseRatings(sparsity, 0, 0, userCount, itemCount, asc);
	}

	public static List<Rating> generateSparseRatings(double sparsity, long userOffset, long itemOffset, long userCount, long itemCount, boolean asc) {
		List<Rating> allRatings = new ArrayList<>();

		for (long i = userOffset; i < userCount + userOffset; i++) {
			for (long j = itemOffset; j < itemOffset + itemCount; j++) {
				if (RandomUtils.nextDouble() > sparsity) {
					double value = asc ? (double) j / (double) itemCount + itemOffset : 1 - (double) j / ((double) itemCount + (double) itemOffset);
					allRatings.add(new Rating(i, j, value));
				}
			}
		}

		Collections.shuffle(allRatings);
		return allRatings;
	}

	public static List<Rating> removeRandomRatings(final long user, int count, List<Rating> allRatings) {
		List<Rating> userRatings = Lists.newArrayList(Iterables.filter(allRatings, new Predicate<Rating>() {
			@Override
			public boolean apply(Rating input) {
				return input.i == user;
			}
		}));
		List<Rating> removedRatings = ListUtils.randomSubList(userRatings, count);

		allRatings.removeAll(removedRatings);
		return removedRatings;
	}

}
