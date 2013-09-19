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
package com.github.pmerienne.cf;

import static org.fest.assertions.Assertions.assertThat;

import java.util.ArrayList;
import java.util.List;

import org.junit.After;
import org.junit.Before;
import org.junit.Test;

import storm.trident.Stream;
import storm.trident.TridentTopology;
import storm.trident.operation.BaseFunction;
import storm.trident.operation.TridentCollector;
import storm.trident.tuple.TridentTuple;
import backtype.storm.Config;
import backtype.storm.LocalCluster;
import backtype.storm.LocalDRPC;
import backtype.storm.tuple.Fields;
import backtype.storm.tuple.Values;
import backtype.storm.utils.Utils;

import com.github.pmerienne.cf.DSGD;
import com.github.pmerienne.cf.DSGD.Options;
import com.github.pmerienne.cf.rating.Rating;
import com.github.pmerienne.cf.recommendation.Recommendation;
import com.github.pmerienne.cf.testing.DRPCUtils;
import com.github.pmerienne.cf.testing.FixedRatingsSpout;
import com.github.pmerienne.cf.util.ListUtils;
import com.google.common.base.Predicate;
import com.google.common.collect.Iterables;
import com.google.common.collect.Lists;

public class DSGDRecommendationsITest {

	private static final long TEST_TIMEOUT = 10000;

	private LocalDRPC drpc;
	private LocalCluster cluster;
	private TridentTopology topology;

	@Before
	public void init() {
		this.cluster = new LocalCluster();
		this.drpc = new LocalDRPC();
		this.topology = new TridentTopology();
	}

	@After
	public void release() {
		this.cluster.shutdown();
		this.drpc.shutdown();
	}

	@Test
	public void should_recommend_successfully_on_movielens100k_dataset() {
		// Given
		int recommendationSize = 5;
		long testUser = 1;
		List<Rating> allRatings = this.generateAscendingRatings(100, 200);
		this.removeRandomRatings(testUser, 10, allRatings);

		Options options = new Options();
		options.d = 2;
		options.k = 10;
		options.lambda = 0.1;
		options.stepSize = 0.1;
		options.newRatingsParallelism = 1;
		options.recommendationsParallelism = 1;

		Config config = new Config();

		Stream ratingsStream = this.topology.newStream("ratings", new FixedRatingsSpout(allRatings));
		Stream recommendationQueryStream = this.topology.newDRPCStream("recommendations", this.drpc).each(new Fields("args"), new ExtractRecommendationsRequest(), new Fields("i"));

		DSGD dsgd = new DSGD(this.topology, ratingsStream, options, config);
		dsgd.addRecommendationStream(recommendationQueryStream, recommendationSize);

		// When
		this.cluster.submitTopology(this.getClass().getSimpleName(), config, topology.build());
		Utils.sleep(TEST_TIMEOUT);
		String drpcResult = this.drpc.execute("recommendations", Long.toString(testUser));

		// Then
		List<List<Object>> recommendations = DRPCUtils.extractValues(drpcResult);
		assertThat(recommendations).hasSize(recommendationSize);

		Recommendation lastRecommendation = null;
		for (List<Object> recommendation : recommendations) {
			Recommendation currentRecommendation = new Recommendation((Long) recommendation.get(0), (Double) recommendation.get(1));
			if (lastRecommendation == null) {
				lastRecommendation = currentRecommendation;
			} else {
				assertThat(currentRecommendation.score).isLessThan(lastRecommendation.score);
				assertThat(currentRecommendation.item).isLessThan(lastRecommendation.item);
			}
		}
	}

	protected List<Rating> generateAscendingRatings(int n, int m) {
		List<Rating> allRatings = new ArrayList<>(n * m);

		for (int i = 0; i < n; i++) {
			for (int j = 0; j < m; j++) {
				allRatings.add(new Rating(i, j, (double) j / (double) m));
			}
		}

		return allRatings;
	}

	protected List<Rating> removeRandomRatings(final long user, int count, List<Rating> allRatings) {
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

	private static class ExtractRecommendationsRequest extends BaseFunction {

		private static final long serialVersionUID = 7171566985006542069L;

		@Override
		public void execute(TridentTuple tuple, TridentCollector collector) {
			String[] args = tuple.getString(0).split(" ");
			long i = Long.parseLong(args[0]);
			collector.emit(new Values(i));
		}
	}

}
