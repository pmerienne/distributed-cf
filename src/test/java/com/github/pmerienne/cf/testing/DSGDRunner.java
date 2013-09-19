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
package com.github.pmerienne.cf.testing;

import java.util.List;

import storm.trident.Stream;
import storm.trident.TridentTopology;
import backtype.storm.Config;
import backtype.storm.LocalCluster;
import backtype.storm.LocalDRPC;

import com.github.pmerienne.cf.DSGD;
import com.github.pmerienne.cf.DSGD.Options;
import com.github.pmerienne.cf.rating.Rating;
import com.github.pmerienne.cf.rmse.RMSEBenchmarkResult;
import com.github.pmerienne.cf.rmse.RMSEEvaluator;

public class DSGDRunner {

	private static final double TRANING_PERCENT = 0.80;

	private LocalDRPC drpc;
	private LocalCluster cluster;
	private TridentTopology topology;

	private final Options options;
	private final long timeout;

	private final List<Rating> trainingRatings;
	private final List<Rating> evalRatings;

	public DSGDRunner(List<Rating> ratings, Options options, long timeout) {
		this.options = options;
		this.timeout = timeout;
		this.evalRatings = DatasetUtils.extractEval(ratings, TRANING_PERCENT);
		this.trainingRatings = ratings;
	}

	public RMSEBenchmarkResult run() {
		try {
			this.init();
			return this.computeRMSE();
		} finally {
			this.release();
		}
	}

	private void init() {
		this.cluster = new LocalCluster();
		this.drpc = new LocalDRPC();
		this.topology = new TridentTopology();
	}

	private void release() {
		this.cluster.shutdown();
		this.drpc.shutdown();
	}

	private RMSEBenchmarkResult computeRMSE() {
		try {
			Config config = new Config();
			
			Stream ratings = this.topology.newStream("ratings", new FixedRatingsSpout(this.trainingRatings));
			DSGD dsgd = new DSGD(this.topology, ratings, options, config);
			RMSEEvaluator rmseEvaluator = new RMSEEvaluator(dsgd, topology, drpc);

			this.cluster.submitTopology(this.getClass().getSimpleName(), config, topology.build());
			Thread.sleep(this.timeout);

			double trainingRMSE = rmseEvaluator.rmse(trainingRatings);
			double evalRMSE = rmseEvaluator.rmse(evalRatings);
			return new RMSEBenchmarkResult(trainingRMSE, evalRMSE);

		} catch (InterruptedException e) {
			e.printStackTrace();
			return null;
		}
	}

}
