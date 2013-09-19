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
package com.github.pmerienne.cf.rmse;

import java.util.List;

import storm.trident.Stream;
import storm.trident.TridentTopology;
import storm.trident.operation.BaseFunction;
import storm.trident.operation.TridentCollector;
import storm.trident.tuple.TridentTuple;
import backtype.storm.LocalDRPC;
import backtype.storm.tuple.Fields;
import backtype.storm.tuple.Values;

import com.github.pmerienne.cf.DSGD;
import com.github.pmerienne.cf.rating.Rating;
import com.github.pmerienne.cf.testing.DRPCUtils;
import com.github.pmerienne.cf.util.ListUtils;

public class RMSEEvaluator {

	private static final int MAX_SIZE = 1000;

	private final DSGD dsgd;
	private final TridentTopology topology;
	private final LocalDRPC drpc;

	public RMSEEvaluator(DSGD dsgd, TridentTopology topology, LocalDRPC drpc) {
		super();
		this.dsgd = dsgd;
		this.topology = topology;
		this.drpc = drpc;

		Stream predictionRequests = this.topology.newDRPCStream("predictionsRequestForRMSE", this.drpc).each(new Fields("args"), new ExtractRecommendationRequest(), new Fields("i", "j"));
		this.dsgd.addPredictionStream(predictionRequests);
	}

	public double rmse(List<Rating> ratings) {
		if (ratings.size() > MAX_SIZE) {
			ratings = ListUtils.randomSubList(ratings, MAX_SIZE);
		}

		RMSE rmse = new RMSE();

		double actual, expected;
		for (Rating rating : ratings) {
			expected = rating.value;
			actual = this.getPrediction(rating);
			rmse.add(expected, actual);
		}

		return rmse.getNormalized();
	}

	protected double getPrediction(Rating rating) {
		double prediction;

		String drpcResult = this.drpc.execute("predictionsRequestForRMSE", rating.i + " " + rating.j);
		try {
			prediction = Double.parseDouble(DRPCUtils.extractSingleValue(drpcResult, 2).toString());
		} catch (Exception ex) {
			prediction = rating.value * 2;
		}

		return prediction;
	}

	private static class ExtractRecommendationRequest extends BaseFunction {

		private static final long serialVersionUID = 7171566985006542069L;

		@Override
		public void execute(TridentTuple tuple, TridentCollector collector) {
			String[] args = tuple.getString(0).split(" ");
			long i = Long.parseLong(args[0]);
			long j = Long.parseLong(args[1]);
			collector.emit(new Values(i, j));
		}
	}

}
