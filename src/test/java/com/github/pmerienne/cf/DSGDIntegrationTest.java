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

import static com.github.pmerienne.cf.testing.dataset.DatasetUtils.extractEval;
import static com.github.pmerienne.cf.testing.dataset.DatasetUtils.generateRatings;
import static com.github.pmerienne.cf.testing.dataset.DatasetUtils.removeRandomRatings;
import static org.fest.assertions.Assertions.assertThat;

import java.util.ArrayList;
import java.util.List;

import org.junit.After;
import org.junit.Before;
import org.junit.Test;

import storm.trident.Stream;
import storm.trident.TridentTopology;
import backtype.storm.Config;
import backtype.storm.LocalCluster;
import backtype.storm.LocalDRPC;
import backtype.storm.tuple.Fields;
import backtype.storm.utils.Utils;

import com.github.pmerienne.cf.DSGD.Options;
import com.github.pmerienne.cf.rating.Rating;
import com.github.pmerienne.cf.recommendation.Recommendation;
import com.github.pmerienne.cf.rmse.RMSEEvaluator;
import com.github.pmerienne.cf.testing.ExtractPredictionRequest;
import com.github.pmerienne.cf.testing.ExtractRecommendationsRequest;
import com.github.pmerienne.cf.testing.FixedRatingsSpout;
import com.github.pmerienne.cf.testing.ModelBasedRatingsSpout;
import com.github.pmerienne.cf.testing.RatingModelFromList;
import com.github.pmerienne.cf.testing.dataset.DatasetUtils;
import com.github.pmerienne.cf.testing.dataset.MovieLensDataset;
import com.github.pmerienne.cf.util.DRPCUtils;

public class DSGDIntegrationTest {

	private static final long TEST_TIMEOUT = 10000;
	private static final double TRAINING_PERCENT = 0.80;

	protected LocalDRPC drpc;
	protected LocalCluster cluster;
	protected TridentTopology topology;
	protected Options options;
	protected Config config;

	@Before
	public void init() {
		this.cluster = new LocalCluster();
		this.drpc = new LocalDRPC();
		this.topology = new TridentTopology();

		this.options = new Options();
		this.options.d = 2;
		this.options.k = 10;
		this.options.lambda = 0.1;
		this.options.stepSize = 0.1;
		this.options.newRatingsParallelism = 1;
		this.options.recommendationsParallelism = 1;

		this.config = new Config();
	}

	@After
	public void release() {
		this.cluster.shutdown();
		this.drpc.shutdown();
	}

	@Test
	public void should_recommend_successfully() {
		// Given
		int recommendationSize = 5;
		long testUser = 1;
		List<Rating> allRatings = DatasetUtils.generateRatings(100, 200, true);
		removeRandomRatings(testUser, 10, allRatings);

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

	@Test
	public void should_predict_successfully() {
		// Given
		long testUser = 1;
		List<Rating> allRatings = generateRatings(100, 200, true);
		List<Rating> removedRatings = removeRandomRatings(testUser, 2, allRatings);
		Rating rating1 = removedRatings.get(0);
		Rating rating2 = removedRatings.get(1);

		Stream ratingsStream = this.topology.newStream("ratings", new FixedRatingsSpout(allRatings));
		Stream predictionQueryStream = this.topology.newDRPCStream("predictions", this.drpc).each(new Fields("args"), new ExtractPredictionRequest(), new Fields("i", "j"));

		DSGD dsgd = new DSGD(this.topology, ratingsStream, options, config);
		dsgd.addPredictionStream(predictionQueryStream);

		// When
		this.cluster.submitTopology(this.getClass().getSimpleName(), config, topology.build());
		Utils.sleep(TEST_TIMEOUT);

		final double prediction1 = DRPCUtils.<Double> extractSingleValue(this.drpc.execute("predictions", rating1.i + " " + rating1.j), 2);
		final double prediction2 = DRPCUtils.<Double> extractSingleValue(this.drpc.execute("predictions", rating2.i + " " + rating2.j), 2);

		// Then
		if (rating1.j > rating2.j) {
			assertThat(prediction1).isGreaterThan(prediction2);
		} else {
			assertThat(prediction1).isLessThan(prediction2);
		}
	}

	@Test
	public void should_learn_movielens100k_dataset() {
		// Given
		List<Rating> training = MovieLensDataset.get();
		List<Rating> eval = DatasetUtils.extractEval(training, TRAINING_PERCENT);

		Stream ratingsStream = this.topology.newStream("ratings", new FixedRatingsSpout(training));
		Stream predictionQueryStream = this.topology.newDRPCStream("predictions", this.drpc).each(new Fields("args"), new ExtractPredictionRequest(), new Fields("i", "j"));

		DSGD dsgd = new DSGD(this.topology, ratingsStream, options, config);
		dsgd.addPredictionStream(predictionQueryStream);

		RMSEEvaluator rmseEvaluator = new RMSEEvaluator(dsgd, topology, drpc);

		// When
		this.cluster.submitTopology(this.getClass().getSimpleName(), config, topology.build());
		Utils.sleep(TEST_TIMEOUT);

		// Then
		double trainingRMSE = rmseEvaluator.rmse(training);
		double evalRMSE = rmseEvaluator.rmse(eval);

		assertThat(trainingRMSE).isLessThan(0.3);
		assertThat(evalRMSE).isLessThan(0.3);
	}

	@Test
	public void should_override_old_ratings() {
		// Given
		List<Rating> originalTraining = generateRatings(0, 0, 500, 100, true);
		List<Rating> newTraining = generateRatings(0, 0, 500, 100, false);
		List<Rating> training = new ArrayList<>();
		training.addAll(originalTraining);
		training.addAll(newTraining);

		List<Rating> eval = DatasetUtils.extractEval(newTraining, TRAINING_PERCENT);

		Stream ratingsStream = this.topology.newStream("ratings", new FixedRatingsSpout(training));
		Stream predictionQueryStream = this.topology.newDRPCStream("predictions", this.drpc).each(new Fields("args"), new ExtractPredictionRequest(), new Fields("i", "j"));

		DSGD dsgd = new DSGD(this.topology, ratingsStream, options, config);
		dsgd.addPredictionStream(predictionQueryStream);

		RMSEEvaluator rmseEvaluator = new RMSEEvaluator(dsgd, topology, drpc);
		this.cluster.submitTopology(this.getClass().getSimpleName(), config, topology.build());

		// When
		Utils.sleep(TEST_TIMEOUT);

		// Then
		double trainingRMSE = rmseEvaluator.rmse(newTraining);
		double evalRMSE = rmseEvaluator.rmse(eval);

		assertThat(trainingRMSE).isLessThan(0.3);
		assertThat(evalRMSE).isLessThan(0.3);
	}

	@Test
	public void should_predict_successfully_after_concept_drift() {
		// Given
		List<Rating> firstConceptTraining = generateRatings(0, 0, 500, 100, true);
		List<Rating> firstConceptEval = extractEval(firstConceptTraining, TRAINING_PERCENT);
		List<Rating> secondConceptTraining = generateRatings(500, 100, 500, 100, false);
		List<Rating> secondConceptEval = extractEval(secondConceptTraining, TRAINING_PERCENT);

		RatingModelFromList ratingsModel = new RatingModelFromList();
		ModelBasedRatingsSpout ratingsSpout = new ModelBasedRatingsSpout(ratingsModel);

		Stream ratingsStream = this.topology.newStream("ratings", ratingsSpout);
		Stream predictionQueryStream = this.topology.newDRPCStream("predictions", this.drpc).each(new Fields("args"), new ExtractPredictionRequest(), new Fields("i", "j"));

		DSGD dsgd = new DSGD(this.topology, ratingsStream, options, config);
		dsgd.addPredictionStream(predictionQueryStream);

		RMSEEvaluator rmseEvaluator = new RMSEEvaluator(dsgd, topology, drpc);
		this.cluster.submitTopology(this.getClass().getSimpleName(), config, topology.build());

		// When
		ratingsModel.setRatings(firstConceptTraining);
		Utils.sleep(TEST_TIMEOUT);

		ratingsModel.setRatings(secondConceptTraining);
		Utils.sleep(TEST_TIMEOUT);

		// Then
		double firstConceptTrainingRMSE = rmseEvaluator.rmse(firstConceptTraining);
		double firstConceptEvalRMSE = rmseEvaluator.rmse(firstConceptEval);
		double secondConceptTrainingRMSE = rmseEvaluator.rmse(secondConceptTraining);
		double secondConceptEvalRMSE = rmseEvaluator.rmse(secondConceptEval);

		assertThat(firstConceptTrainingRMSE).isLessThan(0.3);
		assertThat(firstConceptEvalRMSE).isLessThan(0.3);
		assertThat(secondConceptTrainingRMSE).isLessThan(0.3);
		assertThat(secondConceptEvalRMSE).isLessThan(0.3);
	}

}
