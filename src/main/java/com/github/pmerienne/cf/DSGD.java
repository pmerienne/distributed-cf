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

import storm.trident.Stream;
import storm.trident.TridentState;
import storm.trident.TridentTopology;
import storm.trident.operation.builtin.MapGet;
import storm.trident.state.StateFactory;
import storm.trident.testing.MemoryMapState;
import backtype.storm.Config;
import backtype.storm.tuple.Fields;

import com.github.pmerienne.cf.block.Block;
import com.github.pmerienne.cf.block.BlockAssigner;
import com.github.pmerienne.cf.block.BlockSubmitter;
import com.github.pmerienne.cf.block.GetAllBlocksForUser;
import com.github.pmerienne.cf.block.MatrixBlock;
import com.github.pmerienne.cf.features.GetFeatures;
import com.github.pmerienne.cf.math.BlockSGD;
import com.github.pmerienne.cf.math.DotProduct;
import com.github.pmerienne.cf.rating.AssignRatingsToBlock;
import com.github.pmerienne.cf.rating.GetRatedItems;
import com.github.pmerienne.cf.rating.GetRatingsFromBlock;
import com.github.pmerienne.cf.rating.Rating;
import com.github.pmerienne.cf.recommendation.TopKItemsAggregator;
import com.github.pmerienne.cf.recommendation.TopKItemsToValues;
import com.github.pmerienne.cf.util.MapPut;
import com.github.pmerienne.cf.util.RichSpoutBatchExecutor;
import com.github.pmerienne.trident.state.ExtendedStateFactory;
import com.github.pmerienne.trident.state.MapMultimapState;
import com.github.pmerienne.trident.state.memory.MemoryMapMultimapState;

/**
 * Builds a cf algorithm over a {@link TridentTopology}. online collaborative
 * filtering based on matrix factorization distributed stochastic gradient
 * descent
 * 
 * a distributed matrix factorization approach to collaborative filtering
 * 
 * 
 * TODO : // TODO : // hints in options
 * 
 * @author pmerienne
 * 
 */
public class DSGD {

	private TridentTopology topology;

	private Options options;

	private TridentState ratingsBlockState;
	private TridentState userBlockState;
	private TridentState itemBlockState;

	public DSGD(TridentTopology topology, Stream ratings, Options options, Config config) {
		this.topology = topology;
		this.options = options;

		this.initConfig(config);
		this.initStates();
		this.processNewRatings(ratings);
		this.processDSGD();
	}

	private void initConfig(Config config) {
		config.setMaxSpoutPending((int) options.d);

		config.registerSerialization(Rating.class);
		config.registerSerialization(MatrixBlock.class);
		config.registerSerialization(Block.class);
	}

	private void initStates() {
		this.ratingsBlockState = this.topology.newStaticState(options.ratingsBlockStateFactory);
		this.userBlockState = this.topology.newStaticState(options.userBlockStateFactory);
		this.itemBlockState = this.topology.newStaticState(options.itemBlockStateFactory);
	}

	private void processNewRatings(Stream ratings) {
		ratings
		// Get block indexes
		.each(new Fields("i", "j"), new BlockAssigner(options.d), new Fields("p", "q"))
		// Add ratings to block
		.partitionPersist(options.ratingsBlockStateFactory, new Fields("i", "j", "value"), new AssignRatingsToBlock(options.d));
	}

	private void processDSGD() {
		Stream newBlocksStream = this.topology.newStream("blocksToProcess", new RichSpoutBatchExecutor(new BlockSubmitter(options.d), 1)).shuffle()
		// TODO : //
		// Get blocks
				.stateQuery(this.userBlockState, new Fields("p"), new MapGet(), new Fields("up")).stateQuery(this.itemBlockState, new Fields("q"), new MapGet(), new Fields("vq"))
				// Get ratings
				.stateQuery(this.ratingsBlockState, new Fields("p", "q"), new GetRatingsFromBlock(), new Fields("ratings"))
				// Process blocks
				.each(new Fields("up", "vq", "ratings"), new BlockSGD(options.stepSize, options.lambda, options.k), new Fields("newUp", "newVq")).parallelismHint((int) options.d);

		// Update Up
		newBlocksStream.partitionPersist(options.userBlockStateFactory, new Fields("p", "newUp"), new MapPut()).parallelismHint((int) options.d);

		// Update Vq
		newBlocksStream.partitionPersist(options.itemBlockStateFactory, new Fields("q", "newVq"), new MapPut()).parallelismHint((int) options.d);
	}

	public Stream addRecommendationStream(Stream recommendationRequests, int k) {
		// TODO : optimize this!
		return recommendationRequests
		// Get user features
				.stateQuery(this.userBlockState, new Fields("i"), new GetFeatures(options.d), new Fields("ui"))
				// Get all user blocks
				.each(new Fields("i"), new GetAllBlocksForUser(options.d), new Fields("p", "q")).shuffle()
				// Get rated item
				.stateQuery(this.ratingsBlockState, new Fields("i", "p", "q"), new GetRatedItems(), new Fields("ratedItems"))
				// Get items matrix block
				.stateQuery(this.itemBlockState, new Fields("q"), new MapGet(), new Fields("vq")).parallelismHint(options.recommendationsParallelism)
				// Aggregate result to keep top k rating
				.aggregate(new Fields("ui", "ratedItems", "vq"), new TopKItemsAggregator(k), new Fields("topKItems"))
				// Convert to values
				.each(new Fields("topKItems"), new TopKItemsToValues(), new Fields("item", "score"))
				// Project interesting fields
				.project(new Fields("item", "score")).parallelismHint(options.recommendationsParallelism);
	}

	public Stream addPredictionStream(Stream predictionRequests) {
		// TODO : //
		return predictionRequests
		// Get user features
				.stateQuery(this.userBlockState, new Fields("i"), new GetFeatures(options.d), new Fields("ui"))
				// Get item features
				.stateQuery(this.itemBlockState, new Fields("j"), new GetFeatures(options.d), new Fields("vj"))
				// dot product
				.each(new Fields("ui", "vj"), new DotProduct(), new Fields("prediction"))
				// Projects interesting fields
				.project(new Fields("i", "j", "prediction"));
	}

	public static class Options {
		public long d = 100;
		public int k = 10;

		public double stepSize = 0.001;
		public double lambda = 0.001;

		public int recommendationsParallelism = 10;
		public int newRatingsParallelism = 10;

		@SuppressWarnings({ "rawtypes", "unchecked" })
		public ExtendedStateFactory<MapMultimapState> ratingsBlockStateFactory = new MemoryMapMultimapState.Factory();
		public StateFactory userBlockStateFactory = new MemoryMapState.Factory();
		public StateFactory itemBlockStateFactory = new MemoryMapState.Factory();
	}
}
