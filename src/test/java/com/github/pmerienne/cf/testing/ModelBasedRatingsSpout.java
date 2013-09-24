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

import java.util.Map;

import storm.trident.operation.TridentCollector;
import storm.trident.spout.IBatchSpout;
import backtype.storm.Config;
import backtype.storm.task.TopologyContext;
import backtype.storm.tuple.Fields;
import backtype.storm.tuple.Values;

import com.github.pmerienne.cf.rating.Rating;

/**
 * Not thread safe !!!
 * 
 * @author pmerienne
 * 
 */
public class ModelBasedRatingsSpout implements IBatchSpout {

	private static final long serialVersionUID = 8566700111208314156L;

	private static final int DEFAULT_MAX_BATCH_SIZE = 10000;

	private final RatingModel ratingModel;
	private final int maxBatchSize;

	public ModelBasedRatingsSpout(RatingModel ratingModel) {
		this(ratingModel, DEFAULT_MAX_BATCH_SIZE);
	}

	public ModelBasedRatingsSpout(RatingModel ratingModel, int maxBatchSize) {
		this.ratingModel = ratingModel;
		this.maxBatchSize = maxBatchSize;
	}

	@Override
	public void emitBatch(long batchId, TridentCollector collector) {
		int i = 0;

		while (this.ratingModel != null && this.ratingModel.hasNext() && i < this.maxBatchSize) {
			Rating rating = this.ratingModel.next();
			collector.emit(new Values(rating.i, rating.j, rating.value));
			i++;
		}
	}

	@SuppressWarnings("rawtypes")
	@Override
	public Map getComponentConfiguration() {
		Config conf = new Config();
		conf.setMaxTaskParallelism(1);
		return conf;
	}

	@Override
	public void close() {
	}

	@Override
	public Fields getOutputFields() {
		return new Fields("i", "j", "value");
	}

	@SuppressWarnings("rawtypes")
	@Override
	public void open(Map conf, TopologyContext context) {
	}

	@Override
	public void ack(long batchId) {
	}

	public static interface RatingModel {

		boolean hasNext();

		Rating next();

	}
}
