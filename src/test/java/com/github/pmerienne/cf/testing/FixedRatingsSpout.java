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
import java.util.Map;

import storm.trident.operation.TridentCollector;
import storm.trident.spout.IBatchSpout;
import backtype.storm.Config;
import backtype.storm.task.TopologyContext;
import backtype.storm.tuple.Fields;
import backtype.storm.tuple.Values;

import com.github.pmerienne.cf.rating.Rating;

public class FixedRatingsSpout implements IBatchSpout {

	private static final long serialVersionUID = 8566700111208314156L;

	private static final int MAX_BATCH_SIZE = 10000;

	private int currentIndex = 0;
	private final List<Rating> ratings;

	public FixedRatingsSpout(List<Rating> ratings) {
		this.ratings = ratings;
	}

	@Override
	public void emitBatch(long batchId, TridentCollector collector) {
		if (this.hasNext()) {
			List<Rating> ratings = this.nextBatch();

			for (Rating rating : ratings) {
				collector.emit(new Values(rating.i, rating.j, rating.value));
			}
		}
	}

	private boolean hasNext() {
		return this.currentIndex + 1 < this.ratings.size();
	}

	private List<Rating> nextBatch() {
		int fromIndex = this.currentIndex;
		int toIndex = this.currentIndex + MAX_BATCH_SIZE;
		if (toIndex >= this.ratings.size()) {
			toIndex = this.ratings.size() - 1;
		}
		this.currentIndex += toIndex - fromIndex;

		return this.ratings.subList(fromIndex, toIndex);
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
}
