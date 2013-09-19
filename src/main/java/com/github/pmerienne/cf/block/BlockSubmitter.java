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

import java.util.Map;
import java.util.Set;

import backtype.storm.Config;
import backtype.storm.spout.SpoutOutputCollector;
import backtype.storm.task.TopologyContext;
import backtype.storm.topology.OutputFieldsDeclarer;
import backtype.storm.topology.base.BaseRichSpout;
import backtype.storm.tuple.Fields;
import backtype.storm.tuple.Values;
import backtype.storm.utils.Utils;

/**
 * 
 * @author pmerienne
 * 
 */
public class BlockSubmitter extends BaseRichSpout {

	private static final long serialVersionUID = -4354414409755969022L;

	private SpoutOutputCollector collector;
	private BlockLocker blockLocker;

	private long d;

	public BlockSubmitter(long d) {
		this.blockLocker = new BlockLocker();
		this.d = d;
		this.init();
	}

	public void init() {
		for (long p = 0; p < d; p++) {
			for (long q = 0; q < d; q++) {
				this.blockLocker.add(new Block(p, q));
			}
		}
	}

	public void reset() {
		this.blockLocker = new BlockLocker();
		this.init();
	}

	@SuppressWarnings("rawtypes")
	public void open(Map conf, TopologyContext context, SpoutOutputCollector collector) {
		this.collector = collector;
	}

	public void nextTuple() {
		Set<Block> unlockedBlocks = this.blockLocker.getAndLockRandomBlocks();
		for (Block block : unlockedBlocks) {
			this.collector.emit(new Values(block.getP(), block.getQ()), block);
		}

		Utils.sleep(1);
	}

	@Override
	public void ack(Object msgId) {
		Block block = (Block) msgId;
		this.blockLocker.unlock(block);
	}

	@Override
	public void fail(Object msgId) {
		Block block = (Block) msgId;
		this.blockLocker.unlock(block);
	}

	@Override
	public void declareOutputFields(OutputFieldsDeclarer declarer) {
		declarer.declare(new Fields("p", "q"));
	}

	@SuppressWarnings({ "unchecked", "rawtypes" })
	@Override
	public Map getComponentConfiguration() {
		Config conf = new Config();
		conf.setMaxTaskParallelism(1);
		return conf;
	}

}
