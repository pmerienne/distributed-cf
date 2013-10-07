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
package com.github.pmerienne.cf.math;

import storm.trident.operation.BaseFunction;
import storm.trident.operation.TridentCollector;
import storm.trident.tuple.TridentTuple;
import backtype.storm.tuple.Values;

import com.github.pmerienne.cf.util.MathUtil;

public class Prediction extends BaseFunction {

	private static final long serialVersionUID = -3815389858150502634L;

	@Override
	public void execute(TridentTuple tuple, TridentCollector collector) {

		double[] userFeatures = (double[]) tuple.get(0);
		double[] itemFeatures = (double[]) tuple.get(1);
		Double userBias = tuple.getDouble(2);
		Double itemBias = tuple.getDouble(3);

		Double value = this.predict(userFeatures, itemFeatures, userBias, itemBias);
		collector.emit(new Values(value));
	}

	public Double predict(double[] userFeatures, double[] itemFeatures, double userBias, double itemBias) {
		Double value = null;
		if (userFeatures != null && itemFeatures != null) {
			value = MathUtil.dot(userFeatures, itemFeatures) + userBias + itemBias;
		}

		return value;
	}

}
