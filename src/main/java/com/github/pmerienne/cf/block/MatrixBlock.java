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

import java.util.HashMap;
import java.util.Set;

public class MatrixBlock {

	private final HashMap<Long, double[]> features;
	private final HashMap<Long, Double> biases;

	public MatrixBlock() {
		this.features = new HashMap<>();
		this.biases = new HashMap<>();
	}

	public double[] getFeatures(long i) {
		double[] vector = this.features.get(i);
		return vector;
	}

	public double getBias(long i) {
		Double bias = this.biases.get(i);
		return bias == null ? 0.0 : bias;
	}

	public void setBias(long i, double bias) {
		this.biases.put(i, bias);
	}

	/**
	 * 
	 * @param i
	 * @param vector
	 * @return the previous value, or <tt>null</tt> if there was value.
	 */
	public double[] setFeatures(long i, double[] vector) {
		return this.features.put(i, vector);
	}

	public boolean featuresExists(long i) {
		return this.features.containsKey(i);
	}

	public Set<Long> featureIndexes() {
		return this.features.keySet();
	}

}
