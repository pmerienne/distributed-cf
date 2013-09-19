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

	private final HashMap<Long, double[]> values;

	public MatrixBlock() {
		this.values = new HashMap<>();
	}

	public double[] get(long i) {
		double[] vector = this.values.get(i);
		return vector;
	}

	/**
	 * 
	 * @param i
	 * @param vector
	 * @return the previous value, or <tt>null</tt> if there was value.
	 */
	public double[] set(long i, double[] vector) {
		return this.values.put(i, vector);
	}

	public boolean exists(long i) {
		return this.values.containsKey(i);
	}

	public Set<Long> indexes() {
		return this.values.keySet();
	}
}
