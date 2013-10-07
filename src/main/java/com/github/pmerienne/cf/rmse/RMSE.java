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

public class RMSE {

	private long n = 0;
	private double sum = 0;

	private double max = -Double.MAX_VALUE;
	private double min = Double.MAX_VALUE;

	public void add(double expected, double actual) {
		this.n++;
		this.sum += Math.pow(expected - actual, 2);
		this.max = Math.max(this.max, expected);
		this.min = Math.min(this.min, expected);
	}

	public double get() {
		double rmsd = Math.sqrt(this.sum / this.n);
		return rmsd;
	}

	public double getNormalized() {
		double rmsd = this.get();
		return rmsd / (this.max - this.min);
	}

	@Override
	public String toString() {
		return "RMSE [n=" + n + ", sum=" + sum + ", max=" + max + ", min=" + min + "]";
	}

}