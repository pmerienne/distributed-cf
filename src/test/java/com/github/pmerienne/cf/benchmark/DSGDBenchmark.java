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
package com.github.pmerienne.cf.benchmark;

import java.util.ArrayList;
import java.util.HashMap;
import java.util.List;
import java.util.Map;

import com.github.pmerienne.cf.DSGD.Options;
import com.github.pmerienne.cf.testing.dataset.FileDatasetUtil;

public class DSGDBenchmark {

	private static final int BENCHMARK_TIMEOUT = 20000;

	public static void main(String[] args) {
		DSGDBenchmark runner = new DSGDBenchmark();
		Map<Options, RMSEBenchmarkResult> results = runner.run();
		runner.diplayResults(results);
	}

	public Map<Options, RMSEBenchmarkResult> run() {
		int[] possibleKs = new int[] { 10};
		double[] possibleLambdas = new double[] { 0.05 };
		double[] possibleStepSizes = new double[] { 0.1 };

		List<Options> options = this.createOptions(possibleKs, possibleLambdas, possibleStepSizes);

		Map<Options, RMSEBenchmarkResult> results = new HashMap<>();
		for (Options option : options) {
			RMSEBenchmarkResult result = this.computeRMSE(option);
			results.put(option, result);
			this.displayResult(option, result);
		}
		return results;
	}

	private List<Options> createOptions(int[] possibleKs, double[] possibleLambdas, double[] possibleStepSizes) {
		List<Options> options = new ArrayList<>();

		for (int k : possibleKs) {
			for (double lambda : possibleLambdas) {
				for (double stepSize : possibleStepSizes) {
					Options option = new Options();
					option.d = 10;
					option.k = k;
					option.lambda = lambda;
					option.stepSize = stepSize;

					options.add(option);
				}
			}
		}
		return options;
	}

	private RMSEBenchmarkResult computeRMSE(Options options) {
		DSGDRunner benchmark = new DSGDRunner(FileDatasetUtil.getMovieLensRatings(), options, BENCHMARK_TIMEOUT);
		return benchmark.run();
	}

	private void diplayResults(Map<Options, RMSEBenchmarkResult> results) {
		System.out.println("##########################################################");
		System.out.println("##########################################################");
		System.out.println("##########################################################");
		System.out.println("k\t\tlambda\t\tstep size\t\teval rmse\t\ttraining rmse");

		for (Options option : results.keySet()) {
			RMSEBenchmarkResult result = results.get(option);
			this.displayResult(option, result);
		}
	}

	private void displayResult(Options options, RMSEBenchmarkResult result) {
		System.out.println(options.k + "\t\t" + options.lambda + "\t\t" + options.stepSize + "\t\t" + result.getEval() + "\t\t" + result.getTraining());
	}
}
