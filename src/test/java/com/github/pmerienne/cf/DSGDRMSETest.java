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

import static org.fest.assertions.Assertions.assertThat;

import java.util.List;

import org.junit.Test;

import com.github.pmerienne.cf.DSGD.Options;
import com.github.pmerienne.cf.dataset.MovieLensDataset;
import com.github.pmerienne.cf.rating.Rating;
import com.github.pmerienne.cf.rmse.RMSEBenchmarkResult;
import com.github.pmerienne.cf.testing.DSGDRunner;

public class DSGDRMSETest {

	private static final long TEST_TIMEOUT = 20000;

	@Test
	public void should_learn_movielens100k_dataset() {
		// Given
		List<Rating> ratings = MovieLensDataset.get();

		Options options = new Options();
		options.d = 10;
		options.k = 10;
		options.lambda = 0.1;
		options.stepSize = 0.1;

		DSGDRunner dsgdRunner = new DSGDRunner(ratings, options, TEST_TIMEOUT);

		// When
		RMSEBenchmarkResult rmse = dsgdRunner.run();

		// Then
		assertThat(rmse.getTraining()).isLessThan(0.3);
		assertThat(rmse.getEval()).isLessThan(0.3);
	}
}
